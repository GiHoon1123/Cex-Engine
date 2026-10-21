//! 확정 결과를 순서대로 전달하는 출력 워커다. 거래 상태를 읽거나 수정하지 않는다.
use super::submission::AppliedSubmission;
use crate::{
    domains::cex::engine::{
        executor::Executor,
        types::{MatchResult, OrderEntry},
    },
    shared::kafka::{KafkaProducer, OrderCancelledEvent, TradeExecutedEvent},
};
use anyhow::Result;
use chrono::Utc;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// 한 명령의 출력은 하나의 큐 항목이다. 여러 체결 도중 큐가 가득 차서
/// 일부만 전달되는 것을 막는다. 문자열과 직렬화는 출력 워커에서 구성한다.
#[derive(Debug, Serialize, Deserialize)]
pub(super) enum OutputBatch {
    Submitted {
        order_id: u64,
        trades: Vec<TradeOutput>,
    },
    Cancelled(OrderEntry),
    Rejected {
        order_id: u64,
        reason: String,
    },
}
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct TradeOutput {
    trade: MatchResult,
    balances: [Decimal; 8],
}

impl OutputBatch {
    pub(super) fn submitted(applied: AppliedSubmission, executor: &Executor) -> Self {
        let order_id = applied.order_id;
        let cache = executor.balance_cache();
        let trades = applied
            .matches
            .into_iter()
            .map(|trade| {
                // 전체 주문 확정 후의 스냅샷이라는 기존 계약을 유지한다.
                // 플랫폼 원장과 수수료의 권위·버전 문제는 별도로 해결해야 한다.
                let keys = [
                    (trade.buyer_id, &trade.base_mint),
                    (trade.buyer_id, &trade.quote_mint),
                    (trade.seller_id, &trade.base_mint),
                    (trade.seller_id, &trade.quote_mint),
                ];
                let mut balances = [Decimal::ZERO; 8];
                for (index, (user, asset)) in keys.into_iter().enumerate() {
                    let balance = cache
                        .get_balance(user, asset)
                        .expect("settled account exists");
                    balances[index * 2] = balance.available;
                    balances[index * 2 + 1] = balance.locked;
                }
                TradeOutput { trade, balances }
            })
            .collect();
        Self::Submitted { order_id, trades }
    }
}

/// 동일 저널 위치에서 재발행할 때 식별자와 시간을 그대로 재현한다.
pub(super) struct Position<'a> {
    pub engine_id: &'a str,
    pub sequence: u64,
    pub timestamp: chrono::DateTime<Utc>,
}
#[derive(Debug)]
pub(super) struct DeliveryError {
    pub next_event: usize,
    pub reason: String,
}

pub(super) fn deliver(
    producer: &KafkaProducer,
    batch: &OutputBatch,
    position: &Position<'_>,
    runtime: &tokio::runtime::Runtime,
) -> std::result::Result<(), DeliveryError> {
    runtime.block_on(async {
        match batch {
            OutputBatch::Rejected { .. } => {}
            OutputBatch::Submitted { trades, .. } => {
                for (index, output) in trades.iter().enumerate() {
                    let t = &output.trade;
                    let b = output.balances;
                    let event = TradeExecutedEvent {
                        event_id: format!("{}:{}:{}", position.engine_id, position.sequence, index),
                        engine_id: position.engine_id.into(),
                        event_type: "trade_executed".into(),
                        buy_order_id: t.buy_order_id,
                        sell_order_id: t.sell_order_id,
                        buyer_id: t.buyer_id,
                        seller_id: t.seller_id,
                        price: t.price,
                        amount: t.amount,
                        base_mint: t.base_mint.clone(),
                        quote_mint: t.quote_mint.clone(),
                        timestamp: position.timestamp,
                        buyer_base_available: b[0],
                        buyer_base_locked: b[1],
                        buyer_quote_available: b[2],
                        buyer_quote_locked: b[3],
                        seller_base_available: b[4],
                        seller_base_locked: b[5],
                        seller_quote_available: b[6],
                        seller_quote_locked: b[7],
                    };
                    producer
                        .publish_trade_executed(&event)
                        .await
                        .map_err(|error| DeliveryError {
                            next_event: index,
                            reason: format!("{error:#}"),
                        })?;
                }
            }
            OutputBatch::Cancelled(order) => {
                let event = OrderCancelledEvent {
                    event_id: format!("{}:{}:0", position.engine_id, position.sequence),
                    engine_id: position.engine_id.into(),
                    event_type: "order_cancelled".into(),
                    order_id: order.id,
                    user_id: order.user_id,
                    base_mint: order.base_mint.clone(),
                    quote_mint: order.quote_mint.clone(),
                    timestamp: position.timestamp,
                };
                producer
                    .publish_order_cancelled(&event)
                    .await
                    .map_err(|error| DeliveryError {
                        next_event: 0,
                        reason: format!("{error:#}"),
                    })?;
            }
        }
        Ok(())
    })
}

/// 종료·오류·패닉 어느 경로든 새 명령 진입을 닫는다. 거래 상태를 공유하는
/// 락이 아니라 입구의 생명주기 표식이다. 정상 종료는 엔진을 먼저 닫는다.
struct CloseOnExit(Arc<AtomicBool>);
impl Drop for CloseOnExit {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

/// 저널 워커가 공개한 영속 순번까지만 읽는다. 전달 확인과 커서 저장 사이에
/// 종료되면 같은 이벤트가 재발행될 수 있으므로 플랫폼의 영속 중복 검사가 필수다.
pub(super) fn run_store(
    store: super::journal::store::Store,
    engine_id: String,
    progress: Arc<super::journal::store::Progress>,
    closed: Arc<AtomicBool>,
    ready: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    mut send: impl FnMut(&OutputBatch, &Position<'_>) -> std::result::Result<(), DeliveryError>,
) -> Result<()> {
    let _close = CloseOnExit(closed);
    let startup_end = progress.durable.load(Ordering::Acquire);
    let mut ready = Some(ready);
    let mut cursor = store.delivered()?;
    loop {
        if cursor >= startup_end {
            if let Some(ready) = ready.take() {
                let _ = ready.send(Ok(()));
            }
        }
        if cursor < progress.durable.load(Ordering::Acquire) {
            let record = store.read(cursor + 1)?;
            if let super::journal::Outcome::Applied(Some(batch)) = &record.outcome {
                let position = Position {
                    engine_id: &engine_id,
                    sequence: record.sequence,
                    timestamp: record.timestamp,
                };
                if let Err(error) = send(batch, &position) {
                    eprintln!(
                        "output_delivery_failed sequence={} next_event={} reason={}",
                        record.sequence, error.next_event, error.reason
                    );
                    if let Some(ready) = ready.take() {
                        let _ = ready.send(Err(error.reason.clone()));
                    }
                    anyhow::bail!(
                        "Kafka delivery failed at sequence {} event {}: {}",
                        record.sequence,
                        error.next_event,
                        error.reason
                    );
                }
                match batch {
                    OutputBatch::Submitted { order_id, trades } => eprintln!(
                        "output_completed order_id={} trades={}",
                        order_id,
                        trades.len()
                    ),
                    OutputBatch::Rejected { order_id, reason } => {
                        eprintln!("output_rejected order_id={} reason={}", order_id, reason)
                    }
                    _ => {}
                }
            }
            // Kafka 확인 후에만 커서를 저장한다. 불확실한 전달은 성공으로
            // 건너뛰지 않고 재시작에서 동일 식별자로 다시 보낸다.
            store.mark_delivered(record.sequence)?;
            cursor = record.sequence;
            progress.delivered.store(cursor, Ordering::Release);
            continue;
        }
        if progress.journal_done.load(Ordering::Acquire)
            && cursor == progress.durable.load(Ordering::Acquire)
        {
            return Ok(());
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}
pub(super) fn spawn(
    store: super::journal::store::Store,
    engine_id: String,
    progress: Arc<super::journal::store::Progress>,
    closed: Arc<AtomicBool>,
    producer: KafkaProducer,
) -> Result<(
    std::thread::JoinHandle<Result<()>>,
    tokio::sync::oneshot::Receiver<std::result::Result<(), String>>,
)> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let worker = std::thread::spawn(move || {
        run_store(
            store,
            engine_id,
            progress,
            closed,
            sender,
            |batch, position| deliver(&producer, batch, position, &runtime),
        )
    });
    Ok((worker, receiver))
}
