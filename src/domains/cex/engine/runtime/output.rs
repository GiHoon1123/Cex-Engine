//! 확정 결과를 순서대로 전달하는 출력 워커다. 거래 상태를 읽거나 수정하지 않는다.
use super::submission::{AppliedSubmission, MarketCompletion};
use crate::{
    domains::cex::engine::types::{MatchResult, OrderEntry},
    shared::kafka::{
        KafkaProducer, MarketOrderCompletedEvent, OrderCancelledEvent, OrderRejectedEvent,
        TradeExecutedEvent,
    },
};
use anyhow::Result;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// 한 명령의 출력은 하나의 큐 항목이다. 여러 체결 도중 큐가 가득 차서
/// 일부만 전달되는 것을 막는다. 문자열과 직렬화는 출력 워커에서 구성한다.
#[derive(Debug, Serialize, Deserialize)]
pub(super) enum OutputBatch {
    DepositCredited(super::DepositCredit),
    Submitted {
        order_id: u64,
        trades: Vec<TradeOutput>,
        completion: Option<MarketCompletion>,
    },
    Cancelled(OrderEntry),
    Rejected {
        order: OrderEntry,
        reason: String,
    },
}
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct TradeOutput {
    pub(super) trade: MatchResult,
    /// 실제 차감액도 결과에 보존한다. 복구 시 계산 정책이 달라지면 결과 비교가 실패해야 한다.
    buyer_fee: rust_decimal::Decimal,
    seller_fee: rust_decimal::Decimal,
}

impl OutputBatch {
    pub(super) fn submitted(applied: AppliedSubmission) -> Self {
        let order_id = applied.order_id;
        // 코어에서 확정한 체결을 전달한다. 개별 이벤트에 전체 주문의 마지막 잔고를
        // 반복하면 플랫폼의 후속 주문 예약을 덮어쓰므로 잔고 스냅샷을 보내지 않는다.
        let trades = applied
            .matches
            .into_iter()
            .map(|trade| {
                // 후보 정산에서 이미 성공한 계산만 사용한다. 이 아래의 재생 결과 비교에는
                // 적용률뿐 아니라 실제 차감액도 포함되어야 한다.
                let buyer_fee =
                    crate::domains::cex::engine::amount::fee(trade.amount, trade.buyer_fee_rate)
                        .expect("validated fee");
                let value = crate::domains::cex::engine::amount::quote(trade.price, trade.amount)
                    .expect("validated value");
                let seller_fee =
                    crate::domains::cex::engine::amount::fee(value, trade.seller_fee_rate)
                        .expect("validated fee");
                TradeOutput {
                    trade,
                    buyer_fee,
                    seller_fee,
                }
            })
            .collect();
        Self::Submitted {
            order_id,
            trades,
            completion: applied.completion,
        }
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

/// 출력과 감사 조회가 동일한 확정 이벤트를 직렬화한다. 코어 상태나 디비에 접근하지 않는다.
/// 콜백으로 한 건씩 넘겨 체결 목록 전체를 다시 복사하지 않는다.
pub(super) fn visit_events(
    batch: &OutputBatch,
    position: &Position<'_>,
    mut send: impl FnMut(usize, String) -> std::result::Result<(), DeliveryError>,
) -> std::result::Result<(), DeliveryError> {
    match batch {
        OutputBatch::DepositCredited(credit) => {
            // 결과 식별자는 원래 저널 위치다. 재발행에서도 시각과 식별자를 그대로 유지한다.
            let event = serde_json::json!({
                "event_id": format!("{}:{}:0", position.engine_id, position.sequence),
                "engine_id": position.engine_id,
                "event_type": "deposit_credited",
                "deposit_id": credit.deposit_id,
                "user_id": credit.user_id,
                "asset": credit.asset,
                "amount": credit.amount,
                "timestamp": position.timestamp,
            });
            send(0, encode(&event, 0)?)?;
        }
        OutputBatch::Rejected { order, .. } => {
            // 거절도 같은 명령의 영속 결과다. 오류 문자열이나 HTTP 상태를 반환 근거로
            // 사용하지 않고 원본과 연결된 고유 결과를 체결·취소와 같은 순서로 공개한다.
            let budget_buy = order.order_side == "market" && order.order_type == "buy";
            let event = OrderRejectedEvent {
                event_id: format!("{}:{}:0", position.engine_id, position.sequence),
                engine_id: position.engine_id.into(),
                event_type: "order_rejected",
                order_id: order.id,
                user_id: order.user_id,
                order_type: &order.order_type,
                order_side: &order.order_side,
                base_mint: &order.base_mint,
                quote_mint: &order.quote_mint,
                price: order.price,
                amount: if budget_buy { None } else { Some(order.amount) },
                quote_amount: order.quote_amount,
                fee_rate: order.fee_rate,
                reason_code: "ENGINE_EXECUTION_REJECTED",
                timestamp: position.timestamp,
            };
            send(0, encode(&event, 0)?)?;
        }
        OutputBatch::Submitted {
            trades, completion, ..
        } => {
            for (index, output) in trades.iter().enumerate() {
                let t = &output.trade;
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
                    buyer_fee_rate: t.buyer_fee_rate,
                    seller_fee_rate: t.seller_fee_rate,
                    buyer_fee: output.buyer_fee,
                    seller_fee: output.seller_fee,
                };
                send(index, encode(&event, index)?)?;
            }
            if let Some(completion) = completion {
                // 체결 뒤에 같은 파티션으로 전송한다. 마지막 이벤트 전 실패하면
                // 전달 커서를 남겨 재시작에서 체결·종료 모두 같은 식별자로 재전달한다.
                let event = MarketOrderCompletedEvent {
                    event_id: format!(
                        "{}:{}:{}",
                        position.engine_id,
                        position.sequence,
                        trades.len()
                    ),
                    engine_id: position.engine_id.into(),
                    event_type: "market_order_completed",
                    timestamp: position.timestamp,
                    result: completion,
                };
                send(trades.len(), encode(&event, trades.len())?)?;
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
            send(0, encode(&event, 0)?)?;
        }
    }
    Ok(())
}
fn encode(value: &impl Serialize, index: usize) -> std::result::Result<String, DeliveryError> {
    serde_json::to_string(value).map_err(|error| DeliveryError {
        next_event: index,
        reason: error.to_string(),
    })
}

pub(super) fn deliver(
    producer: &KafkaProducer,
    batch: &OutputBatch,
    position: &Position<'_>,
    runtime: &tokio::runtime::Runtime,
) -> std::result::Result<(), DeliveryError> {
    visit_events(batch, position, |index, payload| {
        runtime
            .block_on(producer.publish_financial_payload(position.engine_id, &payload))
            .map_err(|error| DeliveryError {
                next_event: index,
                reason: format!("{error:#}"),
            })
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
    mut send_market: impl FnMut(&super::market::MarketSnapshot) -> Result<()>,
) -> Result<()> {
    let _close = CloseOnExit(closed);
    let startup_end = progress.durable.load(Ordering::Acquire);
    let mut ready = Some(ready);
    let mut cursor = store.delivered()?;
    let mut market = super::market::Projection::default();
    // 외부 공개용 집계는 자체 저널에서 복원한다. 코어의 실행 상태를 읽지 않는다.
    // 기존 커서까지의 최신 스냅샷을 재발행해 새 플랫폼도 호가 기준점을 얻는다.
    for sequence in 1..=cursor {
        market.apply(&store.read(sequence)?, &engine_id)?;
    }
    for snapshot in market.latest.values() {
        send_market(snapshot)?;
    }
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
            }
            if let Some(snapshot) = market.apply(&record, &engine_id)? {
                if let Err(error) = send_market(&snapshot) {
                    eprintln!(
                        "market_delivery_failed sequence={} reason={error:#}",
                        record.sequence
                    );
                    return Err(error);
                }
            }
            // 금융 결과와 공개 시세 모두 Kafka 확인 후에만 커서를 저장한다. 불확실한 전달은 성공으로
            // 건너뛰지 않고 재시작에서 동일 식별자로 다시 보낸다.
            store.mark_delivered(record.sequence)?;
            if let super::journal::Outcome::Applied(Some(OutputBatch::Submitted {
                order_id,
                trades,
                ..
            })) = &record.outcome
            {
                eprintln!(
                    "output_completed order_id={} trades={}",
                    order_id,
                    trades.len()
                );
            }
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
            |snapshot| runtime.block_on(producer.publish_market_snapshot(snapshot)),
        )
    });
    Ok((worker, receiver))
}
