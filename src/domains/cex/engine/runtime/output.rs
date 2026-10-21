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
use crossbeam::channel::{Receiver, Sender};
use rust_decimal::Decimal;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// 한 명령의 출력은 하나의 큐 항목이다. 여러 체결 도중 큐가 가득 차서
/// 일부만 전달되는 것을 막는다. 문자열과 직렬화는 출력 워커에서 구성한다.
#[derive(Debug)]
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
#[derive(Debug)]
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

/// 전달 확인이 끝나지 않은 위치와 원본 배치를 함께 보존한다.
/// 실패한 발행은 브로커에 도착했을 수도 있으므로 자동 재전송하지 않는다.
#[derive(Debug)]
pub(super) struct FailedOutput {
    pub batch: OutputBatch,
    pub next_event: usize,
    pub reason: String,
}
#[derive(Debug)]
pub(super) struct DeliveryError {
    pub next_event: usize,
    pub reason: String,
}

/// 송신자는 엔진 워커에만 존재한다. 제어 객체는 수신자만 보유하므로 큐가
/// 가득 차지 않았다는 검사 뒤, 다른 생산자가 빈 공간을 차지할 수 없다.
/// 수신자는 처리 실패 뒤에도 제어 객체에 남아 큐의 결과를 보존한다.
pub(super) fn has_capacity(sender: Option<&Sender<OutputBatch>>) -> bool {
    sender.map(|sender| !sender.is_full()).unwrap_or(true)
}

pub(super) fn publish(sender: Option<&Sender<OutputBatch>>, batch: OutputBatch) {
    if let Some(sender) = sender {
        // 한 생산자·한 명령당 한 배치·살아 있는 수신자라는 조건으로 공간을
        // 확보했다. 예상 밖 실패를 성공처럼 무시하지 않는다.
        sender
            .try_send(batch)
            .expect("reserved output capacity remains available");
    }
}

pub(super) fn deliver(
    producer: &KafkaProducer,
    batch: &OutputBatch,
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
                        event_type: "trade_executed".into(),
                        buy_order_id: t.buy_order_id,
                        sell_order_id: t.sell_order_id,
                        buyer_id: t.buyer_id,
                        seller_id: t.seller_id,
                        price: t.price,
                        amount: t.amount,
                        base_mint: t.base_mint.clone(),
                        quote_mint: t.quote_mint.clone(),
                        timestamp: Utc::now(),
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
                    event_type: "order_cancelled".into(),
                    order_id: order.id,
                    user_id: order.user_id,
                    base_mint: order.base_mint.clone(),
                    quote_mint: order.quote_mint.clone(),
                    timestamp: Utc::now(),
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

pub(super) fn run(
    receiver: Receiver<OutputBatch>,
    closed: Arc<AtomicBool>,
    mut deliver: impl FnMut(&OutputBatch) -> std::result::Result<(), DeliveryError>,
) -> Option<FailedOutput> {
    let _close = CloseOnExit(closed);
    while let Ok(batch) = receiver.recv() {
        if let Err(error) = deliver(&batch) {
            // 후속 배치를 소비하지 않는다. 실패 배치는 반환값에, 아직 읽지 않은
            // 배치는 제어 객체가 보유한 수신 큐에 남는다.
            let order_id = match &batch {
                OutputBatch::Submitted { order_id, .. }
                | OutputBatch::Rejected { order_id, .. } => *order_id,
                OutputBatch::Cancelled(order) => order.id,
            };
            eprintln!(
                "output_delivery_failed order_id={} next_event={} reason={}",
                order_id, error.next_event, error.reason
            );
            return Some(FailedOutput {
                batch,
                next_event: error.next_event,
                reason: error.reason,
            });
        }
        if let OutputBatch::Rejected { order_id, reason } = &batch {
            eprintln!("output_rejected order_id={} reason={}", order_id, reason);
        }
        if let OutputBatch::Submitted { order_id, trades } = &batch {
            eprintln!(
                "output_completed order_id={} trades={}",
                order_id,
                trades.len()
            );
        }
    }
    None
}

pub(super) fn spawn(
    receiver: Receiver<OutputBatch>,
    closed: Arc<AtomicBool>,
    producer: KafkaProducer,
) -> Result<std::thread::JoinHandle<Option<FailedOutput>>> {
    // 런타임 생성 실패는 엔진이 상태를 변경하기 전에 호출자에게 전달한다.
    // 런타임은 하나만 생성하고 출력 스레드로 소유권을 이동한다.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    Ok(std::thread::spawn(move || {
        run(receiver, closed, |batch| {
            deliver(&producer, batch, &runtime)
        })
    }))
}
