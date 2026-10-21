//! 체결 상태는 이 워커 하나가 소유한다. 디스크·네트워크·응답 완료 대기 없이
//! 명령과 결과·응답 채널을 저널 큐로 이동한다. 영속 확정은 별도 워커의 책임이다.
use super::{
    balance_commands::BalanceCommand,
    commands::OrderCommand,
    config::configure_engine_thread,
    engine::EngineState,
    journal::{self, Command, Outcome, Pending, Reply},
    submission::{self, AppliedSubmission},
};
use crate::domains::cex::engine::{
    amount,
    executor::Executor,
    matcher::Matcher,
    orderbook::OrderBook,
    types::{OrderEntry, TradingPair},
};
use anyhow::{Context, Result};
use crossbeam::channel::{Receiver, Sender};
use rust_decimal::Decimal;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

/// 유일한 생산자가 빈 공간을 확보한 뒤 한 명령당 한 항목만 보낸다.
/// 소비자는 공간을 늘리기만 하므로 계산 도중 다른 생산자가 슬롯을 차지하지 않는다.
fn publish(sender: Option<&Sender<Pending>>, pending: Pending) {
    if let Some(sender) = sender {
        assert!(
            sender.try_send(pending).is_ok(),
            "reserved journal capacity remains available"
        );
    } else {
        // 순수 계산 벤치 모드에는 영속 보장을 부여하지 않는다.
        pending.finish();
    }
}
pub(super) fn engine_thread_loop(
    orders: Receiver<OrderCommand>,
    balances: Receiver<BalanceCommand>,
    output: Option<Sender<Pending>>,
    mut state: EngineState,
    running: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
) -> EngineState {
    // 어떤 종료 경로에서도 외부 요청이 살아 있는 코어로 오인하지 않게 한다.
    struct CloseCore(Arc<AtomicBool>);
    impl Drop for CloseCore {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    let _close = CloseCore(closed.clone());
    configure_engine_thread();
    while running.load(Ordering::Acquire) && !closed.load(Ordering::Acquire) {
        if output.as_ref().is_some_and(|sender| sender.is_full()) {
            // 유한 버퍼에서 기록을 버리지 않는다. 계산은 잠시 멈추지만 디스크
            // 호출이나 blocking send로 코어를 재우지 않는다.
            std::hint::spin_loop();
            continue;
        }
        if let Ok(BalanceCommand::UpdateBalance {
            user_id,
            mint,
            available_delta,
            response,
        }) = balances.try_recv()
        {
            let command = Command::Balance {
                user_id,
                mint,
                delta: available_delta,
            };
            let outcome = journal::apply(&mut state, &command);
            publish(
                output.as_ref(),
                Pending {
                    command: Some(command),
                    outcome,
                    reply: Reply::Unit(response),
                },
            );
            continue;
        }
        let command = match orders.recv_timeout(Duration::from_millis(1)) {
            Ok(command) => command,
            Err(crossbeam::channel::RecvTimeoutError::Timeout) => continue,
            Err(crossbeam::channel::RecvTimeoutError::Disconnected) => break,
        };
        if closed.load(Ordering::Acquire) || !running.load(Ordering::Acquire) {
            break;
        }
        let (command, reply) = match command {
            OrderCommand::SubmitOrder { order, response } => {
                (Some(Command::Submit(order)), Reply::Submit(response))
            }
            OrderCommand::CancelOrder {
                order_id,
                user_id,
                trading_pair,
                response,
            } => (
                Some(Command::Cancel {
                    order_id,
                    user_id,
                    pair: trading_pair,
                }),
                Reply::Cancel(response),
            ),
            OrderCommand::LockBalance {
                user_id,
                mint,
                amount,
                response,
            } => (
                Some(Command::Lock {
                    user_id,
                    mint,
                    amount,
                }),
                Reply::Unit(response),
            ),
            OrderCommand::UnlockBalance {
                user_id,
                mint,
                amount,
                response,
            } => (
                Some(Command::Unlock {
                    user_id,
                    mint,
                    amount,
                }),
                Reply::Unit(response),
            ),
            OrderCommand::GetBalance {
                user_id,
                mint,
                response,
            } => {
                let value = state
                    .executor
                    .balance_cache()
                    .get_balance(user_id, &mint)
                    .map(|b| (b.available, b.locked))
                    .unwrap_or((Decimal::ZERO, Decimal::ZERO));
                (None, Reply::Balance(response, value))
            }
            OrderCommand::GetOrderbook {
                trading_pair,
                depth,
                response,
            } => {
                let mut bids = Vec::new();
                let mut asks = Vec::new();
                if let Some(book) = state.orderbooks.get(&trading_pair) {
                    let count = depth.unwrap_or(usize::MAX);
                    bids.extend(
                        book.buy_orders
                            .orders
                            .values()
                            .rev()
                            .flatten()
                            .take(count)
                            .cloned(),
                    );
                    asks.extend(
                        book.sell_orders
                            .orders
                            .values()
                            .flatten()
                            .take(count)
                            .cloned(),
                    );
                }
                (None, Reply::Book(response, (bids, asks)))
            }
        };
        let outcome = command
            .as_ref()
            .map(|command| journal::apply(&mut state, command))
            .unwrap_or(Outcome::Applied(None));
        // 조회 응답도 앞선 명령의 커밋 뒤 공개한다. 아직 저장되지 않은 계산
        // 결과를 조회 성공으로 먼저 외부에 노출하지 않는 순서 장벽이다.
        publish(
            output.as_ref(),
            Pending {
                command,
                outcome,
                reply,
            },
        );
    }
    state
}

pub(super) fn process_submit_order(
    order: OrderEntry,
    books: &mut HashMap<TradingPair, OrderBook>,
    matcher: &Matcher,
    executor: &mut Executor,
) -> Result<AppliedSubmission> {
    submission::apply(order, books, matcher, executor)
}

/// 취소 출력도 한 배치로 전달한다. 예약 해제 검증이 실패하면 주문장을
/// 그대로 둔다. 주문 삭제는 카운터를 관리하는 주문장 메서드로만 수행한다.
pub(super) fn cancel_order(
    order_id: u64,
    user_id: u64,
    pair: &TradingPair,
    books: &mut HashMap<TradingPair, OrderBook>,
    executor: &mut Executor,
) -> Result<OrderEntry> {
    let book = books.get_mut(pair).context("OrderBook not found")?;
    let (level, buy, found) = book
        .buy_orders
        .orders
        .iter()
        .find_map(|(level, orders)| {
            orders
                .iter()
                .find(|order| order.id == order_id)
                .map(|order| (*level, true, order.clone()))
        })
        .or_else(|| {
            book.sell_orders.orders.iter().find_map(|(level, orders)| {
                orders
                    .iter()
                    .find(|order| order.id == order_id)
                    .map(|order| (*level, false, order.clone()))
            })
        })
        .context("Order not found")?;
    anyhow::ensure!(found.user_id == user_id, "Unauthorized cancellation");
    anyhow::ensure!(
        found.order_side == "limit" && found.order_type == if buy { "buy" } else { "sell" },
        "Invalid resting order kind"
    );
    anyhow::ensure!(
        found.price == Some(level)
            && level > Decimal::ZERO
            && found.remaining_amount > Decimal::ZERO,
        "Invalid resting order price or remaining quantity"
    );
    anyhow::ensure!(
        found.base_mint == pair.base_mint && found.quote_mint == pair.quote_mint,
        "Resting order market differs from book"
    );
    let asset = if buy {
        &found.quote_mint
    } else {
        &found.base_mint
    };
    let unlock = if buy {
        amount::quote(level, found.remaining_amount)?
    } else {
        found.remaining_amount
    };
    let side = if buy {
        &mut book.buy_orders
    } else {
        &mut book.sell_orders
    };
    anyhow::ensure!(side.total_orders() > 0, "Invalid order counter");
    executor.unlock_balance_for_cancel(order_id, user_id, asset, unlock)?;
    side.remove_order(order_id, level)
        .expect("validated order remains during exclusive borrow");
    Ok(found)
}
