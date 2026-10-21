//! 엔진 워커는 거래 상태를 소유한다. 입력 대기 외에 외부 입출력이나
//! 스레드 생성·런타임 생성·디비 쓰기·동기 로그를 수행하지 않는다.
use super::{
    balance_commands::BalanceCommand,
    commands::OrderCommand,
    config::configure_engine_thread,
    engine::EngineState,
    output::{self, OutputBatch},
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

/// 출력 송신자는 이 함수로 이동하고 복제하지 않는다. 출력 공간을 먼저 검사한
/// 다음 한 명령을 처리해 한 배치를 전달하므로, 확정 도중 큐 포화가 생기지 않는다.
/// 출력 소비자는 공간을 늘리기만 하며 제어 객체가 수신자 수명을 유지한다.
pub(super) fn engine_thread_loop(
    orders: Receiver<OrderCommand>,
    balances: Receiver<BalanceCommand>,
    output: Option<Sender<OutputBatch>>,
    mut state: EngineState,
    running: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
) -> EngineState {
    configure_engine_thread();
    while running.load(Ordering::Acquire) && !closed.load(Ordering::Acquire) {
        if !output::has_capacity(output.as_ref()) {
            // 출력 소비자를 추월하지 않는다. 채널 전송에서 잠들거나 확정 결과를
            // 버리지 않고, 입력을 큐에 남겨 압력을 입구까지 전달한다.
            // 이 진행 제어는 유한 버퍼의 필수 제약이며 디비·네트워크 대기가 아니다.
            std::hint::spin_loop();
            continue;
        }
        // 기존 잔고 우선 정책은 유지한다. 두 입력 큐의 재생 순서·공정성은
        // 별도 계약이 필요하며, 이번 출력 순서 보장과 혼동하지 않는다.
        if let Ok(BalanceCommand::UpdateBalance {
            user_id,
            mint,
            available_delta,
            response,
        }) = balances.try_recv()
        {
            state
                .executor
                .balance_cache_mut()
                .add_available(user_id, &mint, available_delta);
            let _ = response.send(Ok(()));
            continue;
        }
        let command = match orders.recv_timeout(Duration::from_millis(1)) {
            Ok(command) => command,
            Err(crossbeam::channel::RecvTimeoutError::Timeout) => continue,
            Err(crossbeam::channel::RecvTimeoutError::Disconnected) => break,
        };
        // 출력 실패나 정상 종료를 본 뒤에는 새 상태 변경을 시작하지 않는다.
        // 검사와 처리가 경쟁한 마지막 명령의 출력도 살아 있는 큐에 남는다.
        if closed.load(Ordering::Acquire) || !running.load(Ordering::Acquire) {
            break;
        }
        match command {
            OrderCommand::SubmitOrder { order, response } => {
                let order_id = order.id;
                match process_submit_order(
                    order,
                    &mut state.orderbooks,
                    &state.matcher,
                    &mut state.executor,
                ) {
                    Ok(applied) => {
                        // 실제 제출 경로는 응답 대기 없이 접수 증명을 반환한다.
                        // 결과를 기다리는 호출자에게만 체결 목록을 별도로 복사한다.
                        let reply = response.as_ref().map(|_| applied.matches.clone());
                        if output.is_some() {
                            output::publish(
                                output.as_ref(),
                                OutputBatch::submitted(applied, &state.executor),
                            );
                        }
                        if let Some(response) = response {
                            let _ = response.send(Ok(reply.unwrap()));
                        }
                    }
                    Err(error) => {
                        if output.is_some() {
                            output::publish(
                                output.as_ref(),
                                OutputBatch::Rejected {
                                    order_id,
                                    reason: error.to_string(),
                                },
                            );
                        }
                        if let Some(response) = response {
                            let _ = response.send(Err(error));
                        }
                    }
                }
            }
            OrderCommand::CancelOrder {
                order_id,
                user_id,
                trading_pair,
                response,
            } => {
                let result = cancel_order(
                    order_id,
                    user_id,
                    &trading_pair,
                    &mut state.orderbooks,
                    &mut state.executor,
                );
                if let Ok(order) = &result {
                    if output.is_some() {
                        output::publish(output.as_ref(), OutputBatch::Cancelled(order.clone()));
                    }
                }
                let _ = response.send(result);
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
                let _ = response.send(Ok((bids, asks)));
            }
            OrderCommand::GetBalance {
                user_id,
                mint,
                response,
            } => {
                let balance = state
                    .executor
                    .balance_cache()
                    .get_balance(user_id, &mint)
                    .map(|balance| (balance.available, balance.locked))
                    .unwrap_or((Decimal::ZERO, Decimal::ZERO));
                let _ = response.send(Ok(balance));
            }
            OrderCommand::LockBalance {
                user_id,
                mint,
                amount,
                response,
            } => {
                let _ = response.send(
                    state
                        .executor
                        .lock_balance_for_order(0, user_id, &mint, amount),
                );
            }
            OrderCommand::UnlockBalance {
                user_id,
                mint,
                amount,
                response,
            } => {
                let _ = response.send(
                    state
                        .executor
                        .unlock_balance_for_cancel(0, user_id, &mint, amount),
                );
            }
        }
    }
    // 종료 후에는 제어 객체만 상태를 보관한다. 실행 중 공유 접근은 없다.
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
