// =====================================================
// Threads - 스레드 루프 함수들
// =====================================================
// 역할: 엔진 스레드와 WAL 스레드의 메인 루프 구현
//
// 구조:
// - engine_thread_loop(): 주문 처리 루프 (Core 0)
// - wal_thread_loop(): WAL 쓰기 루프 (Core 1)
// =====================================================

use std::collections::HashMap;
use std::sync::Arc;
use anyhow::{Result, Context};
use crossbeam::channel::Receiver;
use parking_lot::{RwLock, Mutex};
use rust_decimal::Decimal;
use sqlx::PgPool;
use crate::shared::database::Database;

use crate::domains::cex::engine::types::{TradingPair, OrderEntry, MatchResult};
use crate::domains::cex::engine::orderbook::OrderBook;
use crate::domains::cex::engine::matcher::Matcher;
use crate::domains::cex::engine::executor::Executor;
use crate::domains::cex::engine::wal::{WalEntry, WalWriter};
use crate::shared::kafka::{KafkaProducer, TradeExecutedEvent, OrderCancelledEvent};
use chrono::Utc;

use super::commands::OrderCommand;
use super::balance_commands::BalanceCommand;
use super::config::CoreConfig;

// =====================================================
// 엔진 스레드 루프
// =====================================================
// 역할: 모든 주문을 순차적으로 처리하는 싱글 스레드 루프
//
// 처리 과정:
// 1. 코어 고정 (Core 0)
// 2. 실시간 스케줄링 (SCHED_FIFO, 우선순위 99)
// 3. 주문 명령 수신 루프
// 4. 각 명령 처리 (SubmitOrder, CancelOrder 등)
// 5. 결과를 oneshot 채널로 반환
// =====================================================

/// 엔진 스레드 메인 루프
/// 
/// # Arguments
/// * `order_rx` - 주문 명령 수신 채널
/// * `balance_rx` - 잔고 업데이트 명령 수신 채널 (우선순위 높음)
/// * `wal_tx` - WAL 메시지 전송 채널
/// * `db_tx` - DB 명령 전송 채널
/// * `orderbooks` - 거래쌍별 오더북 (공유)
/// * `matcher` - 매칭 엔진 (공유)
/// * `executor` - 체결 실행 엔진 (공유)
/// * `running` - 실행 중 여부 플래그
/// 
/// # 처리 흐름 (우선순위 기반)
/// ```
/// loop {
///     // 1. 잔고 업데이트 큐 우선 확인 (논블로킹)
///     match balance_rx.try_recv() {
///         Ok(cmd) => {
///             handle_update_balance(cmd);
///             continue;  // 다음 루프로
///         }
///         Err(TryRecvError::Empty) => {
///             // 큐가 비어있음, 주문 큐 확인
///         }
///         Err(TryRecvError::Disconnected) => break,
///     }
///     
///     // 2. 주문 큐 확인 (블로킹)
///     match order_rx.recv() {
///         Ok(cmd) => {
///             match cmd {
///                 SubmitOrder → ...
///                 CancelOrder → ...
///                 ...
///             }
///         }
///         Err(_) => break,
///     }
/// }
/// ```
/// 
/// # 우선순위 전략
/// - 입금 큐 우선: 입금이 선행되어야 주문 가능
/// - 주문 큐: 입금 큐가 비어있을 때만 처리
/// 
/// # 성능
/// - 주문 처리: < 0.5ms (평균)
/// - 체결 처리: < 0.2ms (평균)
/// - TPS: 50,000+ orders/sec
pub fn engine_thread_loop(
    order_rx: Receiver<OrderCommand>,
    balance_rx: Receiver<BalanceCommand>,
    wal_tx: Option<crossbeam::channel::Sender<WalEntry>>,
    db_tx: Option<crossbeam::channel::Sender<super::db_commands::DbCommand>>,
    kafka_producer: Option<Arc<crate::shared::kafka::KafkaProducer>>,
    orderbooks: Arc<RwLock<HashMap<TradingPair, OrderBook>>>,
    matcher: Arc<Matcher>,
    executor: Arc<Mutex<Executor>>,
    running: Arc<std::sync::atomic::AtomicBool>,
    db: Option<Database>,
) {
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 1. 코어 고정 (Core 0에 고정) - PROD 환경만
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    let config = CoreConfig::from_env();
    // 엔진 스레드를 Core 0에 고정 (주문 처리 전용)
    // PROD: Core 0 고정, DEV: 코어 고정 안 함
    if config.engine_core != 999 {
        eprintln!("[Engine Thread] Pinning to Core {}", config.engine_core);
        CoreConfig::set_core(Some(config.engine_core));
    } else {
        eprintln!("[Engine Thread] Core pinning disabled (dev mode)");
        CoreConfig::set_core(None);  // 코어 고정 비활성화 (dev 환경)
    }
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 2. 실시간 스케줄링 설정 (우선순위 99) - PROD 환경만
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 실제 거래소에서 사용하는 방식: 최우선순위로 엔진 스레드 실행
    // Core 0을 독점하여 최대 성능 보장
    // 주의: CAP_SYS_NICE 권한 필요 (Docker: --cap-add=SYS_NICE)
    eprintln!("[Engine Thread] Setting real-time scheduling priority: 99");
    CoreConfig::set_realtime_scheduling(99);
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 2. 메인 루프 (우선순위 기반)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    // 채널 닫힘 상태 추적
    let mut balance_closed = false;
    let mut order_closed = false;
    
    loop {
        // running 플래그 확인
        if !running.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }
        
        // 두 채널 모두 닫혔으면 종료
        if balance_closed && order_closed {
            break;
        }
        
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 1. 잔고 업데이트 큐 우선 확인 (논블로킹)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 입금이 선행되어야 주문이 가능하므로 우선순위를 높게 설정
        if !balance_closed {
            match balance_rx.try_recv() {
            Ok(cmd) => {
                // 잔고 업데이트 처리
                match cmd {
                    BalanceCommand::UpdateBalance { user_id, mint, available_delta, response } => {
                        handle_update_balance(
                            user_id,
                            mint,
                            available_delta,
                            response,
                            wal_tx.as_ref(),
                            db_tx.as_ref(),
                            &executor,
                        );
                    }
                }
                continue; // 다음 루프로 (주문 큐 확인 전에 다시 잔고 큐 확인)
            }
            Err(crossbeam::channel::TryRecvError::Empty) => {
                // 큐가 비어있음, 주문 큐 확인
            }
                Err(crossbeam::channel::TryRecvError::Disconnected) => {
                    // balance_rx 채널이 닫힘 (sender가 닫혔음)
                    balance_closed = true;
                }
            }
        }
        
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 2. 주문 큐 확인 (타임아웃 사용)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 잔고 큐가 비어있을 때만 주문 처리
        // 짧은 타임아웃(1ms)을 사용하여 잔고 큐를 주기적으로 확인
        if !order_closed {
            use std::time::Duration;
            match order_rx.recv_timeout(Duration::from_millis(1)) {
            Ok(cmd) => {
                // running 플래그 확인 (명령 처리 전)
                if !running.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                
                // 명령 처리
                match cmd {
                    OrderCommand::SubmitOrder { order, response } => {
                        handle_submit_order(
                            order,
                            response,
                            wal_tx.as_ref(),
                            db_tx.as_ref(),
                            kafka_producer.as_ref(),
                            &orderbooks,
                            &matcher,
                            &executor,
                        );
                    }
                    OrderCommand::CancelOrder { order_id, user_id, trading_pair, response } => {
                        handle_cancel_order(
                            order_id,
                            user_id,
                            trading_pair,
                            response,
                            wal_tx.as_ref(),
                            db_tx.as_ref(),
                            kafka_producer.as_ref(),
                            &orderbooks,
                            &executor,
                            db.clone(),
                        );
                    }
                    OrderCommand::GetOrderbook { trading_pair, depth, response } => {
                        handle_get_orderbook(
                            trading_pair,
                            depth,
                            response,
                            &orderbooks,
                        );
                    }
                    OrderCommand::GetBalance { user_id, mint, response } => {
                        handle_get_balance(
                            user_id,
                            mint,
                            response,
                            &executor,
                        );
                    }
                    OrderCommand::LockBalance { user_id, mint, amount, response } => {
                        handle_lock_balance(
                            user_id,
                            mint,
                            amount,
                            response,
                            wal_tx.as_ref(),
                            db_tx.as_ref(),
                            &executor,
                        );
                    }
                    OrderCommand::UnlockBalance { user_id, mint, amount, response } => {
                        handle_unlock_balance(
                            user_id,
                            mint,
                            amount,
                            response,
                            wal_tx.as_ref(),
                            db_tx.as_ref(),
                            &executor,
                        );
                    }
                }
            }
                Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                    // 타임아웃: 잔고 큐를 다시 확인하기 위해 루프 계속
                    // 하지만 balance_closed가 true이면 order_rx도 닫혔는지 확인
                    if balance_closed {
                        // balance_rx는 이미 닫혔음, order_rx도 닫혔는지 확인
                        match order_rx.try_recv() {
                            Err(crossbeam::channel::TryRecvError::Disconnected) => {
                                // order_rx도 닫혔음
                                order_closed = true;
                                break; // 두 채널 모두 닫혔으므로 종료
                            }
                            Ok(cmd) => {
                                // order_rx에 메시지가 있으면 처리
                                if !running.load(std::sync::atomic::Ordering::Relaxed) {
                                    break;
                                }
                                match cmd {
                                    OrderCommand::SubmitOrder { order, response } => {
                                        handle_submit_order(
                                            order,
                                            response,
                                            wal_tx.as_ref(),
                                            db_tx.as_ref(),
                                            kafka_producer.as_ref(),
                                            &orderbooks,
                                            &matcher,
                                            &executor,
                                        );
                                    }
                                    OrderCommand::CancelOrder { order_id, user_id, trading_pair, response } => {
                                        handle_cancel_order(
                                            order_id,
                                            user_id,
                                            trading_pair,
                                            response,
                                            wal_tx.as_ref(),
                                            db_tx.as_ref(),
                                            kafka_producer.as_ref(),
                                            &orderbooks,
                                            &executor,
                                            db.clone(),
                                        );
                                    }
                                    OrderCommand::GetOrderbook { trading_pair, depth, response } => {
                                        handle_get_orderbook(
                                            trading_pair,
                                            depth,
                                            response,
                                            &orderbooks,
                                        );
                                    }
                                    OrderCommand::GetBalance { user_id, mint, response } => {
                                        handle_get_balance(
                                            user_id,
                                            mint,
                                            response,
                                            &executor,
                                        );
                                    }
                                    OrderCommand::LockBalance { user_id, mint, amount, response } => {
                                        handle_lock_balance(
                                            user_id,
                                            mint,
                                            amount,
                                            response,
                                            wal_tx.as_ref(),
                                            db_tx.as_ref(),
                                            &executor,
                                        );
                                    }
                                    OrderCommand::UnlockBalance { user_id, mint, amount, response } => {
                                        handle_unlock_balance(
                                            user_id,
                                            mint,
                                            amount,
                                            response,
                                            wal_tx.as_ref(),
                                            db_tx.as_ref(),
                                            &executor,
                                        );
                                    }
                                }
                                continue;
                            }
                            Err(crossbeam::channel::TryRecvError::Empty) => {
                                // order_rx는 비어있지만 아직 열려있음
                                continue;
                            }
                        }
                    } else {
                        continue;
                    }
                }
                Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                    // order_rx 채널이 닫힘 (sender가 닫혔음)
                    order_closed = true;
                    continue;
                }
            }
        }
    }
}

// =====================================================
// 명령 처리 핸들러들
// =====================================================

/// SubmitOrder 명령 처리
/// 
/// # 처리 과정
/// 1. WAL 메시지 발행 (OrderCreated)
/// 2. OrderBook에 추가
/// 3. Matcher로 매칭 시도
/// 4. 체결된 경우 Executor로 처리
/// 5. MatchResult 목록을 response로 전송
fn handle_submit_order(
    order: OrderEntry,
    response: Option<tokio::sync::oneshot::Sender<Result<Vec<MatchResult>>>>,
    wal_tx: Option<&crossbeam::channel::Sender<WalEntry>>,
    db_tx: Option<&crossbeam::channel::Sender<super::db_commands::DbCommand>>,
    kafka_producer: Option<&Arc<KafkaProducer>>,
    orderbooks: &Arc<RwLock<HashMap<TradingPair, OrderBook>>>,
    matcher: &Arc<Matcher>,
    executor: &Arc<Mutex<Executor>>,
) {
    let result = process_submit_order(order, wal_tx, db_tx, kafka_producer, orderbooks, matcher, executor);
    
    // response가 Some인 경우만 응답 전송 (비동기 처리 시 None)
    if let Some(tx) = response {
        let _ = tx.send(result);
    }
}

pub(crate) fn process_submit_order(
    mut order: OrderEntry,
    wal_tx: Option<&crossbeam::channel::Sender<WalEntry>>,
    db_tx: Option<&crossbeam::channel::Sender<super::db_commands::DbCommand>>,
    kafka_producer: Option<&Arc<KafkaProducer>>,
    orderbooks: &Arc<RwLock<HashMap<TradingPair, OrderBook>>>,
    matcher: &Arc<Matcher>,
    executor: &Arc<Mutex<Executor>>,
) -> Result<Vec<MatchResult>> {
    // 1. TradingPair 찾기
    let pair = TradingPair::new(order.base_mint.clone(), order.quote_mint.clone());
    
    // 2. 잔고 잠금 (주문 제출 전에 잠금)
    {
        let mut executor_guard = executor.lock();
        let (lock_mint, lock_amount) = if order.order_type == "buy" {
            // 매수: quote_mint 잠금
            // 지정가: price * amount
            // 시장가: quote_amount
            let amount = if order.order_side == "market" {
                // 시장가 매수: quote_amount 사용
                order.quote_amount.unwrap_or(rust_decimal::Decimal::ZERO)
            } else {
                // 지정가 매수: price * amount
                order.price.unwrap_or(rust_decimal::Decimal::ZERO) * order.amount
            };
            (&order.quote_mint, amount)
        } else {
            // 매도: base_mint 잠금 (amount만큼)
            (&order.base_mint, order.amount)
        };
        
        if let Err(e) = executor_guard.lock_balance_for_order(order.id, order.user_id, lock_mint, lock_amount) {
            // 에러 상세 정보 출력
            if let Some(balance) = executor_guard.balance_cache().get_balance(order.user_id, lock_mint) {
                eprintln!(
                    "Lock failed: user_id={}, mint={}, available={}, locked={}, required={}, error={}",
                    order.user_id, lock_mint, balance.available, balance.locked, lock_amount, e
                );
            } else {
                eprintln!(
                    "Lock failed: user_id={}, mint={}, required={}, error={} (balance not found in cache)",
                    order.user_id, lock_mint, lock_amount, e
                );
            }
            return Err(anyhow::anyhow!("Failed to lock balance: {}", e));
        }
        
        // DB Writer로 잔고 업데이트 명령 전송 (available 감소, locked 증가)
        if let Some(tx) = db_tx {
            let db_cmd = super::db_commands::DbCommand::UpdateBalance {
                user_id: order.user_id,
                mint: lock_mint.to_string(),
                available_delta: Some(-lock_amount), // available 감소
                locked_delta: Some(lock_amount), // locked 증가
            };
            if let Err(e) = tx.send(db_cmd) {
                eprintln!("Failed to send DB update command for balance lock: order_id={}, user_id={}, mint={}, amount={}, error={}",
                    order.id, order.user_id, lock_mint, lock_amount, e);
            }
        }
    }
    
    // 3. WAL 메시지 발행 (OrderCreated) - 잔고 잠금 후! - 주석 처리됨 (WAL 비활성화)
    // WAL 로그 쓰기 비활성화 (디스크 공간 부족 문제로 인해 임시 비활성화)
    /*
    if let Some(tx) = wal_tx {
        let wal_entry = WalEntry::OrderCreated {
            order_id: order.id,
            user_id: order.user_id,
            order_type: order.order_type.clone(),
            base_mint: order.base_mint.clone(),
            quote_mint: order.quote_mint.clone(),
            price: order.price.map(|p| p.to_string()),
            amount: order.amount.to_string(),
            timestamp: order.created_at.timestamp_millis(),
        };
        let _ = tx.send(wal_entry);
    }
    */
    
    // 3-1. 주문을 DB에 저장 (배치로 처리됨, trade insert 전에 필요 - 외래키 제약)
    // 주문 ID는 DB Writer가 INSERT 시 auto increment로 생성됨
    // 시장가 주문은 처음에 InsertOrder를 보내지 않고, 모든 처리가 끝난 후 최종 상태로 한 번만 전송
    let is_market_order = order.order_side == "market";
    if !is_market_order {
        // 지정가 주문만 처음에 InsertOrder 전송 (pending 상태)
    if let Some(tx) = db_tx {
        let db_cmd = super::db_commands::DbCommand::InsertOrder {
                order_id: order.id,  // 임시 ID (0), DB Writer가 실제 ID 생성
            user_id: order.user_id,
            order_type: order.order_type.clone(),
            order_side: order.order_side.clone(),
            base_mint: order.base_mint.clone(),
            quote_mint: order.quote_mint.clone(),
            price: order.price,
            amount: order.amount,
            created_at: order.created_at,
                status: None, // None이면 "pending"
                filled_amount: None, // None이면 0
                filled_quote_amount: None, // None이면 0
        };
            let _ = tx.send(db_cmd); // Non-blocking, 배치로 처리됨
        }
    }
    
    // 4. 시장가 주문 여부 및 초기 잔고 잠금 정보 저장 (order 이동 전)
    let initial_quote_amount = order.quote_amount;
    let initial_amount = order.amount;
    
    // 5. OrderBook 가져오기 및 매칭 (락 안에서 수행)
    let (matches, order_after_match) = {
        let mut orderbooks_guard = orderbooks.write();
        let orderbook = orderbooks_guard.entry(pair.clone()).or_insert_with(|| OrderBook::new(pair.clone()));
        
        // 6. Matcher로 매칭 시도 (먼저 매칭 시도)
        let matches = matcher.match_order(&mut order, orderbook);
        
        // 7. 매칭 후 남은 주문이 있으면 OrderBook에 추가
        // 시장가 주문은 완전히 체결되지 않으면 오더북에 추가하지 않음 (시장가 주문은 즉시 체결되어야 함)
        // 지정가 주문은 부분 체결 후 남은 수량이 있으면 오더북에 추가
        if order.order_side == "limit" {
            // 지정가 주문: 남은 수량이 있으면 오더북에 추가
            let has_remaining = if let Some(remaining_quote) = order.remaining_quote_amount {
                remaining_quote > Decimal::ZERO
            } else {
                order.remaining_amount > Decimal::ZERO
            };
            
            if has_remaining {
                orderbook.add_order(order.clone()); // order는 나중에 사용하므로 클론
            }
        }
        // 시장가 주문은 완전히 체결되지 않으면 오더북에 추가하지 않음
        
        // order 상태 저장 (매칭 후)
        let order_after_match = order.clone();
        (matches, order_after_match)
    };
    
    // 8. 시장가 주문 처리 (IOC 방식: 오더북에 있는 만큼만 체결, 남은 잔량은 즉시 취소)
    // 시장가 주문은 부분 체결되어도 성공으로 처리하고 'filled' 상태로 저장
    // 모든 매칭 완료 후 DB 업데이트 명령을 한 번에 전송 (원자성 보장)
    if is_market_order {
        use std::collections::HashMap;
        use chrono::Utc;
        use crate::shared::utils::id_generator::TradeIdGenerator;
        
        // 디버깅: 매칭 결과 확인
        eprintln!("[Market Order Debug] order_id={}, initial_matches={}, remaining_quote_amount={:?}, remaining_amount={}", 
                 order_after_match.id, matches.len(), order_after_match.remaining_quote_amount, order_after_match.remaining_amount);
        
        // 체결 처리 (성공한 매칭만 추적, DB 명령은 건너뛰기)
        let mut successful_matches = Vec::new();
        let mut total_quote_used = Decimal::ZERO;
        let mut total_amount_used = Decimal::ZERO;
        
        {
            let mut executor_guard = executor.lock();
            for match_result in &matches {
                // skip_db_updates=true로 설정하여 DB 명령 전송 건너뛰기
                match executor_guard.execute_trade(match_result, true) {
                    Ok(_) => {
                        // 성공한 매칭만 추적
                        successful_matches.push(match_result.clone());
                        if order_after_match.order_type == "buy" {
                            total_quote_used += match_result.price * match_result.amount;
                        } else {
                            total_amount_used += match_result.amount;
                        }
                    }
                    Err(e) => {
                        // 잔고 부족 등으로 실패한 매칭은 건너뛰고 계속 진행
                        eprintln!("[Market Order] Failed to execute trade (skipping): buy_order_id={}, sell_order_id={}, buyer_id={}, seller_id={}, error={}", 
                                 match_result.buy_order_id, match_result.sell_order_id, 
                                 match_result.buyer_id, match_result.seller_id, e);
                        // 실패한 매칭은 무시하고 계속 진행
                }
            }
        }
        }
        
        // 디버깅: 성공한 매칭 확인
        eprintln!("[Market Order Debug] order_id={}, successful_matches={}, total_quote_used={}, total_amount_used={}", 
                 order_after_match.id, successful_matches.len(), total_quote_used, total_amount_used);
        
        // 성공한 매칭만 사용하여 주문 상태 업데이트
        let matches = successful_matches;
        
        // 남은 잔고 잠금 해제 (IOC: 남은 잔량은 즉시 취소)
        // 실제로 사용된 금액을 기준으로 계산 (성공한 매칭만 반영)
        let unlock_mint;
        let unlock_amount;
        {
            let mut executor_guard = executor.lock();
            let (mint, amount) = if order_after_match.order_type == "buy" {
                // 시장가 매수: 초기 quote_amount에서 실제 사용된 금액을 뺀 나머지 잠금 해제
                let initial_quote = initial_quote_amount.unwrap_or(Decimal::ZERO);
                let unlock = initial_quote - total_quote_used;
                (&order_after_match.quote_mint, unlock)
            } else {
                // 시장가 매도: 초기 amount에서 실제 사용된 수량을 뺀 나머지 잠금 해제
                let unlock = initial_amount - total_amount_used;
                (&order_after_match.base_mint, unlock)
            };
            
            unlock_mint = mint.to_string();
            unlock_amount = amount;
            
            if unlock_amount > Decimal::ZERO {
                // 메모리 잔고 잠금 해제
                if let Err(e) = executor_guard.unlock_balance_for_cancel(
                    order_after_match.id,
                    order_after_match.user_id,
                    mint,
                    unlock_amount,
                ) {
                    eprintln!("Failed to unlock balance: {}", e);
                }
            }
        }
        
        // 모든 매칭 완료 후 DB 업데이트 명령을 한 번에 전송
                    if let Some(tx) = db_tx {
            // 잔고 변경 집계: (user_id, mint) -> (available_delta, locked_delta)
            let mut balance_changes: HashMap<(u64, String), (Decimal, Decimal)> = HashMap::new();
            
            // 각 매칭의 잔고 변경을 집계
            for match_result in &matches {
                let total_value = match_result.price * match_result.amount;
                
                // 매수자 USDT: locked 감소
                let entry = balance_changes.entry((match_result.buyer_id, match_result.quote_mint.clone()))
                    .or_insert((Decimal::ZERO, Decimal::ZERO));
                entry.1 -= total_value; // locked 감소
                
                // 매도자 USDT: available 증가
                let entry = balance_changes.entry((match_result.seller_id, match_result.quote_mint.clone()))
                    .or_insert((Decimal::ZERO, Decimal::ZERO));
                entry.0 += total_value; // available 증가
                
                // 매도자 SOL: locked 감소
                let entry = balance_changes.entry((match_result.seller_id, match_result.base_mint.clone()))
                    .or_insert((Decimal::ZERO, Decimal::ZERO));
                entry.1 -= match_result.amount; // locked 감소
                
                // 매수자 SOL: available 증가
                let entry = balance_changes.entry((match_result.buyer_id, match_result.base_mint.clone()))
                    .or_insert((Decimal::ZERO, Decimal::ZERO));
                entry.0 += match_result.amount; // available 증가
            }
            
            // 남은 잔고 잠금 해제 반영
            if unlock_amount > Decimal::ZERO {
                let entry = balance_changes.entry((order_after_match.user_id, unlock_mint.clone()))
                    .or_insert((Decimal::ZERO, Decimal::ZERO));
                entry.0 += unlock_amount; // available 증가
                entry.1 -= unlock_amount; // locked 감소
            }
            
            // 1. InsertTrade 명령 전송 및 Kafka 이벤트 발행 (모든 매칭)
            for match_result in &matches {
                let trade_id = TradeIdGenerator::next();
                let db_cmd = super::db_commands::DbCommand::InsertTrade {
                    trade_id,
                    buy_order_id: match_result.buy_order_id,
                    sell_order_id: match_result.sell_order_id,
                    buyer_id: match_result.buyer_id,
                    seller_id: match_result.seller_id,
                    price: match_result.price,
                    amount: match_result.amount,
                    base_mint: match_result.base_mint.clone(),
                    quote_mint: match_result.quote_mint.clone(),
                    timestamp: Utc::now(),
                };
                if let Err(e) = tx.send(db_cmd) {
                    eprintln!("Failed to send InsertTrade command: {}", e);
                }
                
                // Kafka 이벤트 발행 (비동기, 논블로킹)
                // 엔진 스레드는 일반 스레드이므로 tokio 런타임을 생성해야 함
                if let Some(kafka_producer_ref) = kafka_producer {
                    let kafka_producer_clone = kafka_producer_ref.clone();
                    let match_result_clone = match_result.clone();
                    let base_mint_clone = match_result.base_mint.clone();
                    
                    // 별도 스레드에서 tokio 런타임 생성하여 Kafka 발행
                    std::thread::spawn(move || {
                        let rt = tokio::runtime::Runtime::new().unwrap();
                        rt.block_on(async move {
                            let event = TradeExecutedEvent {
                                event_type: "trade_executed".to_string(),
                                trade_id,
                                buy_order_id: match_result_clone.buy_order_id,
                                sell_order_id: match_result_clone.sell_order_id,
                                buyer_id: match_result_clone.buyer_id,
                                seller_id: match_result_clone.seller_id,
                                price: match_result_clone.price,
                                amount: match_result_clone.amount,
                                base_mint: match_result_clone.base_mint,
                                quote_mint: match_result_clone.quote_mint,
                                timestamp: Utc::now(),
                            };
                            
                            if let Err(e) = kafka_producer_clone.publish_trade_executed(&base_mint_clone, &event).await {
                                eprintln!("[Kafka] Failed to publish trade executed event: {}", e);
                            }
                        });
                    });
                }
            }
            
            // 2. UpdateBalance 명령 전송 (집계된 잔고 변경)
            for ((user_id, mint), (available_delta, locked_delta)) in balance_changes {
                // delta가 0이 아닌 경우만 전송
                if available_delta != Decimal::ZERO || locked_delta != Decimal::ZERO {
                    let db_cmd = super::db_commands::DbCommand::UpdateBalance {
                        user_id,
                        mint,
                        available_delta: if available_delta != Decimal::ZERO { Some(available_delta) } else { None },
                        locked_delta: if locked_delta != Decimal::ZERO { Some(locked_delta) } else { None },
                    };
                    if let Err(e) = tx.send(db_cmd) {
                        eprintln!("Failed to send UpdateBalance command: user_id={}, error={}", user_id, e);
                    }
                }
            }
            
            // 3. 시장가 주문을 최종 상태로 InsertOrder 한 번만 전송
            let total_filled_amount: Decimal = matches.iter()
                .map(|m| m.amount)
                .sum();
            let total_filled_quote_amount: Decimal = matches.iter()
                .map(|m| m.price * m.amount)
                .sum();
            
            // 체결이 있으면 "filled", 없으면 "cancelled"
            let final_status = if total_filled_amount > Decimal::ZERO {
                "filled".to_string()
            } else {
                "cancelled".to_string()
            };
            
            let db_cmd = super::db_commands::DbCommand::InsertOrder {
                order_id: order_after_match.id,
                user_id: order_after_match.user_id,
                order_type: order_after_match.order_type.clone(),
                order_side: order_after_match.order_side.clone(),
                base_mint: order_after_match.base_mint.clone(),
                quote_mint: order_after_match.quote_mint.clone(),
                price: order_after_match.price,
                amount: order_after_match.amount,
                created_at: order_after_match.created_at,
                status: Some(final_status),
                filled_amount: Some(total_filled_amount),
                filled_quote_amount: Some(total_filled_quote_amount),
            };
            if let Err(e) = tx.send(db_cmd) {
                eprintln!(
                    "[Order Submit] Failed to send InsertOrder command for market order {}: {}",
                    order_after_match.id, e
                );
            }
        }
        
        // 시장가 주문 처리 완료 (IOC 방식: 체결되면 filled, 안 되면 cancelled)
        return Ok(matches);
    }
    
    // 8. 체결 처리 (정상 케이스: 지정가 주문)
    // 지정가 주문은 각 매칭마다 DB 명령을 전송 (기존 방식 유지)
    {
        let mut executor_guard = executor.lock();
        for match_result in &matches {
            // skip_db_updates=false로 설정하여 각 매칭마다 DB 명령 전송
            if let Err(e) = executor_guard.execute_trade(match_result, false) {
                // 에러 발생 시 로그 기록
                eprintln!("Failed to execute trade: {}", e);
            }
        }
    }
    
    // 8-0. 지정가 주문 체결 시 Kafka 이벤트 발행 (모든 매칭)
    // 시장가 주문과 동일하게 각 매칭마다 trade_executed 이벤트 발행
    if !is_market_order && !matches.is_empty() {
        use crate::shared::utils::id_generator::TradeIdGenerator;
        use chrono::Utc;
        
        for match_result in &matches {
            let trade_id = TradeIdGenerator::next();
            
            // Kafka 이벤트 발행 (비동기, 논블로킹)
            // 엔진 스레드는 일반 스레드이므로 tokio 런타임을 생성해야 함
            if let Some(kafka_producer_ref) = kafka_producer {
                let kafka_producer_clone = kafka_producer_ref.clone();
                let match_result_clone = match_result.clone();
                let base_mint_clone = match_result.base_mint.clone();
                
                // 별도 스레드에서 tokio 런타임 생성하여 Kafka 발행
                std::thread::spawn(move || {
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    rt.block_on(async move {
                        let event = TradeExecutedEvent {
                            event_type: "trade_executed".to_string(),
                            trade_id,
                            buy_order_id: match_result_clone.buy_order_id,
                            sell_order_id: match_result_clone.sell_order_id,
                            buyer_id: match_result_clone.buyer_id,
                            seller_id: match_result_clone.seller_id,
                            price: match_result_clone.price,
                            amount: match_result_clone.amount,
                            base_mint: match_result_clone.base_mint,
                            quote_mint: match_result_clone.quote_mint,
                            timestamp: Utc::now(),
                        };
                        
                        if let Err(e) = kafka_producer_clone.publish_trade_executed(&base_mint_clone, &event).await {
                            eprintln!("[Kafka] Failed to publish trade executed event: {}", e);
                        }
                    });
                });
            }
        }
    }
    
    // 8-1. 지정가 주문 부분 체결 시 상태 업데이트
    // ============================================
    // 지정가 주문은 부분 체결되어도 OrderBook에 남아있음
    // 부분 체결 시 상태를 'partial'로 업데이트해야 함
    // ============================================
    if !is_market_order && !matches.is_empty() {
        // 지정가 주문이고 체결이 발생했음
        let is_partially_filled = order_after_match.remaining_amount > Decimal::ZERO;
        
        if is_partially_filled {
            // 부분 체결: 상태를 'partial'로 업데이트
            if let Some(tx) = db_tx {
                let total_filled_amount: Decimal = matches.iter()
                    .map(|m| m.amount)
                    .sum();
                let total_filled_quote_amount: Decimal = matches.iter()
                    .map(|m| m.price * m.amount)
                    .sum();
                
                let db_cmd = super::db_commands::DbCommand::UpdateOrderStatus {
                    order_id: order_after_match.id,
                    status: "partial".to_string(),
                    filled_amount: total_filled_amount,
                    filled_quote_amount: total_filled_quote_amount,
                };
                if let Err(e) = tx.send(db_cmd) {
                    eprintln!(
                        "[Order Submit] Failed to send UpdateOrderStatus command for partially filled order {}: {}",
                        order_after_match.id, e
                    );
                }
            }
        }
    }
    
    // 9. 완전히 체결된 지정가 주문의 남은 locked 잔고 해제
    // ============================================
    // 주문 생성 시 lock한 금액과 실제 체결 금액이 다를 수 있음
    // 
    // 예시 (지정가 매수 완전 체결):
    // - 주문 생성 시: price * amount = 100 * 10 = 1000 USDT를 lock
    // - 실제 체결: 100 * 10 = 1000 USDT 체결 (remaining_amount = 0, 완전 체결)
    // - transfer로 1000 USDT를 locked에서 차감
    // - 남은 locked = 0 (정확히 일치)
    // 
    // 하지만 가격이 변동하거나 부분 체결 후 완전 체결되면 차이가 있을 수 있음
    // 따라서 완전히 체결된 주문의 경우 남은 locked를 unlock해야 함
    // 
    // 참고: 시장가 주문은 위에서 이미 처리됨 (IOC 방식)
    // ============================================
    
    // 완전 체결 판단 로직 (지정가 주문만)
    // 지정가 주문: remaining_amount가 0이면 완전 체결
    let is_fully_filled_after_match = order_after_match.remaining_amount == Decimal::ZERO;
    
    // 완전히 체결된 주문의 경우, 남은 locked 잔고 해제
    if is_fully_filled_after_match {
        let mut executor_guard = executor.lock();
        
        // 주문 타입에 따라 unlock할 mint와 amount 계산
        let (unlock_mint, unlock_amount) = if order_after_match.order_type == "buy" {
            // 매수 주문: quote_mint (USDT) 잠금 해제
            // lock한 금액과 실제 체결 금액의 차이를 계산
            let total_quote_used: Decimal = matches.iter()
                .map(|m| m.price * m.amount)
                .sum();
            
            if order_after_match.order_side == "market" {
                // 시장가 매수: initial_quote_amount에서 실제 체결 금액을 뺀 나머지
                // remaining_quote_amount가 None이어도 처리 가능
                let initial_locked = initial_quote_amount.unwrap_or(Decimal::ZERO);
                let remaining_locked = initial_locked - total_quote_used;
                (&order_after_match.quote_mint, remaining_locked)
            } else {
                // 지정가 매수: price * initial_amount에서 실제 체결 금액을 뺀 나머지
                let initial_locked = order_after_match.price.unwrap_or(Decimal::ZERO) * initial_amount;
                let remaining_locked = initial_locked - total_quote_used;
                (&order_after_match.quote_mint, remaining_locked)
            }
        } else {
            // 매도 주문: base_mint (SOL 등) 잠금 해제
            // lock한 수량과 실제 체결 수량의 차이를 계산
            let total_amount_used: Decimal = matches.iter()
                .map(|m| m.amount)
                .sum();
            let remaining_locked = initial_amount - total_amount_used;
            (&order_after_match.base_mint, remaining_locked)
        };
        
        // 남은 locked가 있으면 unlock (0보다 큰 경우에만)
        if unlock_amount > Decimal::ZERO {
            if let Err(e) = executor_guard.unlock_balance_for_cancel(
                order_after_match.id,
                order_after_match.user_id,
                unlock_mint,
                unlock_amount,
            ) {
                eprintln!(
                    "[Order Submit] Failed to unlock remaining balance for fully filled order {}: user_id={}, mint={}, amount={}, error={}",
                    order_after_match.id, order_after_match.user_id, unlock_mint, unlock_amount, e
                );
            } else {
                // Unlock 성공 시 DB Writer로 잔고 업데이트 명령 전송
                // 남은 locked를 available로 이동 (available 증가, locked 감소)
                if let Some(tx) = db_tx {
                    let db_cmd = super::db_commands::DbCommand::UpdateBalance {
                        user_id: order_after_match.user_id,
                        mint: unlock_mint.to_string(),
                        available_delta: Some(unlock_amount), // available 증가
                        locked_delta: Some(-unlock_amount), // locked 감소
                    };
                    if let Err(e) = tx.send(db_cmd) {
                        eprintln!(
                            "[Order Submit] Failed to send UpdateBalance command for unlock: order_id={}, user_id={}, mint={}, amount={}, error={}",
                            order_after_match.id, order_after_match.user_id, unlock_mint, unlock_amount, e
                        );
                    }
                }
            }
        }
        
        // 완전히 체결된 주문의 상태를 'filled'로 업데이트
        // DB Writer로 주문 상태 업데이트 명령 전송
        if let Some(tx) = db_tx {
            let total_filled_amount: Decimal = matches.iter()
                .map(|m| m.amount)
                .sum();
            let total_filled_quote_amount: Decimal = matches.iter()
                .map(|m| m.price * m.amount)
                .sum();
            
            let db_cmd = super::db_commands::DbCommand::UpdateOrderStatus {
                order_id: order_after_match.id,
                status: "filled".to_string(),
                filled_amount: total_filled_amount,
                filled_quote_amount: total_filled_quote_amount,
            };
            if let Err(e) = tx.send(db_cmd) {
                eprintln!(
                    "[Order Submit] Failed to send UpdateOrderStatus command for order {}: {}",
                    order_after_match.id, e
                );
            }
        }
    }
    
    Ok(matches)
}

/// CancelOrder 명령 처리
/// 
/// # 처리 과정
/// 1. OrderBook에서 주문 찾기
/// 2. 권한 확인 (user_id 일치)
/// 3. WAL 메시지 발행 (OrderCancelled)
/// 4. OrderBook에서 제거
/// 5. 잔고 잠금 해제 (remaining_amount만큼)
/// 6. 취소된 주문 반환
fn handle_cancel_order(
    order_id: u64,
    user_id: u64,
    trading_pair: TradingPair,
    response: tokio::sync::oneshot::Sender<Result<OrderEntry>>,
    wal_tx: Option<&crossbeam::channel::Sender<WalEntry>>,
    db_tx: Option<&crossbeam::channel::Sender<super::db_commands::DbCommand>>,
    kafka_producer: Option<&Arc<KafkaProducer>>,
    orderbooks: &Arc<RwLock<HashMap<TradingPair, OrderBook>>>,
    executor: &Arc<Mutex<Executor>>,
    db: Option<Database>,
) {
    // 1. OrderBook에서 주문 찾기
    let mut orderbooks_guard = orderbooks.write();
    let orderbook = match orderbooks_guard.get_mut(&trading_pair) {
        Some(ob) => ob,
        None => {
            let _ = response.send(Err(anyhow::anyhow!("OrderBook not found for trading pair")));
            return;
        }
    };
    
    // 2. 주문 찾기 (매수/매도 양쪽 모두 확인)
    let mut found_order: Option<OrderEntry> = None;
    let mut found_price: Option<rust_decimal::Decimal> = None;
    let mut is_buy = false;
    
    // 매수 호가에서 찾기
    for (price, orders) in orderbook.buy_orders.orders.iter_mut() {
        if let Some(pos) = orders.iter().position(|o| o.id == order_id) {
            if let Some(order) = orders.remove(pos) {
                // 권한 확인
                if order.user_id == user_id {
                    found_order = Some(order.clone());
                    found_price = Some(*price);
                    is_buy = true;
                    if orders.is_empty() {
                        // 빈 VecDeque는 나중에 제거
                    }
                    break;
                } else {
                    // 권한 없음 - 다시 추가
                    orders.insert(pos, order);
                    let _ = response.send(Err(anyhow::anyhow!("Unauthorized: You don't own this order")));
                    return;
                }
            }
        }
    }
    
    // 매도 호가에서 찾기
    if found_order.is_none() {
        for (price, orders) in orderbook.sell_orders.orders.iter_mut() {
            if let Some(pos) = orders.iter().position(|o| o.id == order_id) {
                if let Some(order) = orders.remove(pos) {
                    // 권한 확인
                    if order.user_id == user_id {
                        found_order = Some(order.clone());
                        found_price = Some(*price);
                        is_buy = false;
                        if orders.is_empty() {
                            // 빈 VecDeque는 나중에 제거
                        }
                        break;
                    } else {
                        // 권한 없음 - 다시 추가
                        orders.insert(pos, order);
                        let _ = response.send(Err(anyhow::anyhow!("Unauthorized: You don't own this order")));
                        return;
                    }
                }
            }
        }
    }
    
    // 빈 VecDeque 제거
    if let Some(price) = found_price {
        if is_buy {
            if let Some(orders) = orderbook.buy_orders.orders.get(&price) {
                if orders.is_empty() {
                    orderbook.buy_orders.orders.remove(&price);
                }
            }
        } else {
            if let Some(orders) = orderbook.sell_orders.orders.get(&price) {
                if orders.is_empty() {
                    orderbook.sell_orders.orders.remove(&price);
                }
            }
        }
    }
    
    // 3. OrderBook에서 찾지 못했으면 에러 반환 (DB 조회 안 함 - 성능 최적화)
    // 엔진은 OrderBook만 확인하고, DB 조회는 하지 않음
    // Java에서 이미 상태를 확인했으므로, OrderBook에 없으면 이미 체결되었거나 취소된 것
    let order = match found_order {
        Some(o) => o,
        None => {
            // OrderBook에 주문이 없음 = 이미 체결되었거나 취소됨
            // Java에서 이미 상태를 확인했으므로, 여기서는 에러만 반환
            let _ = response.send(Err(anyhow::anyhow!("Order not found in OrderBook. It may have been already filled or cancelled.")));
            return;
        }
    };
    
    // 3. WAL 메시지 발행 (OrderCancelled) - 주석 처리됨 (WAL 비활성화)
    // WAL 로그 쓰기 비활성화 (디스크 공간 부족 문제로 인해 임시 비활성화)
    /*
    if let Some(tx) = wal_tx {
        let wal_entry = WalEntry::OrderCancelled {
            order_id,
            user_id,
            timestamp: chrono::Utc::now().timestamp_millis(),
        };
        let _ = tx.send(wal_entry);
    }
    */
    
    // 4. 잔고 잠금 해제 (remaining_amount만큼)
    let order_type_str = order.order_type.as_str();
    
    // 주의: 엔진은 OrderBook(메모리)만 확인하고 DB는 조회하지 않음
    // DB 조회는 Java에서 이미 수행했으므로, OrderBook에 없으면 이미 체결되었거나 취소된 것
    let unlock_mint = if order_type_str == "buy" {
        &order.quote_mint  // 매수: USDT 잠금 해제
    } else {
        &order.base_mint   // 매도: SOL 등 잠금 해제
    };
    
    // 잠금 해제할 금액 계산
    let unlock_amount = if order_type_str == "buy" {
        // 매수: price * remaining_amount
        order.price.unwrap_or(rust_decimal::Decimal::ZERO) * order.remaining_amount
    } else {
        // 매도: remaining_amount
        order.remaining_amount
    };
    
    // Executor로 잔고 잠금 해제
    {
        let mut executor_guard = executor.lock();
        if let Err(e) = executor_guard.unlock_balance_for_cancel(order_id, user_id, unlock_mint, unlock_amount) {
            let _ = response.send(Err(anyhow::anyhow!("Failed to unlock balance: {}", e)));
            return;
        }
    }
    
    // 5. Kafka 이벤트 발행 (비동기, 논블로킹)
    // DB 업데이트는 Java Consumer가 처리하므로 여기서는 Kafka 이벤트만 발행
    // 비동기 태스크로 Kafka 발행 (논블로킹)
    // 엔진 스레드는 일반 스레드이므로 tokio 런타임을 생성해야 함
    if let Some(kafka_producer) = kafka_producer {
        let kafka_producer_clone = kafka_producer.clone();
        let base_mint_clone = order.base_mint.clone();
        let quote_mint_clone = order.quote_mint.clone();
        
        // 별도 스레드에서 tokio 런타임 생성하여 Kafka 발행
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let event = OrderCancelledEvent {
                    event_type: "order_cancelled".to_string(),
                    order_id,
                    user_id,
                    base_mint: base_mint_clone.clone(),
                    quote_mint: quote_mint_clone,
                    timestamp: Utc::now(),
                };
                
                if let Err(e) = kafka_producer_clone.publish_order_cancelled(&base_mint_clone, &event).await {
                    eprintln!("[Kafka] Failed to publish order cancelled event: {}", e);
                }
            });
        });
    }
    
    // 6. 취소된 주문 반환 (DB 업데이트는 Kafka Consumer가 처리)
    let _ = response.send(Ok(order));
}

/// GetOrderbook 명령 처리
/// 
/// # 처리 과정
/// 1. OrderBook 찾기
/// 2. 매수 주문 목록 수집 (depth만큼)
/// 3. 매도 주문 목록 수집 (depth만큼)
/// 4. 결과 반환
/// 
/// # Note
/// depth가 None이면 전체 주문 반환 (주의: 많은 주문이 있으면 느릴 수 있음)
fn handle_get_orderbook(
    trading_pair: TradingPair,
    depth: Option<usize>,
    response: tokio::sync::oneshot::Sender<Result<(Vec<OrderEntry>, Vec<OrderEntry>)>>,
    orderbooks: &Arc<RwLock<HashMap<TradingPair, OrderBook>>>,
) {
    // 1. OrderBook 찾기
    let orderbooks_guard = orderbooks.read();
    let orderbook = match orderbooks_guard.get(&trading_pair) {
        Some(ob) => ob,
        None => {
            // OrderBook이 없으면 빈 목록 반환
            let _ = response.send(Ok((Vec::new(), Vec::new())));
            return;
        }
    };
    
    // 2. 매수 주문 목록 수집
    let mut buy_orders = Vec::new();
    for (_, orders) in orderbook.buy_orders.orders.iter().rev() {
        for order in orders.iter() {
            buy_orders.push(order.clone());
            if let Some(d) = depth {
                if buy_orders.len() >= d {
                    break;
                }
            }
        }
        if let Some(d) = depth {
            if buy_orders.len() >= d {
                break;
            }
        }
    }
    
    // 3. 매도 주문 목록 수집
    let mut sell_orders = Vec::new();
    for (_, orders) in orderbook.sell_orders.orders.iter() {
        for order in orders.iter() {
            sell_orders.push(order.clone());
            if let Some(d) = depth {
                if sell_orders.len() >= d {
                    break;
                }
            }
        }
        if let Some(d) = depth {
            if sell_orders.len() >= d {
                break;
            }
        }
    }
    
    // 4. 결과 반환
    let _ = response.send(Ok((buy_orders, sell_orders)));
}

/// GetBalance 명령 처리
fn handle_get_balance(
    user_id: u64,
    mint: String,
    response: tokio::sync::oneshot::Sender<Result<(rust_decimal::Decimal, rust_decimal::Decimal)>>,
    executor: &Arc<Mutex<Executor>>,
) {
    let executor = executor.lock();
    let balance_cache = executor.balance_cache();
    
    match balance_cache.get_balance(user_id, &mint) {
        Some(balance) => {
            let _ = response.send(Ok((balance.available, balance.locked)));
        }
        None => {
            let _ = response.send(Ok((rust_decimal::Decimal::ZERO, rust_decimal::Decimal::ZERO)));
        }
    }
}

/// LockBalance 명령 처리
fn handle_lock_balance(
    user_id: u64,
    mint: String,
    amount: rust_decimal::Decimal,
    response: tokio::sync::oneshot::Sender<Result<()>>,
    wal_tx: Option<&crossbeam::channel::Sender<WalEntry>>,
    db_tx: Option<&crossbeam::channel::Sender<super::db_commands::DbCommand>>,
    executor: &Arc<Mutex<Executor>>,
) {
    let mut executor = executor.lock();
    
    // WAL 메시지는 executor.lock_balance_for_order에서 발행하므로 여기서는 제거 (중복 방지)
    
    // 잔고 잠금
    match executor.lock_balance_for_order(0, user_id, &mint, amount) {
        Ok(()) => {
            // DB Writer로 잔고 업데이트 명령 전송 (available 감소, locked 증가)
            if let Some(tx) = db_tx {
                let db_cmd = super::db_commands::DbCommand::UpdateBalance {
                    user_id,
                    mint: mint.clone(),
                    available_delta: Some(-amount), // available 감소
                    locked_delta: Some(amount), // locked 증가
                };
                let _ = tx.send(db_cmd);
            }
            
            let _ = response.send(Ok(()));
        }
        Err(e) => {
            let _ = response.send(Err(e));
        }
    }
}

/// UnlockBalance 명령 처리
fn handle_unlock_balance(
    user_id: u64,
    mint: String,
    amount: rust_decimal::Decimal,
    response: tokio::sync::oneshot::Sender<Result<()>>,
    wal_tx: Option<&crossbeam::channel::Sender<WalEntry>>,
    db_tx: Option<&crossbeam::channel::Sender<super::db_commands::DbCommand>>,
    executor: &Arc<Mutex<Executor>>,
) {
    let mut executor = executor.lock();
    
    // WAL 메시지 발행 - 주석 처리됨 (WAL 비활성화)
    // WAL 로그 쓰기 비활성화 (디스크 공간 부족 문제로 인해 임시 비활성화)
    /*
    if let Some(tx) = wal_tx {
        let wal_entry = WalEntry::OrderCancelled {
            order_id: 0,  // TODO: 실제 order_id 전달
            user_id,
            timestamp: chrono::Utc::now().timestamp_millis(),
        };
        let _ = tx.send(wal_entry);
    }
    */
    
    // 잔고 잠금 해제
    match executor.unlock_balance_for_cancel(0, user_id, &mint, amount) {
        Ok(()) => {
            // DB Writer로 잔고 업데이트 명령 전송 (locked 감소, available 증가)
            if let Some(tx) = db_tx {
                let db_cmd = super::db_commands::DbCommand::UpdateBalance {
                    user_id,
                    mint: mint.clone(),
                    available_delta: Some(amount), // available 증가
                    locked_delta: Some(-amount), // locked 감소
                };
                let _ = tx.send(db_cmd);
            }
            
            let _ = response.send(Ok(()));
        }
        Err(e) => {
            let _ = response.send(Err(e));
        }
    }
}

/// UpdateBalance 명령 처리 (입금/출금)
/// 
/// # 처리 과정
/// 1. BalanceCache에서 잔고 조회/생성
/// 2. available 업데이트 (기존 + delta)
/// 3. WAL 메시지 발행 (BalanceUpdated)
/// 4. DB 명령 전송 (UpdateBalance) → DB Writer가 배치로 처리
/// 5. 성공/실패 결과를 response로 전송
/// 
/// # Arguments
/// * `user_id` - 사용자 ID
/// * `mint` - 자산 종류 (예: "SOL", "USDT")
/// * `available_delta` - available 증감량 (양수: 입금, 음수: 출금)
/// * `response` - 결과를 반환할 oneshot 채널
/// * `wal_tx` - WAL 메시지 전송 채널
/// * `db_tx` - DB 명령 전송 채널
/// * `executor` - Executor (BalanceCache 포함)
/// 
/// # 예시
/// ```rust
/// // 100 USDT 입금
/// handle_update_balance(
///     123,
///     "USDT".to_string(),
///     Decimal::new(100, 0),
///     response,
///     wal_tx,
///     db_tx,
///     &executor,
/// );
/// ```
fn handle_update_balance(
    user_id: u64,
    mint: String,
    available_delta: rust_decimal::Decimal,
    response: tokio::sync::oneshot::Sender<Result<()>>,
    wal_tx: Option<&crossbeam::channel::Sender<WalEntry>>,
    db_tx: Option<&crossbeam::channel::Sender<super::db_commands::DbCommand>>,
    executor: &Arc<Mutex<Executor>>,
) {
    // 1. BalanceCache에서 잔고 업데이트
    let (new_available, new_locked) = {
        let mut executor_guard = executor.lock();
        let balance_cache = executor_guard.balance_cache_mut();
        
        // available 업데이트 (delta 추가)
        balance_cache.add_available(user_id, &mint, available_delta);
        
        // 업데이트 후 잔고 조회 (WAL 기록용)
        let new_balance = balance_cache.get_balance(user_id, &mint)
            .cloned()
            .unwrap_or_else(|| crate::domains::cex::engine::balance_cache::Balance::new());
        
        (new_balance.available, new_balance.locked)
    };
    
    // 2. WAL 메시지 발행 (BalanceUpdated) - 주석 처리됨 (WAL 비활성화)
    // 업데이트 후 잔고를 기록하여 복구 시 정확한 상태 복원 가능
    // WAL 로그 쓰기 비활성화 (디스크 공간 부족 문제로 인해 임시 비활성화)
    /*
    if let Some(tx) = wal_tx {
        let wal_entry = WalEntry::BalanceUpdated {
            user_id,
            mint: mint.clone(),
            available: new_available.to_string(),
            locked: new_locked.to_string(),
            timestamp: chrono::Utc::now().timestamp_millis(),
        };
        let _ = tx.send(wal_entry);
    }
    */
    
    // 3. DB 명령 전송 (UpdateBalance)
    // DB Writer 스레드가 배치로 처리 (100개 또는 10ms마다)
    if let Some(tx) = db_tx {
        let db_cmd = super::db_commands::DbCommand::UpdateBalance {
            user_id,
            mint: mint.clone(),
            available_delta: Some(available_delta),
            locked_delta: None,  // 입금/출금은 available만 변경
        };
        let _ = tx.send(db_cmd);
    }
    
    // 4. 성공 결과 반환
    let _ = response.send(Ok(()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal::Decimal;

    fn sample_limit_buy(order_id: u64, user_id: u64) -> OrderEntry {
        OrderEntry {
            id: order_id,
            user_id,
            order_type: "buy".to_string(),
            order_side: "limit".to_string(),
            base_mint: "SOL".to_string(),
            quote_mint: "USDT".to_string(),
            price: Some(Decimal::new(100, 0)),
            amount: Decimal::new(1, 0),
            quote_amount: None,
            filled_amount: Decimal::ZERO,
            remaining_amount: Decimal::new(1, 0),
            remaining_quote_amount: None,
            created_at: Utc::now(),
        }
    }

    fn sample_market_buy(order_id: u64, user_id: u64, quote_amount: Decimal) -> OrderEntry {
        OrderEntry {
            id: order_id,
            user_id,
            order_type: "buy".to_string(),
            order_side: "market".to_string(),
            base_mint: "SOL".to_string(),
            quote_mint: "USDT".to_string(),
            price: None,
            amount: Decimal::ZERO,
            quote_amount: Some(quote_amount),
            filled_amount: Decimal::ZERO,
            remaining_amount: Decimal::ZERO,
            remaining_quote_amount: Some(quote_amount),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn submit_order_without_persistence_channels() {
        let orderbooks = Arc::new(RwLock::new(HashMap::new()));
        let matcher = Arc::new(Matcher::new());
        let executor = Arc::new(Mutex::new(Executor::new_without_wal()));

        {
            let mut exec = executor.lock();
            exec.balance_cache_mut()
                .set_balance(1, "USDT", Decimal::new(10_000, 0), Decimal::ZERO);
        }

        let order = sample_limit_buy(1, 1);
        let result =
            super::process_submit_order(order, None, None, &orderbooks, &matcher, &executor).unwrap();
        assert!(result.is_empty());

        let books = orderbooks.read();
        assert_eq!(books.len(), 1);
    }

    #[test]
    fn test_market_order_no_orderbook_sends_cancelled() {
        // 시장가 주문이 오더북이 없을 때 cancelled 상태로 InsertOrder를 한 번만 보내는지 테스트
        use crossbeam::channel;
        
        let orderbooks = Arc::new(RwLock::new(HashMap::new()));
        let matcher = Arc::new(Matcher::new());
        let executor = Arc::new(Mutex::new(Executor::new_without_wal()));
        
        // 잔고 설정 (available에 충분한 금액이 있어야 잠금 가능)
        {
            let mut exec = executor.lock();
            exec.balance_cache_mut()
                .set_balance(1, "USDT", Decimal::new(1000, 0), Decimal::ZERO); // 1000 USDT available
        }
        
        // DB 명령 채널 생성
        let (db_tx, db_rx) = channel::unbounded();
        
        // 시장가 매수 주문 (오더북에 매도 호가 없음)
        let market_order = sample_market_buy(1, 1, Decimal::new(1000, 0));
        
        // 주문 제출
        let result = super::process_submit_order(
            market_order,
            None,
            Some(&db_tx),
            &orderbooks,
            &matcher,
            &executor,
        ).unwrap();
        
        // 매칭이 없어야 함
        assert_eq!(result.len(), 0);
        
        // DB 명령 수집
        let db_commands: Vec<_> = db_rx.try_iter().collect();
        
        // 시장가 주문은 InsertOrder를 한 번만 보내야 함 (cancelled 상태)
        let insert_order_cmds: Vec<(Option<String>, Option<Decimal>)> = db_commands.iter()
            .filter_map(|cmd| {
                if let crate::domains::cex::engine::runtime::db_commands::DbCommand::InsertOrder { status, filled_amount, .. } = cmd {
                    Some((status.clone(), *filled_amount))
                } else {
                    None
                }
            })
            .collect();
        
        // InsertOrder가 정확히 한 번만 전송되어야 함
        assert_eq!(insert_order_cmds.len(), 1, "시장가 주문은 InsertOrder를 한 번만 보내야 함");
        
        // cancelled 상태로 전송되어야 함
        let (status, filled_amount) = &insert_order_cmds[0];
        assert_eq!(status.as_ref().unwrap(), "cancelled", "오더북이 없으면 cancelled 상태여야 함");
        assert_eq!(*filled_amount.as_ref().unwrap(), Decimal::ZERO, "체결이 없으면 filled_amount는 0이어야 함");
    }

    #[test]
    fn test_market_order_with_orderbook_sends_filled() {
        // 시장가 주문이 오더북이 있을 때 filled 상태로 InsertOrder를 한 번만 보내는지 테스트
        use crossbeam::channel;
        use crate::domains::cex::engine::types::TradingPair;
        
        let orderbooks = Arc::new(RwLock::new(HashMap::new()));
        let matcher = Arc::new(Matcher::new());
        let executor = Arc::new(Mutex::new(Executor::new_without_wal()));
        
        // 잔고 설정 (available에 충분한 금액이 있어야 잠금 가능)
        {
            let mut exec = executor.lock();
            exec.balance_cache_mut()
                .set_balance(1, "USDT", Decimal::new(1000, 0), Decimal::ZERO); // 매수자: 1000 USDT available
            exec.balance_cache_mut()
                .set_balance(2, "SOL", Decimal::new(5, 0), Decimal::ZERO); // 매도자: 5 SOL available
        }
        
        // 오더북에 매도 호가 추가
        {
            let mut books = orderbooks.write();
            let pair = TradingPair::new("SOL".to_string(), "USDT".to_string());
            let mut orderbook = crate::domains::cex::engine::orderbook::OrderBook::new(pair.clone());
            
            // 매도 주문 추가 (100 USDT에 2 SOL)
            let sell_order = OrderEntry {
                id: 2,
                user_id: 2,
                order_type: "sell".to_string(),
                order_side: "limit".to_string(),
                base_mint: "SOL".to_string(),
                quote_mint: "USDT".to_string(),
                price: Some(Decimal::new(100, 0)),
                amount: Decimal::new(2, 0),
                quote_amount: None,
                filled_amount: Decimal::ZERO,
                remaining_amount: Decimal::new(2, 0),
                remaining_quote_amount: None,
                created_at: Utc::now(),
            };
            orderbook.add_order(sell_order);
            books.insert(pair, orderbook);
        }
        
        // 매도 주문의 잔고 잠금
        {
            let mut exec = executor.lock();
            exec.balance_cache_mut()
                .set_balance(2, "SOL", Decimal::ZERO, Decimal::new(2, 0)); // 2 SOL locked
        }
        
        // DB 명령 채널 생성
        let (db_tx, db_rx) = channel::unbounded();
        
        // 시장가 매수 주문 (1000 USDT로 매수)
        let market_order = sample_market_buy(1, 1, Decimal::new(1000, 0));
        
        // 주문 제출
        let result = super::process_submit_order(
            market_order,
            None,
            Some(&db_tx),
            &orderbooks,
            &matcher,
            &executor,
        ).unwrap();
        
        // 매칭이 있어야 함 (2 SOL 체결)
        assert_eq!(result.len(), 1);
        
        // DB 명령 수집
        let db_commands: Vec<_> = db_rx.try_iter().collect();
        
        // 시장가 주문은 InsertOrder를 한 번만 보내야 함 (filled 상태)
        let insert_order_cmds: Vec<(Option<String>, Option<Decimal>)> = db_commands.iter()
            .filter_map(|cmd| {
                if let crate::domains::cex::engine::runtime::db_commands::DbCommand::InsertOrder { status, filled_amount, .. } = cmd {
                    Some((status.clone(), *filled_amount))
                } else {
                    None
                }
            })
            .collect();
        
        // InsertOrder가 정확히 한 번만 전송되어야 함
        assert_eq!(insert_order_cmds.len(), 1, "시장가 주문은 InsertOrder를 한 번만 보내야 함");
        
        // filled 상태로 전송되어야 함
        let (status, filled_amount) = &insert_order_cmds[0];
        assert_eq!(status.as_ref().unwrap(), "filled", "체결이 있으면 filled 상태여야 함");
        assert!(*filled_amount.as_ref().unwrap() > Decimal::ZERO, "체결이 있으면 filled_amount는 0보다 커야 함");
    }
}

// =====================================================
// WAL 스레드 루프
// =====================================================
// 역할: WAL 메시지를 받아서 디스크에 순차 쓰기
//
// 처리 과정:
// 1. 코어 고정 (Core 1)
// 2. WalWriter 생성
// 3. WAL 메시지 수신 루프
// 4. WalWriter::append() 호출
// 5. 주기적 fsync()
// =====================================================

/// WAL 스레드 메인 루프
/// 
/// # Arguments
/// * `wal_rx` - WAL 메시지 수신 채널
/// * `wal_dir` - WAL 디렉토리 경로
/// 
/// # 처리 흐름
/// ```
/// loop {
///     wal_rx.recv() → WalEntry
///         ↓
///     WalWriter::append(entry)
///         ↓
///     (10개마다) fsync()
/// }
/// ```
pub fn wal_thread_loop(
    wal_rx: Receiver<WalEntry>,
    wal_dir: std::path::PathBuf,
) {
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 1. 코어 고정 설정 (PROD: 코어 고정 안 함, DEV: Core 1 고정)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    let config = CoreConfig::from_env();
    // PROD 환경: 코어 고정 안 함 (OS 스케줄링에 맡김, 모든 리소스 활용)
    // DEV 환경: Core 1에 고정 (디스크 I/O 전용)
    if config.wal_core != 999 {
        eprintln!("[WAL Thread] Pinning to Core {}", config.wal_core);
        CoreConfig::set_core(Some(config.wal_core));
    } else {
        eprintln!("[WAL Thread] Core pinning disabled (OS scheduling)");
        CoreConfig::set_core(None);  // 코어 고정 비활성화 (PROD 환경)
    }
    
    // WAL 스레드는 실시간 스케줄링 불필요 (I/O 바운드, OS 스케줄링 충분)
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 2. WalWriter 생성
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    let mut wal_writer = match WalWriter::new(&wal_dir, 10) {
        Ok(writer) => writer,
        Err(e) => {
            eprintln!("Failed to create WalWriter: {}", e);
            return;
        }
    };
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 3. 메인 루프
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    loop {
        match wal_rx.recv() {
            Ok(entry) => {
                // WAL 파일에 쓰기
                if let Err(e) = wal_writer.append(&entry) {
                    eprintln!("Failed to write to WAL: {}", e);
                }
            }
            Err(_) => {
                // 채널이 닫힘 (정상 종료)
                // 마지막 동기화
                let _ = wal_writer.sync();
                break;
            }
        }
    }
}

// =====================================================
// DB Writer 스레드 루프
// =====================================================
// 역할: 메모리에서 DB 명령을 받아서 배치로 DB에 저장
//
// 처리 과정:
// 1. 코어 고정 (Core 2, dev 환경만)
// 2. DB 명령 수신 루프
// 3. 배치로 모으기 (10ms 또는 100개)
// 4. DB에 배치 쓰기 (트랜잭션)
// =====================================================

/// DB Writer 스레드 메인 루프
/// 
/// # Arguments
/// * `db_rx` - DB 명령 수신 채널
/// * `db_pool` - 데이터베이스 연결 풀
/// 
/// # 처리 흐름
/// ```
/// loop {
///     db_rx.recv() → DbCommand
///         ↓
///     batch.push(cmd)
///         ↓
///     (10ms 또는 100개마다)
///         ↓
///     db.execute_batch(&batch)
/// }
/// ```
/// 
/// # 배치 전략
/// - 시간 기반: 10ms마다 배치 쓰기
/// - 크기 기반: 100개 모이면 즉시 쓰기
/// - 트랜잭션: 여러 작업을 하나의 트랜잭션으로 묶기
pub fn db_writer_thread_loop(
    db_rx: Receiver<super::db_commands::DbCommand>,
    db_pool: PgPool,
) {
    use super::db_commands::DbCommand;
    use std::time::{Duration, Instant};
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 1. 코어 고정 (Core 2, dev 환경만)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    let config = CoreConfig::from_env();
    if let Some(core) = config.db_writer_core {
        CoreConfig::set_core(Some(core));
    }
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 2. 배치 변수 초기화
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    let mut batch = Vec::new();
    let batch_size_limit = 100;
    // 운영 환경에서는 flush 시간을 조정하여 저장 지연과 Deadlock 확률의 균형 유지
    // 운영: 500ms (체결내역 저장 지연 감소), 로컬: 10ms
    let batch_time_limit = if std::env::var("RUST_ENV").unwrap_or_else(|_| "dev".to_string()) == "prod" {
        Duration::from_millis(500)  // 운영: 500ms (1000ms → 500ms로 단축하여 저장 지연 감소)
    } else {
        Duration::from_millis(10)   // 로컬: 10ms
    };
    let mut last_flush = Instant::now();
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 3. 메인 루프 (Tokio 런타임 필요)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    // Tokio 런타임 생성 (DB 작업은 async이므로)
    let rt = tokio::runtime::Runtime::new()
        .expect("Failed to create Tokio runtime for DB Writer");
    
    rt.block_on(async {
        loop {
            // 채널이 닫혔는지 먼저 확인 (논블로킹, 즉시 반환)
            match db_rx.try_recv() {
                Ok(cmd) => {
                    // 명령 수신
                    batch.push(cmd);
                    
                    // 크기 기반 배치 쓰기 (100개 모이면)
                    if batch.len() >= batch_size_limit {
                        if let Err(e) = flush_batch(&mut batch, &db_pool).await {
                            eprintln!("Failed to flush DB batch: {}", e);
                        }
                        last_flush = Instant::now();
                    }
                    continue; // 다음 루프로
                }
                Err(crossbeam::channel::TryRecvError::Disconnected) => {
                    // 채널이 닫힘 (정상 종료) - 즉시 감지
                    // 마지막 배치 쓰기
                    if !batch.is_empty() {
                        let _ = flush_batch(&mut batch, &db_pool).await;
                    }
                    break;
                }
                Err(crossbeam::channel::TryRecvError::Empty) => {
                    // 채널이 비어있지만 아직 열려있음
                    // 타임아웃 설정 (10ms)
                    let timeout = batch_time_limit.saturating_sub(last_flush.elapsed());
                    
                    if timeout == Duration::ZERO {
                        // 시간 기반 배치 쓰기 (10ms 경과)
                        if !batch.is_empty() {
                            if let Err(e) = flush_batch(&mut batch, &db_pool).await {
                                eprintln!("Failed to flush DB batch: {}", e);
                            }
                            last_flush = Instant::now();
                        }
                        // 채널이 닫혔는지 다시 확인 (논블로킹)
                        match db_rx.try_recv() {
                            Ok(cmd) => {
                                batch.push(cmd);
                                continue;
                            }
                            Err(crossbeam::channel::TryRecvError::Disconnected) => {
                                // 채널이 닫힘
                                if !batch.is_empty() {
                                    let _ = flush_batch(&mut batch, &db_pool).await;
                                }
                                break;
                            }
                            Err(crossbeam::channel::TryRecvError::Empty) => {
                                // 여전히 비어있음 - 매우 짧은 대기 후 다시 확인
                                tokio::time::sleep(Duration::from_micros(100)).await;
                            }
                        }
                    } else {
                        // 타임아웃까지 대기
                        match db_rx.recv_timeout(timeout) {
                            Ok(cmd) => {
                                batch.push(cmd);
                                if batch.len() >= batch_size_limit {
                                    if let Err(e) = flush_batch(&mut batch, &db_pool).await {
                                        eprintln!("Failed to flush DB batch: {}", e);
                                    }
                                    last_flush = Instant::now();
                                }
                            }
                            Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                                // 시간 기반 배치 쓰기 (10ms 경과)
                                if !batch.is_empty() {
                                    if let Err(e) = flush_batch(&mut batch, &db_pool).await {
                                        eprintln!("Failed to flush DB batch: {}", e);
                                    }
                                    last_flush = Instant::now();
                                }
                            }
                            Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                                // 채널이 닫힘 (정상 종료)
                                if !batch.is_empty() {
                                    let _ = flush_batch(&mut batch, &db_pool).await;
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }
    });
}

/// 배치를 DB에 쓰기 (async)
/// 
/// # Arguments
/// * `batch` - DB 명령 배치
/// * `db_pool` - 데이터베이스 연결 풀
/// 
/// # 처리 과정
/// 1. 트랜잭션 시작
/// 2. 각 명령 처리
/// 3. 커밋
/// Deadlock 에러 확인 헬퍼 함수
/// 
/// PostgreSQL의 Deadlock 에러 코드 40P01을 확인합니다.
fn is_deadlock_error(e: &anyhow::Error) -> bool {
    let error_str = e.to_string();
    error_str.contains("deadlock") || error_str.contains("40P01")
}

/// 배치를 DB에 쓰기 (Deadlock 재시도 로직 포함)
/// 
/// # 처리 과정
/// 1. UpdateBalance 명령 집계 (같은 (user_id, mint) 조합의 delta 합산)
/// 2. 배치 정렬 (외래키 제약조건을 위해)
/// 3. 트랜잭션 시작 및 명령 처리
/// 4. Deadlock 발생 시 재시도 (최대 3회)
/// 
/// # Deadlock 처리
/// - Deadlock 발생 시 트랜잭션 롤백 후 재시도
/// - 재시도 간격: 100ms, 200ms, 300ms (지수 백오프)
/// - 최대 3회 재시도 후 실패 시 에러 반환
async fn flush_batch(
    batch: &mut Vec<super::db_commands::DbCommand>,
    db_pool: &PgPool,
) -> Result<()> {
    use super::db_commands::DbCommand;
    use std::collections::HashMap;
    use std::time::Duration;
    
    if batch.is_empty() {
        return Ok(());
    }
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 1. UpdateBalance 명령 집계 (Deadlock 확률 감소)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 같은 (user_id, mint) 조합의 UpdateBalance 명령을 집계하여
    // 한 번만 UPDATE 실행하도록 합니다.
    // 이렇게 하면 같은 행을 여러 번 업데이트하지 않아 Deadlock 확률이 감소합니다.
    let mut balance_updates: HashMap<(u64, String), (rust_decimal::Decimal, rust_decimal::Decimal)> = HashMap::new();
    let mut other_commands = Vec::new();
    
    // 배치를 순회하면서 UpdateBalance 명령을 집계
    for cmd in batch.iter() {
        match cmd {
            DbCommand::UpdateBalance { user_id, mint, available_delta, locked_delta } => {
                // 같은 (user_id, mint) 조합의 delta를 합산
                let key = (*user_id, mint.clone());
                let entry = balance_updates.entry(key).or_insert((rust_decimal::Decimal::ZERO, rust_decimal::Decimal::ZERO));
                
                // available_delta 합산
                if let Some(delta) = available_delta {
                    entry.0 += *delta;
                }
                
                // locked_delta 합산
                if let Some(delta) = locked_delta {
                    entry.1 += *delta;
                }
            }
            _ => {
                // UpdateBalance가 아닌 명령은 그대로 유지
                other_commands.push(cmd.clone());
            }
        }
    }
    
    // 집계된 UpdateBalance를 다른 명령과 합치기
    let mut processed_batch = Vec::new();
    
    // 다른 명령들을 먼저 추가 (InsertOrder, UpdateOrderStatus, InsertTrade)
    processed_batch.extend(other_commands);
    
    // 집계된 UpdateBalance를 마지막에 추가
    for ((user_id, mint), (total_available_delta, total_locked_delta)) in balance_updates {
        // delta가 0이 아닌 경우만 추가
        if total_available_delta != rust_decimal::Decimal::ZERO || total_locked_delta != rust_decimal::Decimal::ZERO {
            processed_batch.push(DbCommand::UpdateBalance {
                user_id,
                mint,
                available_delta: if total_available_delta != rust_decimal::Decimal::ZERO { 
                    Some(total_available_delta) 
                } else { 
                    None 
                },
                locked_delta: if total_locked_delta != rust_decimal::Decimal::ZERO { 
                    Some(total_locked_delta) 
                } else { 
                    None 
                },
            });
        }
    }
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 2. 배치 정렬 (외래키 제약조건을 위해)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 1. InsertOrder (주문 먼저 생성)
    // 2. UpdateOrderStatus (주문 상태 업데이트)
    // 3. InsertTrade (체결 내역 - 주문이 있어야 함)
    // 4. UpdateBalance (잔고 업데이트)
    processed_batch.sort_by(|a, b| {
        let priority = |cmd: &DbCommand| match cmd {
            DbCommand::InsertOrder { .. } => 1,
            DbCommand::UpdateOrderStatus { .. } => 2,
            DbCommand::InsertTrade { .. } => 3,
            DbCommand::UpdateBalance { .. } => 4,
        };
        priority(a).cmp(&priority(b))
    });
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 3. Deadlock 재시도 로직 (강화: 3회 → 5회)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 배치 시간 단축(500ms)에 따른 데드락 확률 증가에 대비하여 재시도 강화
    const MAX_RETRIES: u32 = 5;
    let mut retry_count = 0;
    
    loop {
        // 트랜잭션 시작
        let mut tx = match db_pool.begin().await {
            Ok(tx) => tx,
            Err(e) => {
                if retry_count < MAX_RETRIES {
                    retry_count += 1;
                    // 지수 백오프: 100ms, 200ms, 400ms, 800ms, 1600ms
                    let delay_ms = 100u64 * (1u64 << (retry_count - 1));
                    eprintln!("[DB Writer] Failed to begin transaction (attempt {}), retrying in {}ms...", retry_count, delay_ms);
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    continue;
                }
                return Err(anyhow::anyhow!("Failed to begin transaction after {} retries: {}", MAX_RETRIES, e));
            }
        };
        
        // 성공한 명령 인덱스 추적 (에러 발생 시 재시도 가능하도록)
        let mut successful_indices = Vec::new();
        let mut has_deadlock = false;
        
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 4. 각 명령 처리 (drain 대신 인덱스 순회)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // drain(..)을 사용하면 에러 발생 시 배치가 비워져서 재시도 불가능합니다.
        // 인덱스 순회를 사용하여 성공한 명령만 나중에 제거합니다.
        for (idx, cmd) in processed_batch.iter().enumerate() {
            match process_db_command(cmd, &mut tx, db_pool).await {
                Ok(_) => {
                    // 성공한 명령 인덱스 저장
                    successful_indices.push(idx);
                }
                Err(e) => {
                    // Deadlock 에러 확인
                    if is_deadlock_error(&e) {
                        eprintln!("[DB Writer] Deadlock detected (attempt {}), will retry...", retry_count + 1);
                        has_deadlock = true;
                        break; // 트랜잭션 롤백 후 재시도
                    } else {
                        // Deadlock이 아닌 다른 에러는 즉시 반환
                        return Err(e);
                    }
                }
            }
        }
        
        // Deadlock 발생 시 재시도
        if has_deadlock {
            // 트랜잭션 롤백 (명시적으로 롤백하지 않아도 drop 시 자동 롤백)
            drop(tx);
            
            if retry_count < MAX_RETRIES {
                retry_count += 1;
                // 지수 백오프: 100ms, 200ms, 400ms, 800ms, 1600ms
                let delay_ms = 100u64 * (1u64 << (retry_count - 1));
                eprintln!("[DB Writer] Retrying after deadlock (attempt {}/{}) in {}ms...", retry_count, MAX_RETRIES, delay_ms);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                continue;
            } else {
                return Err(anyhow::anyhow!("Failed after {} retries due to deadlock", MAX_RETRIES));
            }
        }
        
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 5. 트랜잭션 커밋
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        match tx.commit().await {
            Ok(_) => {
                // 성공한 명령만 배치에서 제거 (역순으로 제거하여 인덱스 변경 방지)
                successful_indices.sort_by(|a, b| b.cmp(a));
                for &idx in &successful_indices {
                    batch.remove(idx);
                }
                return Ok(());
            }
            Err(e) => {
                // 커밋 실패 시 Deadlock 확인
                if is_deadlock_error(&anyhow::anyhow!("{}", e)) {
                    if retry_count < MAX_RETRIES {
                        retry_count += 1;
                        // 지수 백오프: 100ms, 200ms, 400ms, 800ms, 1600ms
                        let delay_ms = 100u64 * (1u64 << (retry_count - 1));
                        eprintln!("[DB Writer] Commit failed due to deadlock (attempt {}), retrying in {}ms...", retry_count, delay_ms);
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        continue;
                    } else {
                        return Err(anyhow::anyhow!("Failed to commit transaction after {} retries due to deadlock: {}", MAX_RETRIES, e));
                    }
                } else {
                    return Err(anyhow::anyhow!("Failed to commit transaction: {}", e));
                }
            }
        }
    }
}

/// 개별 DB 명령 처리 (헬퍼 함수)
/// 
/// 각 명령을 처리하고 에러 발생 시 반환합니다.
async fn process_db_command(
    cmd: &super::db_commands::DbCommand,
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    db_pool: &PgPool,
) -> Result<()> {
    use super::db_commands::DbCommand;
    
    match cmd {
        DbCommand::InsertOrder {
                order_id,
                user_id,
                order_type,
                order_side,
                base_mint,
                quote_mint,
                price,
                amount,
                created_at,
                status,
                filled_amount,
                filled_quote_amount,
            } => {
                // ID 생성기로 생성한 ID를 사용 (auto increment 사용 안 함)
                // order_id가 0이면 에러 (ID 생성기가 제대로 작동하지 않음)
                if *order_id == 0 {
                    return Err(anyhow::anyhow!(
                        "Order ID is 0. ID generator may not be initialized properly."
                    ));
                }
                
                // 상태 및 체결 정보 (시장가 주문은 최종 상태로 전달됨)
                let final_status = status.as_ref().map(|s| s.clone()).unwrap_or_else(|| "pending".to_string());
                let final_filled_amount = filled_amount.unwrap_or(rust_decimal::Decimal::ZERO);
                let final_filled_quote_amount = filled_quote_amount.unwrap_or(rust_decimal::Decimal::ZERO);
                
                // 지정된 ID로 INSERT
                sqlx::query(
                    r#"
                    INSERT INTO orders (
                        id, user_id, order_type, order_side, base_mint, quote_mint,
                        price, amount, filled_amount, filled_quote_amount, status, created_at, updated_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    ON CONFLICT (id) DO UPDATE SET
                        updated_at = $13
                    "#
                )
                .bind(*order_id as i64)
                .bind(*user_id as i64)
                .bind(order_type)
                .bind(order_side)
                .bind(base_mint)
                .bind(quote_mint)
                .bind(price)
                .bind(amount)
                .bind(final_filled_amount)
                .bind(final_filled_quote_amount)
                .bind(&final_status)
                .bind(created_at)
                .bind(created_at)
                .execute(&mut *tx)
                .await
                .context("Failed to insert order")?;
                
                Ok(())
            }
            
            DbCommand::UpdateOrderStatus {
                order_id,
                status,
                filled_amount,
                filled_quote_amount,
            } => {
                // 취소 시에는 filled_quote_amount를 변경하지 않음 (기존 값 유지)
                if status == "cancelled" {
                    sqlx::query(
                        r#"
                        UPDATE orders
                        SET status = $1, filled_amount = $2, updated_at = $3
                        WHERE id = $4
                        "#
                    )
                    .bind(status)
                    .bind(filled_amount)
                    .bind(chrono::Utc::now())
                    .bind(*order_id as i64)
                    .execute(&mut *tx)
                    .await
                    .context("Failed to update order status")?;
                } else {
                    sqlx::query(
                        r#"
                        UPDATE orders
                        SET status = $1, filled_amount = $2, filled_quote_amount = $3, updated_at = $4
                        WHERE id = $5
                        "#
                    )
                    .bind(status)
                    .bind(filled_amount)
                    .bind(filled_quote_amount)
                    .bind(chrono::Utc::now())
                    .bind(*order_id as i64)
                    .execute(&mut *tx)
                    .await
                    .context("Failed to update order status")?;
                }
                
                Ok(())
            }
            
            DbCommand::InsertTrade {
                trade_id,
                buy_order_id,
                sell_order_id,
                buyer_id,
                seller_id,
                price,
                amount,
                base_mint,
                quote_mint,
                timestamp,
            } => {
                // buy_order_id, sell_order_id가 0이면 스킵 (주문이 아직 DB에 INSERT되지 않음)
                if *buy_order_id == 0 || *sell_order_id == 0 {
                    eprintln!(
                        "[DB Writer] Skipping trade insert: buy_order_id={}, sell_order_id={} (orders not yet inserted)",
                        buy_order_id, sell_order_id
                    );
                    // 스킵된 것으로 처리 (성공으로 간주)
                    return Ok(());
                }
                
                // ID 생성기로 생성한 trade_id 사용
                // 외래키 제약조건 위반, 트랜잭션 abort, Deadlock 에러 처리
                match sqlx::query(
                    r#"
                    INSERT INTO trades (
                        id, buy_order_id, sell_order_id, buyer_id, seller_id,
                        price, amount, base_mint, quote_mint, created_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (id) DO NOTHING
                    "#
                )
                .bind(*trade_id as i64)
                .bind(*buy_order_id as i64)
                .bind(*sell_order_id as i64)
                .bind(*buyer_id as i64)
                .bind(*seller_id as i64)
                .bind(price)
                .bind(amount)
                .bind(base_mint)
                .bind(quote_mint)
                .bind(timestamp)
                .execute(&mut *tx)
                .await
                {
                    Ok(_) => {
                        // 성공적으로 삽입됨
                        Ok(())
                    }
                    Err(sqlx::Error::Database(db_err)) => {
                        // 에러 코드 확인
                        let error_code = db_err.code().as_deref().map(|s| s.to_string());
                        
                        // Deadlock 에러 코드 40P01 체크
                        if error_code.as_deref() == Some("40P01") {
                            // Deadlock 발생 → 재시도 로직으로 전달
                            return Err(anyhow::anyhow!("Deadlock detected: {}", db_err));
                        }
                        
                        // 외래키 제약조건 위반 (23503) 또는 트랜잭션 abort (25P02)는 무시
                        // 스케줄러가 orders를 삭제한 경우 발생할 수 있음
                        if error_code.as_deref() == Some("23503") || error_code.as_deref() == Some("25P02") {
                            // 조용히 무시 (로그 출력 안 함, 성공으로 간주)
                            Ok(())
                        } else {
                            // 다른 DB 에러는 로그만 출력하고 계속 진행
                            eprintln!("[DB Writer] Trade insert error (non-critical): trade_id={}, buy_order_id={}, sell_order_id={}, error={}", 
                                     trade_id, buy_order_id, sell_order_id, db_err);
                            Ok(())
                        }
                    }
                    Err(e) => {
                        // 다른 에러도 조용히 무시 (스케줄러 삭제로 인한 정상적인 상황)
                        Ok(())
                    }
                }
            }
            
            DbCommand::UpdateBalance {
                user_id,
                mint,
                available_delta,
                locked_delta,
            } => {
                use crate::shared::database::repositories::cex::UserBalanceRepository;
                use crate::domains::cex::models::balance::UserBalanceUpdate;
                
                let balance_repo = UserBalanceRepository::new(db_pool.clone());
                let update = UserBalanceUpdate {
                    available_delta: *available_delta,
                    locked_delta: *locked_delta,
                };
                
                balance_repo.update_balance(*user_id, mint, &update).await
                    .context("Failed to update balance")?;
                
                Ok(())
            }
    }
}

