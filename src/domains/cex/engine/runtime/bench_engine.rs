// =====================================================
// BenchEngine - 벤치마크 전용 엔진
// =====================================================
// 역할: 벤치마크 성능 측정 전용 엔진
// 
// 핵심 설계:
// 1. 락 없음 - 싱글스레드로 동작하므로 락 불필요
// 2. 엔진 스레드 없음 - 직접 호출
// 3. 채널 없음 - 직접 접근
// 4. 프로덕션 코드의 핵심 로직 재사용
//
// 성능:
// - 프로덕션 코드 경로와 동일한 로직 사용
// - 락 오버헤드 제거로 더 빠른 측정 가능
// =====================================================

use std::collections::HashMap;
use anyhow::Result;
use rust_decimal::Decimal;

use crate::domains::cex::engine::types::{TradingPair, OrderEntry, MatchResult};
use crate::domains::cex::engine::orderbook::OrderBook;
use crate::domains::cex::engine::matcher::Matcher;
use crate::domains::cex::engine::executor::Executor;

/// 벤치마크 전용 엔진
/// 
/// 프로덕션 코드의 핵심 로직을 재사용하되, 락 없이 직접 접근
pub struct BenchEngine {
    /// 거래쌍별 오더북 (락 없이 직접 소유)
    orderbooks: HashMap<TradingPair, OrderBook>,
    
    /// 매칭 엔진
    matcher: Matcher,
    
    /// 체결 실행 엔진 (락 없이 직접 소유)
    executor: Executor,
}

impl BenchEngine {
    /// 새 벤치마크 엔진 생성
    pub fn new() -> Self {
        Self {
            orderbooks: HashMap::new(),
            matcher: Matcher::new(),
            executor: Executor::new_without_wal(),
        }
    }
    
    /// 직접 주문 처리 (락 없이 직접 접근)
    /// 
    /// process_submit_order_bench를 사용하여 락 오버헤드 완전 제거
    pub fn submit_direct(&mut self, order: OrderEntry) -> Result<Vec<MatchResult>> {
        process_submit_order_bench(
            order,
            &mut self.orderbooks,
            &self.matcher,
            &mut self.executor,
        )
    }
    
    /// 잔고 설정
    pub fn set_balance(&mut self, user_id: u64, mint: &str, available: Decimal, locked: Decimal) {
        self.executor.balance_cache_mut().set_balance(user_id, mint, available, locked);
    }
    
    /// 잔고 초기화
    pub fn clear_balances(&mut self) {
        self.executor.balance_cache_mut().clear();
    }
    
    /// 오더북 초기화
    pub fn clear_orderbooks(&mut self) {
        self.orderbooks.clear();
    }
}

/// 벤치마크용 주문 처리 (락 없이 직접 접근)
/// 
/// 프로덕션 코드의 process_submit_order 로직을 재사용하되 락 없이 직접 접근
fn process_submit_order_bench(
    mut order: OrderEntry,
    orderbooks: &mut HashMap<TradingPair, OrderBook>,
    matcher: &Matcher,
    executor: &mut Executor,
) -> Result<Vec<MatchResult>> {
    // 프로덕션 코드의 핵심 로직을 락 없이 재사용
    // process_submit_order의 핵심 부분을 복사하되 락 제거
    
    // 1. TradingPair 찾기
    let pair = TradingPair::new(order.base_mint.clone(), order.quote_mint.clone());
    
    // 2. 잔고 잠금 (락 없이 직접 접근)
    let (lock_mint, lock_amount) = if order.order_type == "buy" {
        let amount = if order.order_side == "market" {
            order.quote_amount.unwrap_or(Decimal::ZERO)
        } else {
            order.price.unwrap_or(Decimal::ZERO) * order.amount
        };
        (&order.quote_mint, amount)
    } else {
        (&order.base_mint, order.amount)
    };
    
    if let Err(e) = executor.lock_balance_for_order(order.id, order.user_id, lock_mint, lock_amount) {
        return Err(anyhow::anyhow!("Failed to lock balance: {}", e));
    }
    
    // 3. 시장가 주문 여부 및 초기 잔고 잠금 정보 저장
    let is_market_order = order.order_side == "market";
    let initial_quote_amount = order.quote_amount;
    let initial_amount = order.amount;
    
    // 4. OrderBook 가져오기 및 매칭 (락 없이 직접 접근)
    let orderbook = orderbooks.entry(pair.clone()).or_insert_with(|| OrderBook::new(pair.clone()));
    
    // 5. Matcher로 매칭 시도
    let matches = matcher.match_order(&mut order, orderbook);
    
    // 6. 매칭 후 남은 주문이 있으면 OrderBook에 추가
    if order.order_side == "limit" {
        let has_remaining = if let Some(remaining_quote) = order.remaining_quote_amount {
            remaining_quote > Decimal::ZERO
        } else {
            order.remaining_amount > Decimal::ZERO
        };
        
        if has_remaining {
            orderbook.add_order(order.clone());
        }
    }
    
    let order_after_match = order.clone();
    
    // 7. 시장가 주문 처리
    if is_market_order {
        let mut successful_matches = Vec::new();
        let mut total_quote_used = Decimal::ZERO;
        let mut total_amount_used = Decimal::ZERO;
        
        for match_result in &matches {
            match executor.execute_trade(match_result, true) {
                Ok(_) => {
                    successful_matches.push(match_result.clone());
                    if order_after_match.order_type == "buy" {
                        total_quote_used += match_result.price * match_result.amount;
                    } else {
                        total_amount_used += match_result.amount;
                    }
                }
                Err(_) => {
                    // 실패한 매칭은 무시
                }
            }
        }
        
        let matches = successful_matches;
        
        // 남은 잔고 잠금 해제
        if order_after_match.order_type == "buy" {
            let initial_quote = initial_quote_amount.unwrap_or(Decimal::ZERO);
            let unlock = initial_quote - total_quote_used;
            if unlock > Decimal::ZERO {
                let _ = executor.unlock_balance_for_cancel(
                    order_after_match.id,
                    order_after_match.user_id,
                    &order_after_match.quote_mint,
                    unlock,
                );
            }
        } else {
            let unlock = initial_amount - total_amount_used;
            if unlock > Decimal::ZERO {
                let _ = executor.unlock_balance_for_cancel(
                    order_after_match.id,
                    order_after_match.user_id,
                    &order_after_match.base_mint,
                    unlock,
                );
            }
        }
        
        return Ok(matches);
    }
    
    // 8. 체결 처리 (지정가 주문)
    for match_result in &matches {
        let _ = executor.execute_trade(match_result, false);
    }
    
    // 9. 완전히 체결된 지정가 주문의 남은 locked 잔고 해제
    if order_after_match.remaining_amount == Decimal::ZERO {
        let total_quote_used: Decimal = matches.iter()
            .map(|m| m.price * m.amount)
            .sum();
        let total_amount_used: Decimal = matches.iter()
            .map(|m| m.amount)
            .sum();
        
        let (unlock_mint, unlock_amount) = if order_after_match.order_type == "buy" {
            let initial_locked = order_after_match.price.unwrap_or(Decimal::ZERO) * initial_amount;
            let remaining_locked = initial_locked - total_quote_used;
            (&order_after_match.quote_mint, remaining_locked)
        } else {
            let remaining_locked = initial_amount - total_amount_used;
            (&order_after_match.base_mint, remaining_locked)
        };
        
        if unlock_amount > Decimal::ZERO {
            let _ = executor.unlock_balance_for_cancel(
                order_after_match.id,
                order_after_match.user_id,
                unlock_mint,
                unlock_amount,
            );
        }
    }
    
    Ok(matches)
}
