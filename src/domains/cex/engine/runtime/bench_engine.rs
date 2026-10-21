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
    /// 실제 워커와 순수 계산의 잔액 결과를 비교한다. 테스트에만 포함하며
    /// 실행 중인 워커의 캐시를 외부에 공유하는 API를 추가하지 않는다.
    #[cfg(test)]
    pub(crate) fn test_balance(&self, user: u64, asset: &str) -> (Decimal, Decimal) {
        self.executor.balance_cache().get_balance(user, asset)
            .map(|balance| (balance.available, balance.locked))
            .unwrap_or((Decimal::ZERO, Decimal::ZERO))
    }

    /// 새 벤치마크 엔진 생성
    pub fn new() -> Self {
        Self {
            orderbooks: HashMap::new(),
            matcher: Matcher::new(),
            executor: Executor::new(),
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
    order:OrderEntry,orderbooks:&mut HashMap<TradingPair,OrderBook>,matcher:&Matcher,executor:&mut Executor,
)->Result<Vec<MatchResult>> {
    // 제품 워커와 동일한 준비·확정 경계다. 채널·외부 발행만 생략한다.
    Ok(super::submission::apply(order,orderbooks,matcher,executor)?.matches)
}
