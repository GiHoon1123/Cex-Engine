//! 실행 캐시의 소유자. 주문 단위 확정은 runtime::submission이 담당한다.
//! DB·Kafka·WAL 채널을 소유하거나 개별 체결을 외부에 전달하지 않는다.
use super::balance_cache::BalanceCache;
use anyhow::{Context, Result};
use rust_decimal::Decimal;

pub struct Executor {
    balance_cache: BalanceCache,
}

impl Executor {
    pub fn new() -> Self {
        Self {
            balance_cache: BalanceCache::new(),
        }
    }

    /// 단독 예약 경로도 원본·두 결과를 검사한 뒤 변경한다. 신규 주문의 예약은
    /// submission의 작업 공간에서 실행되므로 전체 후보 실패 때 원본에 남지 않는다.
    pub fn lock_balance_for_order(
        &mut self,
        order_id: u64,
        user_id: u64,
        mint: &str,
        amount: Decimal,
    ) -> Result<()> {
        self.balance_cache
            .lock_balance(user_id, mint, amount)
            .with_context(|| format!("Failed to lock balance for order {order_id}"))
    }

    /// 취소 명령은 기존 경로에서 호출한다. 이 함수의 성공 자체가 DB 취소 확정이나
    /// 이벤트 전달 보장은 아니다. 취소·영속 결과 경계는 별도로 완성해야 한다.
    pub fn unlock_balance_for_cancel(
        &mut self,
        order_id: u64,
        user_id: u64,
        mint: &str,
        amount: Decimal,
    ) -> Result<()> {
        self.balance_cache
            .unlock_balance(user_id, mint, amount)
            .with_context(|| format!("Failed to unlock balance for order {order_id}"))
    }

    pub fn balance_cache(&self) -> &BalanceCache {
        &self.balance_cache
    }
    pub fn balance_cache_mut(&mut self) -> &mut BalanceCache {
        &mut self.balance_cache
    }
}
