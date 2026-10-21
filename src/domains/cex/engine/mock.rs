use super::{Engine, TradingPair, OrderEntry, MatchResult};
use anyhow::{Result, bail};
use async_trait::async_trait;
use rust_decimal::Decimal;

/// Mock Engine (테스트용 임시 구현)
/// Mock Engine (temporary implementation for testing)
/// 
/// 실제 체결 엔진이 구현되기 전까지 사용하는 임시 구현입니다.
/// 모든 메서드가 작동하지만, 실제로는 아무 일도 하지 않거나 더미 데이터를 반환합니다.
/// 
/// TODO: 나중에 실제 HighPerfEngine으로 교체 필요
pub struct MockEngine;

impl MockEngine {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Engine for MockEngine {
    async fn submit_order(&self, _order: OrderEntry) -> Result<()> {
        // TODO: 실제 매칭 로직 구현 필요
        // 일단 성공으로 반환 (체결 없음)
        Ok(())
    }

    async fn cancel_order(
        &self,
        order_id: u64,
        _user_id: u64,
        _trading_pair: &TradingPair,
    ) -> Result<OrderEntry> {
        // TODO: 실제 취소 로직 구현 필요
        bail!("MockEngine: cancel_order not implemented (order_id: {})", order_id)
    }

    async fn get_orderbook(
        &self,
        _trading_pair: &TradingPair,
        _depth: Option<usize>,
    ) -> Result<(Vec<OrderEntry>, Vec<OrderEntry>)> {
        // TODO: 실제 오더북 조회 구현 필요
        // 일단 빈 오더북 반환
        Ok((Vec::new(), Vec::new()))
    }

    async fn get_best_bid(&self, _trading_pair: &TradingPair) -> Result<Option<Decimal>> {
        // TODO: 실제 로직 구현 필요
        Ok(None)
    }

    async fn get_best_ask(&self, _trading_pair: &TradingPair) -> Result<Option<Decimal>> {
        // TODO: 실제 로직 구현 필요
        Ok(None)
    }

    async fn lock_balance(
        &self,
        _user_id: u64,
        _mint: &str,
        _amount: Decimal,
    ) -> Result<()> {
        // TODO: 실제 잔고 잠금 구현 필요
        // 일단 성공으로 처리
        Ok(())
    }

    async fn unlock_balance(
        &self,
        _user_id: u64,
        _mint: &str,
        _amount: Decimal,
    ) -> Result<()> {
        // TODO: 실제 잔고 해제 구현 필요
        Ok(())
    }

    async fn get_balance(&self, _user_id: u64, _mint: &str) -> Result<(Decimal, Decimal)> {
        // TODO: 실제 잔고 조회 구현 필요
        // 일단 무제한 잔고로 반환 (테스트용)
        Ok((Decimal::MAX, Decimal::ZERO))
    }

    async fn update_balance(
        &self,
        _user_id: u64,
        _mint: &str,
        _available_delta: Decimal,
    ) -> Result<()> {
        // TODO: 실제 잔고 업데이트 구현 필요
        // 일단 성공으로 처리 (테스트용)
        Ok(())
    }


}

