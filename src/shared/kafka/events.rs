// =====================================================
// Kafka 이벤트 구조체
// =====================================================
// 역할: Kafka로 발행할 이벤트의 구조 정의
// 
// 이벤트 타입:
// 1. TradeExecutedEvent - 체결 이벤트
// 2. OrderCancelledEvent - 취소 이벤트
// 
// 중요: 단일 토픽으로 통합하여 순서 보장
// - 토픽: order-events (파티션 12개)
// - 파티션 키: user_id (같은 유저의 이벤트는 같은 파티션으로)
// - 정산용: trade_executed, order_cancelled만 발행
// =====================================================

use serde::Serialize;
use rust_decimal::Decimal;
use chrono::{DateTime, Utc};

/// 체결 이벤트
/// Trade Executed Event
/// 
/// Kafka로 발행되는 체결 이벤트
/// Java Consumer가 이를 받아서 DB에 저장합니다.
/// 
/// Note: trade_id는 Java DB에서 auto increment로 생성되므로 Rust에서 보내지 않음
#[derive(Debug, Clone, Serialize)]
pub struct TradeExecutedEvent {
    /// 이벤트 타입 ("trade_executed")
    #[serde(rename = "event_type")]
    pub event_type: String,

    /// 매수 주문 ID
    pub buy_order_id: u64,

    /// 매도 주문 ID
    pub sell_order_id: u64,

    /// 매수자 ID
    pub buyer_id: u64,

    /// 매도자 ID
    pub seller_id: u64,

    /// 체결 가격
    pub price: Decimal,

    /// 체결 수량
    pub amount: Decimal,

    /// 기준 자산 (예: "SOL")
    pub base_mint: String,

    /// 기준 통화 (예: "USDT")
    pub quote_mint: String,

    /// 체결 시간
    pub timestamp: DateTime<Utc>,
}

/// 주문 취소 이벤트
/// Order Cancelled Event
/// 
/// Kafka로 발행되는 주문 취소 이벤트
/// Java Consumer가 이를 받아서 DB에 주문 상태를 업데이트합니다.
#[derive(Debug, Clone, Serialize)]
pub struct OrderCancelledEvent {
    /// 이벤트 타입 ("order_cancelled")
    #[serde(rename = "event_type")]
    pub event_type: String,

    /// 취소된 주문 ID
    pub order_id: u64,

    /// 주문한 사용자 ID
    pub user_id: u64,

    /// 기준 자산 (예: "SOL")
    pub base_mint: String,

    /// 기준 통화 (예: "USDT")
    pub quote_mint: String,

    /// 취소 시간
    pub timestamp: DateTime<Utc>,
}
