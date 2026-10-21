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
// - 토픽: order-events (파티션 1개)
// - 파티션 키: 엔진 식별자 (한 코어의 결과는 같은 파티션으로)
// - 정산용: trade_executed, order_cancelled, market_order_completed, order_rejected 발행
// =====================================================

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;

/// 체결 이벤트
/// Trade Executed Event
///
/// Kafka로 발행되는 체결 이벤트
/// Java Consumer가 이를 받아서 DB에 저장합니다.
///
/// Note: trade_id는 Java DB에서 auto increment로 생성되므로 Rust에서 보내지 않음
#[derive(Debug, Clone, Serialize)]
pub struct TradeExecutedEvent {
    /// 재시작·재전송에도 변하지 않는 저널 기반 이벤트 식별자다.
    pub event_id: String,
    /// 단일 코어의 모든 결과를 같은 파티션으로 보내는 엔진 식별자다.
    pub engine_id: String,
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

    /// 매수자는 기준 자산, 매도자는 결제 자산에서 납부한다. 적용률과 실제 차감액을 함께 기록한다.
    pub buyer_fee_rate: Decimal,
    pub seller_fee_rate: Decimal,
    pub buyer_fee: Decimal,
    pub seller_fee: Decimal,
}

/// 주문 취소 이벤트
/// Order Cancelled Event
///
/// Kafka로 발행되는 주문 취소 이벤트
/// Java Consumer가 이를 받아서 DB에 주문 상태를 업데이트합니다.
#[derive(Debug, Clone, Serialize)]
pub struct OrderCancelledEvent {
    /// 재시작·재전송에도 변하지 않는 저널 기반 이벤트 식별자다.
    pub event_id: String,
    /// 단일 코어의 모든 결과를 같은 파티션으로 보내는 엔진 식별자다.
    pub engine_id: String,
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

/// 체결 목록 다음에 보내는 시장가 종료다. 영속 결과를 빌려 직렬화하며 금융 결과를 다시 계산하지 않는다.
#[derive(Debug, Serialize)]
pub(crate) struct MarketOrderCompletedEvent<'a> {
    pub event_id: String,
    pub engine_id: String,
    pub event_type: &'static str,
    pub timestamp: DateTime<Utc>,
    #[serde(flatten)]
    pub result: &'a crate::domains::cex::engine::runtime::MarketCompletion,
}

/// 전부 거절된 원본 명령을 식별한다. 플랫폼은 원본을 대조한 뒤 예약을 반환한다.
/// 부분 체결 후 만료와 다르며 내부 오류 문자열은 공개하지 않는다.
#[derive(Debug, Serialize)]
pub(crate) struct OrderRejectedEvent<'a> {
    pub event_id: String,
    pub engine_id: String,
    pub event_type: &'static str,
    pub order_id: u64,
    pub user_id: u64,
    pub order_type: &'a str,
    pub order_side: &'a str,
    pub base_mint: &'a str,
    pub quote_mint: &'a str,
    pub price: Option<Decimal>,
    pub amount: Option<Decimal>,
    pub quote_amount: Option<Decimal>,
    pub fee_rate: Decimal,
    pub reason_code: &'static str,
    pub timestamp: DateTime<Utc>,
}
