// =====================================================
// DbCommand - DB Writer 스레드로 전달할 명령
// =====================================================
// 역할: 엔진 스레드에서 DB Writer 스레드로 DB 쓰기 명령 전달
//
// 사용 목적:
// - 주문 생성/업데이트
// - 체결 내역 저장
// - 잔고 업데이트
//
// 특징:
// - Lock-free 채널로 전송 (~100ns)
// - Non-blocking (메인 엔진 성능 영향 없음)
// =====================================================

use rust_decimal::Decimal;
use chrono::{DateTime, Utc};

/// DB Writer 스레드로 전달할 명령
/// 
/// 각 명령은 DB Writer 스레드에서 배치로 처리됩니다.
#[derive(Debug, Clone)]
pub enum DbCommand {
    /// 주문 생성
    /// 
    /// # Fields
    /// * `order_id` - 주문 ID
    /// * `user_id` - 사용자 ID
    /// * `order_type` - 주문 타입 ("buy" or "sell")
    /// * `order_side` - 주문 방식 ("limit" or "market")
    /// * `base_mint` - 기준 자산
    /// * `quote_mint` - 기준 통화
    /// * `price` - 주문 가격 (지정가만)
    /// * `amount` - 주문 수량
    /// * `created_at` - 생성 시간
    /// * `status` - 주문 상태 (None이면 "pending", 시장가 주문은 최종 상태로 전달)
    /// * `filled_amount` - 체결된 수량 (None이면 0)
    /// * `filled_quote_amount` - 체결된 금액 (None이면 0)
    InsertOrder {
        order_id: u64,
        user_id: u64,
        order_type: String,
        order_side: String,
        base_mint: String,
        quote_mint: String,
        price: Option<Decimal>,
        amount: Decimal,
        created_at: DateTime<Utc>,
        status: Option<String>,
        filled_amount: Option<Decimal>,
        filled_quote_amount: Option<Decimal>,
    },
    
    /// 주문 상태 업데이트
    /// 
    /// # Fields
    /// * `order_id` - 주문 ID
    /// * `status` - 새 상태 ("pending", "partial", "filled", "cancelled")
    /// * `filled_amount` - 체결된 수량
    /// * `filled_quote_amount` - 체결된 금액 (USDT 기준)
    UpdateOrderStatus {
        order_id: u64,
        status: String,
        filled_amount: Decimal,
        filled_quote_amount: Decimal,
    },
    
    /// 체결 내역 저장
    /// 
    /// # Fields
    /// * `buy_order_id` - 매수 주문 ID
    /// * `sell_order_id` - 매도 주문 ID
    /// * `buyer_id` - 매수자 ID
    /// * `seller_id` - 매도자 ID
    /// * `price` - 체결 가격
    /// * `amount` - 체결 수량
    /// * `base_mint` - 기준 자산
    /// * `quote_mint` - 기준 통화
    /// * `timestamp` - 체결 시간
    /// 
    /// Note: trade_id는 Java DB에서 auto increment로 생성됨 (Kafka 이벤트 경로).
    /// Rust DB Writer 경로는 비활성화되어 있음.
    InsertTrade {
        buy_order_id: u64,
        sell_order_id: u64,
        buyer_id: u64,
        seller_id: u64,
        price: Decimal,
        amount: Decimal,
        base_mint: String,
        quote_mint: String,
        timestamp: DateTime<Utc>,
    },
    
    /// 잔고 업데이트
    /// 
    /// # Fields
    /// * `user_id` - 사용자 ID
    /// * `mint` - 자산 종류
    /// * `available_delta` - available 증감량 (None이면 변경 없음)
    /// * `locked_delta` - locked 증감량 (None이면 변경 없음)
    UpdateBalance {
        user_id: u64,
        mint: String,
        available_delta: Option<Decimal>,
        locked_delta: Option<Decimal>,
    },
}

