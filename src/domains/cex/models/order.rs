use serde::{Deserialize, Serialize, Deserializer, Serializer};
use utoipa::ToSchema;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

// =====================================================
// ID 직렬화 헬퍼 함수 (JavaScript 정밀도 손실 방지)
// =====================================================
/// u64를 문자열로 직렬화 (JavaScript 정밀도 손실 방지)
/// Serialize u64 as string to avoid precision loss in JavaScript
fn serialize_u64_as_string<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.to_string())
}

/// 문자열을 u64로 역직렬화
/// Deserialize string to u64
fn deserialize_string_to_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    s.parse::<u64>().map_err(serde::de::Error::custom)
}

// =====================================================
// Order 모델
// =====================================================
// 역할: 주문 정보를 나타내는 데이터 모델
// 설명: CEX 거래소에서 사용자가 생성한 매수/매도 주문을 표현
// 
// 주문 타입:
// - order_type: 'buy' (매수) 또는 'sell' (매도)
// - order_side: 'limit' (지정가) 또는 'market' (시장가)
// 
// 주문 상태:
// - pending: 대기 중 (체결 안 됨)
// - partial: 부분 체결 (일부만 체결됨)
// - filled: 전량 체결 완료
// - cancelled: 주문 취소됨
// =====================================================

/// 주문 정보 (데이터베이스에서 조회한 주문)
/// Order information (order retrieved from database)
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
#[schema(as = Order)]
pub struct Order {
    /// Order ID (BIGSERIAL, auto-generated)
    /// 주문 ID (DB에서 자동 생성)
    /// Serialized as string to avoid precision loss in JavaScript
    /// JavaScript 정밀도 손실 방지를 위해 문자열로 직렬화
    #[serde(serialize_with = "serialize_u64_as_string", deserialize_with = "deserialize_string_to_u64")]
    #[schema(value_type = String, example = "1850278129743992082")]
    pub id: u64,

    /// User ID (who created this order)
    /// 사용자 ID (주문 생성자)
    pub user_id: u64,

    /// Order type: 'buy' (purchase) or 'sell' (sale)
    /// 주문 유형: 'buy' (매수) 또는 'sell' (매도)
    #[schema(example = "buy")]
    pub order_type: String,

    /// Order side: 'limit' (limit order) or 'market' (market order)
    /// 주문 방식: 'limit' (지정가) 또는 'market' (시장가)
    #[schema(example = "limit")]
    pub order_side: String,

    /// Base asset (the asset being traded, e.g., 'SOL', 'USDC')
    /// 기준 자산 (거래되는 자산, 예: 'SOL', 'USDC')
    #[schema(example = "SOL")]
    pub base_mint: String,

    /// Quote currency (always 'USDT' as base currency)
    /// 기준 통화 (항상 'USDT'가 기준 통화)
    #[schema(example = "USDT")]
    pub quote_mint: String,

    /// Price for limit orders (NULL for market orders)
    /// 지정가 가격 (시장가 주문은 NULL)
    /// Unit: USDT per base asset (e.g., 1 SOL = 100 USDT)
    /// 단위: 기준 자산당 USDT (예: 1 SOL = 100 USDT)
    #[schema(value_type = Option<String>, example = "100.0")]
    pub price: Option<Decimal>,

    /// Order amount (in base asset)
    /// 주문 수량 (기준 자산 기준)
    /// Supports up to 9 decimal places
    /// 소수점 9자리까지 지원
    #[schema(value_type = String, example = "1.0")]
    pub amount: Decimal,

    /// Filled amount (how much has been executed)
    /// 체결된 수량 (얼마나 체결되었는지)
    /// If filled_amount == amount, then order is fully filled
    /// filled_amount == amount면 주문이 전량 체결된 것
    #[schema(value_type = String, example = "0.0")]
    pub filled_amount: Decimal,

    /// Filled quote amount (total USDT paid/received for executed trades)
    /// 체결된 금액 (USDT 기준, 체결된 거래의 총 결제 금액)
    /// For market orders, this is the sum of (price * amount) for all trades
    /// 시장가 주문의 경우, 모든 체결의 (가격 * 수량) 합계
    #[schema(value_type = String, example = "0.0")]
    pub filled_quote_amount: Decimal,

    /// Order status: 'pending', 'partial', 'filled', or 'cancelled'
    /// 주문 상태: 'pending' (대기), 'partial' (부분체결), 'filled' (완료), 'cancelled' (취소)
    #[schema(example = "pending")]
    pub status: String,

    /// Created timestamp
    /// 주문 생성 시간
    pub created_at: DateTime<Utc>,

    /// Updated timestamp
    /// 주문 정보 마지막 업데이트 시간
    pub updated_at: DateTime<Utc>,
}

// =====================================================
// 주문 생성 요청 (Create Order Request)
// =====================================================
/// 주문 생성 요청 모델
/// Request model for creating a new order
#[derive(Debug, Deserialize, ToSchema)]
#[schema(as = CreateOrderRequest)]
pub struct CreateOrderRequest {
    /// User ID (from Java API, no authentication required)
    /// 사용자 ID (Java API에서 전달, 인증 불필요)
    #[schema(example = "1")]
    pub user_id: u64,

    /// Order type: 'buy' or 'sell'
    /// 주문 유형: 'buy' (매수) 또는 'sell' (매도)
    #[schema(example = "buy")]
    pub order_type: String,

    /// Order side: 'limit' or 'market'
    /// 주문 방식: 'limit' (지정가) 또는 'market' (시장가)
    #[schema(example = "limit")]
    pub order_side: String,

    /// Base asset (e.g., 'SOL', 'USDC')
    /// 기준 자산 (예: 'SOL', 'USDC')
    #[schema(example = "SOL")]
    pub base_mint: String,

    /// Quote currency (always 'USDT')
    /// 기준 통화 (항상 'USDT')
    #[schema(example = "USDT", default = "USDT")]
    pub quote_mint: Option<String>,

    /// Price for limit orders (required for limit orders, not needed for market orders)
    /// 지정가 가격 (지정가 주문 시 필수, 시장가 주문은 불필요)
    /// Unit: USDT per base asset
    /// 단위: 기준 자산당 USDT
    #[schema(value_type = Option<String>, example = "100.0")]
    pub price: Option<Decimal>,

    /// Order amount (in base asset)
    /// 주문 수량 (기준 자산 기준)
    /// Supports decimal values (e.g., 0.1 SOL, 0.0001 SOL)
    /// 소수점 값 지원 (예: 0.1 SOL, 0.0001 SOL)
    /// 
    /// Note: For market buy orders, use `quote_amount` instead.
    /// 시장가 매수 주문의 경우 `quote_amount`를 사용하세요.
    #[schema(value_type = Option<String>, example = "1.0")]
    pub amount: Option<Decimal>,

    /// Quote amount (for market buy orders only)
    /// 금액 기반 주문 (시장가 매수만)
    /// 
    /// Example: "1000 USDT worth of SOL"
    /// 예: "1000 USDT어치 SOL 사기"
    /// 
    /// Rules:
    /// - Market buy: `quote_amount` is required (amount-based market buy is not supported)
    /// - Limit buy: `amount` is required
    /// - All sell orders: `amount` is required
    /// 
    /// 규칙:
    /// - 시장가 매수: `quote_amount` 필수 (수량 기반 시장가 매수는 지원하지 않음)
    /// - 지정가 매수: `amount` 필수
    /// - 모든 매도: `amount` 필수
    #[schema(value_type = Option<String>, example = "1000.0")]
    pub quote_amount: Option<Decimal>,
}

// =====================================================
// 주문 응답 (Order Response)
// =====================================================
/// 주문 응답 모델
/// Response model for order operations
#[derive(Debug, Serialize, ToSchema)]
#[schema(as = OrderResponse)]
pub struct OrderResponse {
    /// Order information
    /// 주문 정보
    pub order: Order,

    /// Success message
    /// 성공 메시지
    #[schema(example = "Order created successfully")]
    pub message: String,
}

// =====================================================
// 주문 목록 응답 (Orders List Response)
// =====================================================
/// 주문 목록 응답 모델
/// Response model for list of orders
#[derive(Debug, Serialize, ToSchema)]
#[schema(as = OrdersResponse)]
pub struct OrdersResponse {
    /// List of orders
    /// 주문 목록
    pub orders: Vec<Order>,
}

// =====================================================
// 오더북 응답 (Order Book Response)
// =====================================================
/// 오더북 항목 (가격별 주문 집계)
/// Order book entry (aggregated orders by price)
#[derive(Debug, Serialize, ToSchema)]
#[schema(as = OrderBookEntry)]
pub struct OrderBookEntry {
    /// Price level
    /// 가격 레벨
    #[schema(value_type = String, example = "100.0")]
    pub price: Decimal,

    /// Total amount at this price level
    /// 이 가격 레벨의 총 수량
    #[schema(value_type = String, example = "5.5")]
    pub amount: Decimal,

    /// Total value (price * amount)
    /// 총 가치 (가격 * 수량)
    #[schema(value_type = String, example = "550.0")]
    pub total: Decimal,
}

/// 오더북 응답 모델
/// Order book response model
#[derive(Debug, Serialize, ToSchema)]
#[schema(as = OrderBookResponse)]
pub struct OrderBookResponse {
    /// Buy orders (sorted by price descending, highest first)
    /// 매수 주문들 (가격 내림차순, 높은 가격부터)
    pub buy_orders: Vec<OrderBookEntry>,

    /// Sell orders (sorted by price ascending, lowest first)
    /// 매도 주문들 (가격 오름차순, 낮은 가격부터)
    pub sell_orders: Vec<OrderBookEntry>,
}

// =====================================================
// Order 생성용 (Repository에서 사용)
// =====================================================
/// 주문 생성 시 사용하는 내부 모델 (DB 저장용)
/// Internal model for creating orders (for database storage)
#[derive(Debug)]
pub struct OrderCreate {
    /// User ID
    /// 사용자 ID
    pub user_id: u64,

    /// Order type: 'buy' or 'sell'
    /// 주문 유형: 'buy' 또는 'sell'
    pub order_type: String,

    /// Order side: 'limit' or 'market'
    /// 주문 방식: 'limit' 또는 'market'
    pub order_side: String,

    /// Base asset
    /// 기준 자산
    pub base_mint: String,

    /// Quote currency
    /// 기준 통화
    pub quote_mint: String,

    /// Price (None for market orders)
    /// 가격 (시장가 주문은 None)
    pub price: Option<Decimal>,

    /// Order amount
    /// 주문 수량
    pub amount: Decimal,
}

