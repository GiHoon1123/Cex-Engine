use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

// =====================================================
// 엔진 내부 타입 정의
// Engine Internal Types
// =====================================================
// 체결 엔진 내부에서 사용하는 타입들을 정의합니다.
// 이 타입들은 엔진의 메모리 기반 처리에 최적화되어 있습니다.
// =====================================================

/// 거래쌍
/// Trading Pair
/// 
/// 거래소에서 거래되는 자산 쌍을 나타냅니다.
/// 예: SOL/USDT, USDC/USDT 등
/// 
/// # Fields
/// * `base_mint` - 기준 자산 (예: "SOL", "USDC")
/// * `quote_mint` - 기준 통화 (항상 "USDT")
/// 
/// # Examples
/// ```
/// let pair = TradingPair {
///     base_mint: "SOL".to_string(),
///     quote_mint: "USDT".to_string(),
/// };
/// // SOL/USDT 거래쌍을 나타냄
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TradingPair {
    /// 기준 자산 (Base Asset)
    /// 예: "SOL", "USDC", "RAY"
    pub base_mint: String,
    
    /// 기준 통화 (Quote Currency)
    /// 항상 "USDT"
    pub quote_mint: String,
}

impl TradingPair {
    /// 새 거래쌍 생성
    /// Create new trading pair
    /// 
    /// # Arguments
    /// * `base_mint` - 기준 자산
    /// * `quote_mint` - 기준 통화
    pub fn new(base_mint: String, quote_mint: String) -> Self {
        Self {
            base_mint,
            quote_mint,
        }
    }

    /// 거래쌍 문자열 표현 (예: "SOL/USDT")
    /// String representation of trading pair
    pub fn to_string(&self) -> String {
        format!("{}/{}", self.base_mint, self.quote_mint)
    }
}

/// 엔진 내부 주문 엔트리
/// Engine Internal Order Entry
/// 
/// 엔진의 메모리 오더북에서 사용하는 주문 구조체입니다.
/// DB 모델(Order)과 별도로 관리되며, 메모리 기반 처리에 최적화되어 있습니다.
/// 
/// # 차이점
/// - DB Order: 영구 저장, 모든 필드 포함
/// - OrderEntry: 메모리 전용, 매칭에 필요한 필드만 포함
/// 
/// # Fields
/// * `id` - 주문 ID (DB와 동일)
/// * `user_id` - 주문한 사용자 ID
/// * `order_type` - 매수("buy") 또는 매도("sell")
/// * `order_side` - 지정가("limit") 또는 시장가("market")
/// * `base_mint` - 기준 자산
/// * `quote_mint` - 기준 통화
/// * `price` - 주문 가격 (시장가는 None)
/// * `amount` - 주문 수량 (base_mint 기준)
/// * `filled_amount` - 체결된 수량
/// * `remaining_amount` - 남은 수량 (amount - filled_amount)
/// * `created_at` - 주문 생성 시간 (Time Priority에 사용)
/// 
/// # Examples
/// ```
/// // 지정가 매수 주문
/// let order = OrderEntry {
///     id: 1,
///     user_id: 100,
///     order_type: "buy".to_string(),
///     order_side: "limit".to_string(),
///     base_mint: "SOL".to_string(),
///     quote_mint: "USDT".to_string(),
///     price: Some(Decimal::new(100, 0)), // 100 USDT
///     amount: Decimal::new(10, 0),        // 10 SOL
///     filled_amount: Decimal::ZERO,
///     remaining_amount: Decimal::new(10, 0),
///     created_at: Utc::now(),
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderEntry {
    /// 주문 고유 ID
    /// Unique order ID (same as DB)
    pub id: u64,
    
    /// 주문한 사용자 ID
    /// User ID who placed the order
    pub user_id: u64,
    
    /// 주문 타입: "buy" (매수) 또는 "sell" (매도)
    /// Order type: "buy" or "sell"
    pub order_type: String,
    
    /// 주문 방식: "limit" (지정가) 또는 "market" (시장가)
    /// Order side: "limit" or "market"
    pub order_side: String,
    
    /// 기준 자산 (예: "SOL")
    /// Base asset
    pub base_mint: String,
    
    /// 기준 통화 (예: "USDT")
    /// Quote currency
    pub quote_mint: String,
    
    /// 주문 가격 (지정가만, 시장가는 None)
    /// Order price (limit orders only, None for market orders)
    pub price: Option<Decimal>,
    
    /// 주문 수량 (base_mint 기준)
    /// Order amount (in base_mint)
    /// 
    /// Note: For market buy orders with quote_amount, this is calculated from quote_amount / price
    /// 시장가 매수 주문의 경우 (quote_amount 기반), 이 값은 quote_amount / price로 계산됨
    pub amount: Decimal,
    
    /// 금액 기반 주문 (시장가 매수만)
    /// Quote amount (for market buy orders only)
    /// 
    /// Example: "1000 USDT worth of SOL"
    /// 예: "1000 USDT어치 SOL 사기"
    /// 
    /// If Some, this order is a market buy with quote amount.
    /// If None, this order uses amount (quantity-based).
    /// Some이면 시장가 매수 금액 기반, None이면 수량 기반
    pub quote_amount: Option<Decimal>,
    
    /// 체결된 수량 (base_mint 기준)
    /// Filled amount (in base_mint)
    pub filled_amount: Decimal,
    
    /// 남은 수량 (amount - filled_amount)
    /// Remaining amount
    pub remaining_amount: Decimal,
    
    /// 남은 금액 (quote_amount 기반 주문의 경우)
    /// Remaining quote amount (for quote_amount-based orders)
    /// 
    /// For market buy orders with quote_amount, this tracks remaining USDT to spend.
    /// 시장가 매수 주문(quote_amount 기반)의 경우, 남은 USDT 금액을 추적
    pub remaining_quote_amount: Option<Decimal>,
    
    /// 주문 생성 시간 (Time Priority에 사용)
    /// Order creation time (used for Time Priority)
    pub created_at: DateTime<Utc>,
}

impl OrderEntry {
    /// 주문이 완전 체결되었는지 확인
    /// Check if order is fully filled
    /// 
    /// # Returns
    /// * `true` - 주문이 완전히 체결됨 (remaining_amount == 0)
    /// * `false` - 아직 미체결 수량 존재
    pub fn is_fully_filled(&self) -> bool {
        self.remaining_amount == Decimal::ZERO
    }

    /// 주문이 매수 주문인지 확인
    /// Check if order is a buy order
    pub fn is_buy(&self) -> bool {
        self.order_type == "buy"
    }

    /// 주문이 매도 주문인지 확인
    /// Check if order is a sell order
    pub fn is_sell(&self) -> bool {
        self.order_type == "sell"
    }

    /// 주문이 시장가 주문인지 확인
    /// Check if order is a market order
    pub fn is_market(&self) -> bool {
        self.order_side == "market"
    }

    /// 주문이 지정가 주문인지 확인
    /// Check if order is a limit order
    pub fn is_limit(&self) -> bool {
        self.order_side == "limit"
    }
}

/// 매칭 결과
/// Match Result
/// 
/// 두 주문이 매칭되어 체결될 때의 정보를 담는 구조체입니다.
/// 매칭 알고리즘(Matcher)이 생성하고, 체결 실행기(Executor)가 처리합니다.
/// 
/// # Fields
/// * `buy_order_id` - 매수 주문 ID
/// * `sell_order_id` - 매도 주문 ID
/// * `price` - 체결 가격 (기존 주문의 가격 사용, Price Priority)
/// * `amount` - 체결 수량 (base_mint 기준)
/// * `base_mint` - 기준 자산
/// * `quote_mint` - 기준 통화
/// 
/// # Examples
/// ```
/// // 매수자 A와 매도자 B의 주문이 매칭됨
/// let match_result = MatchResult {
///     buy_order_id: 1,   // 매수자 A의 주문
///     sell_order_id: 2,  // 매도자 B의 주문
///     price: Decimal::new(100, 0),  // 100 USDT에 체결
///     amount: Decimal::new(1, 0),   // 1 SOL 체결
///     base_mint: "SOL".to_string(),
///     quote_mint: "USDT".to_string(),
/// };
/// // 이 결과는 Executor가 받아서 실제 체결 처리(잔고 업데이트, Trade 생성 등)
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchResult {
    /// 매수 주문 ID
    /// Buy order ID
    pub buy_order_id: u64,
    
    /// 매도 주문 ID
    /// Sell order ID
    pub sell_order_id: u64,
    
    /// 매수자 ID
    /// Buyer user ID
    pub buyer_id: u64,
    
    /// 매도자 ID
    /// Seller user ID
    pub seller_id: u64,
    
    /// 체결 가격 (USDT 기준)
    /// Execution price (in USDT)
    /// 
    /// Price Priority:
    /// - 기존에 오더북에 있던 주문의 가격 사용
    /// - 새로 들어온 주문이 아닌, 먼저 있던 주문의 가격으로 체결
    pub price: Decimal,
    
    /// 체결 수량 (base_mint 기준)
    /// Execution amount (in base_mint)
    /// 
    /// 부분 체결 가능:
    /// - min(buy_order.remaining, sell_order.remaining)
    pub amount: Decimal,
    
    /// 기준 자산
    /// Base asset
    pub base_mint: String,
    
    /// 기준 통화
    /// Quote currency
    pub quote_mint: String,
}

impl MatchResult {
    /// 체결 총액 계산 (price * amount)
    /// Calculate total value (price * amount)
    /// 
    /// # Returns
    /// 체결 총액 (USDT 기준)
    /// 
    /// # Examples
    /// ```
    /// let result = MatchResult {
    ///     price: Decimal::new(100, 0),  // 100 USDT
    ///     amount: Decimal::new(2, 0),   // 2 SOL
    ///     ...
    /// };
    /// let total = result.total_value(); // 200 USDT
    /// ```
    pub fn total_value(&self) -> Decimal {
        self.price * self.amount
    }
}

/// 엔진 이벤트 타입
/// Engine Event Type
/// 
/// 엔진에서 발생하는 이벤트를 나타냅니다.
/// WAL (Write-Ahead Log) 및 DB 쓰기 큐에서 사용됩니다.
/// 
/// # Variants
/// * `OrderCreated` - 주문 생성
/// * `OrderCancelled` - 주문 취소
/// * `TradeExecuted` - 체결 실행
/// 
/// # 용도
/// 1. WAL 기록: 복구를 위해 모든 이벤트를 순차적으로 기록
/// 2. DB 쓰기 큐: 비동기 배치 쓰기를 위한 이벤트 큐
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EngineEvent {
    /// 주문 생성 이벤트
    /// Order created event
    OrderCreated {
        /// 생성된 주문
        order: OrderEntry,
    },
    
    /// 주문 취소 이벤트
    /// Order cancelled event
    OrderCancelled {
        /// 취소된 주문 ID
        order_id: u64,
        
        /// 주문한 사용자 ID
        user_id: u64,
        
        /// 거래쌍
        trading_pair: TradingPair,
    },
    
    /// 체결 실행 이벤트
    /// Trade executed event
    TradeExecuted {
        /// 매칭 결과
        match_result: MatchResult,
        
        /// 매수자 ID
        buyer_id: u64,
        
        /// 매도자 ID
        seller_id: u64,
    },
}

/// 주문 상태
/// Order Status
/// 
/// 주문의 현재 상태를 나타냅니다.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderStatus {
    /// 대기 중 (아직 체결 안 됨)
    /// Pending (not yet filled)
    Pending,
    
    /// 부분 체결 (일부만 체결됨)
    /// Partially filled
    Partial,
    
    /// 전량 체결
    /// Fully filled
    Filled,
    
    /// 취소됨
    /// Cancelled
    Cancelled,
}

impl OrderStatus {
    /// 문자열로 변환
    /// Convert to string
    pub fn as_str(&self) -> &str {
        match self {
            OrderStatus::Pending => "pending",
            OrderStatus::Partial => "partial",
            OrderStatus::Filled => "filled",
            OrderStatus::Cancelled => "cancelled",
        }
    }

    /// 문자열에서 변환
    /// Convert from string
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(OrderStatus::Pending),
            "partial" => Some(OrderStatus::Partial),
            "filled" => Some(OrderStatus::Filled),
            "cancelled" => Some(OrderStatus::Cancelled),
            _ => None,
        }
    }
}
