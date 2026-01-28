// =====================================================
// 체결 엔진 모듈
// Matching Engine Module
// =====================================================
// 고성능 주문 매칭 및 체결 엔진을 제공합니다.
// 
// 구조:
// - types: 엔진 내부 타입 정의
// - Engine trait: 엔진 인터페이스 (구현체와 분리)
// - 실제 구현: orderbook, matcher, executor 등 (나중에 구현)
// 
// 설계 철학:
// - 인터페이스와 구현 분리 (Dependency Inversion)
// - Service는 trait만 참조 (구체적 구현 몰라도 됨)
// - 나중에 다른 구현체로 교체 가능 (Mock, 고성능 버전 등)
// =====================================================

pub mod types;
pub mod mock;
pub mod orderbook;
pub mod matcher;
pub mod executor;
pub mod balance_cache;
pub mod wal;
pub mod runtime;

// TODO: 나중에 구현
// pub mod db_writer;

use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;
use chrono::Utc;

pub use types::{
    TradingPair, OrderEntry, MatchResult, EngineEvent, OrderStatus,
};
pub use mock::MockEngine;

// =====================================================
// Engine Trait (엔진 인터페이스)
// =====================================================
// 체결 엔진의 공개 인터페이스를 정의합니다.
// Service 계층은 이 trait만 사용하여 엔진과 통신합니다.
// 
// 장점:
// 1. 구현 세부사항 숨김 (Information Hiding)
// 2. 테스트 용이성 (Mock 구현 가능)
// 3. 다양한 구현체 지원 (동기/비동기, 단순/고성능)
// =====================================================

/// 체결 엔진 인터페이스
/// Matching Engine Interface
/// 
/// 주문 제출, 체결, 취소 등 엔진의 핵심 기능을 정의합니다.
/// 
/// # 구현체
/// - `SimpleEngine`: 동기 방식, DB 직접 사용 (Phase 1)
/// - `HighPerfEngine`: 메모리 기반, WAL, Ring Buffer (Phase 2)
/// - `MockEngine`: 테스트용
/// 
/// # 사용 예시
/// ```
/// // Service에서 사용
/// pub struct OrderService {
///     engine: Arc<dyn Engine>,  // trait 객체로 사용
///     // ...
/// }
/// 
/// impl OrderService {
///     pub async fn create_order(&self, ...) -> Result<Order> {
///         // 엔진에 주문 제출
///         self.engine.submit_order(order).await?;
///         Ok(order)
///     }
/// }
/// ```
#[async_trait]
pub trait Engine: Send + Sync {
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 주문 관리 (Order Management)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /// 주문 제출
    /// Submit order to engine
    /// 
    /// 새로운 주문을 엔진에 제출합니다.
    /// 엔진은 이 주문을 오더북에 추가하고, 즉시 매칭을 시도합니다.
    /// 
    /// # Arguments
    /// * `order` - 제출할 주문
    /// 
    /// # Returns
    /// * `Ok(Vec<MatchResult>)` - 체결된 결과 목록 (부분 체결 가능)
    /// * `Err` - 주문 제출 실패 (잔고 부족, 유효하지 않은 주문 등)
    /// 
    /// # 처리 과정
    /// 1. 주문 유효성 검증
    /// 2. 잔고 확인 & 잠금
    /// 3. 오더북에 추가
    /// 4. 매칭 시도
    /// 5. 체결 실행 (있을 경우)
    /// 6. 체결 결과 반환
    /// 
    /// # Examples
    /// ```
    /// let order = OrderEntry {
    ///     id: 1,
    ///     user_id: 100,
    ///     order_type: "buy".to_string(),
    ///     // ...
    /// };
    /// 
    /// let matches = engine.submit_order(order).await?;
    /// 
    /// if matches.is_empty() {
    ///     println!("주문이 오더북에 등록됨 (체결 안 됨)");
    /// } else {
    ///     println!("{}건 체결됨", matches.len());
    /// }
    /// ```
    async fn submit_order(&self, order: OrderEntry) -> Result<()>;

    /// 주문 취소
    /// Cancel order
    /// 
    /// 오더북에 있는 주문을 취소합니다.
    /// 
    /// # Arguments
    /// * `order_id` - 취소할 주문 ID
    /// * `user_id` - 주문한 사용자 ID (권한 확인용)
    /// * `trading_pair` - 거래쌍
    /// 
    /// # Returns
    /// * `Ok(OrderEntry)` - 취소된 주문 정보
    /// * `Err` - 주문 취소 실패 (존재하지 않음, 권한 없음, 이미 체결됨 등)
    /// 
    /// # 처리 과정
    /// 1. 주문 존재 확인
    /// 2. 권한 확인 (user_id 일치)
    /// 3. 오더북에서 제거
    /// 4. 잠긴 잔고 해제
    /// 5. 주문 상태 업데이트 (cancelled)
    /// 
    /// # Examples
    /// ```
    /// let cancelled = engine.cancel_order(
    ///     1,  // order_id
    ///     100, // user_id
    ///     trading_pair,
    /// ).await?;
    /// 
    /// println!("주문 취소: {:?}", cancelled);
    /// ```
    async fn cancel_order(
        &self,
        order_id: u64,
        user_id: u64,
        trading_pair: &TradingPair,
    ) -> Result<OrderEntry>;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 오더북 조회 (OrderBook Query)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /// 오더북 조회 (매수/매도 주문 목록)
    /// Get orderbook (buy/sell orders)
    /// 
    /// 특정 거래쌍의 오더북을 조회합니다.
    /// 
    /// # Arguments
    /// * `trading_pair` - 조회할 거래쌍
    /// * `depth` - 조회할 가격 레벨 개수 (None이면 전체)
    /// 
    /// # Returns
    /// * `(매수 주문 목록, 매도 주문 목록)`
    /// * 매수: 가격 내림차순 정렬 (높은 가격 먼저)
    /// * 매도: 가격 오름차순 정렬 (낮은 가격 먼저)
    /// 
    /// # Examples
    /// ```
    /// let (buy_orders, sell_orders) = engine.get_orderbook(
    ///     &trading_pair,
    ///     Some(10),  // 상위 10개 레벨만
    /// ).await?;
    /// 
    /// println!("매수 주문: {} 건", buy_orders.len());
    /// println!("매도 주문: {} 건", sell_orders.len());
    /// ```
    async fn get_orderbook(
        &self,
        trading_pair: &TradingPair,
        depth: Option<usize>,
    ) -> Result<(Vec<OrderEntry>, Vec<OrderEntry>)>;

    /// 최고 매수가 조회 (Best Bid)
    /// Get best bid price
    /// 
    /// 오더북에서 가장 높은 매수 가격을 조회합니다.
    /// 
    /// # Arguments
    /// * `trading_pair` - 거래쌍
    /// 
    /// # Returns
    /// * `Some(price)` - 최고 매수가
    /// * `None` - 매수 주문 없음
    async fn get_best_bid(&self, trading_pair: &TradingPair) -> Result<Option<Decimal>>;

    /// 최저 매도가 조회 (Best Ask)
    /// Get best ask price
    /// 
    /// 오더북에서 가장 낮은 매도 가격을 조회합니다.
    /// 
    /// # Arguments
    /// * `trading_pair` - 거래쌍
    /// 
    /// # Returns
    /// * `Some(price)` - 최저 매도가
    /// * `None` - 매도 주문 없음
    async fn get_best_ask(&self, trading_pair: &TradingPair) -> Result<Option<Decimal>>;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 잔고 관리 (Balance Management)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /// 잔고 잠금 (주문 생성 시)
    /// Lock balance for order
    /// 
    /// 주문 생성 시 필요한 잔고를 잠급니다.
    /// 
    /// # Arguments
    /// * `user_id` - 사용자 ID
    /// * `mint` - 자산 종류
    /// * `amount` - 잠글 수량
    /// 
    /// # Returns
    /// * `Ok(())` - 잠금 성공
    /// * `Err` - 잠금 실패 (잔고 부족)
    /// 
    /// # 동작
    /// - available 감소
    /// - locked 증가
    /// 
    /// # Examples
    /// ```
    /// // 매수 주문: USDT 잠금
    /// engine.lock_balance(
    ///     user_id,
    ///     "USDT",
    ///     price * amount,  // 필요한 USDT
    /// ).await?;
    /// ```
    async fn lock_balance(
        &self,
        user_id: u64,
        mint: &str,
        amount: Decimal,
    ) -> Result<()>;

    /// 잔고 잠금 해제 (주문 취소 시)
    /// Unlock balance (order cancelled)
    /// 
    /// 주문 취소 시 잠긴 잔고를 해제합니다.
    /// 
    /// # Arguments
    /// * `user_id` - 사용자 ID
    /// * `mint` - 자산 종류
    /// * `amount` - 해제할 수량
    /// 
    /// # Returns
    /// * `Ok(())` - 해제 성공
    /// 
    /// # 동작
    /// - locked 감소
    /// - available 증가
    async fn unlock_balance(
        &self,
        user_id: u64,
        mint: &str,
        amount: Decimal,
    ) -> Result<()>;

    /// 사용 가능 잔고 조회
    /// Get available balance
    /// 
    /// 사용자의 특정 자산 잔고를 조회합니다.
    /// 
    /// # Arguments
    /// * `user_id` - 사용자 ID
    /// * `mint` - 자산 종류
    /// 
    /// # Returns
    /// * `(available, locked)` - (사용 가능, 잠김) 잔고
    async fn get_balance(&self, user_id: u64, mint: &str) -> Result<(Decimal, Decimal)>;

    /// 잔고 업데이트 (입금/출금)
    /// Update balance (deposit/withdrawal)
    /// 
    /// 외부 입금/출금 이벤트를 처리하여 사용자 잔고를 업데이트합니다.
    /// 
    /// # 사용 시나리오
    /// 1. 외부 지갑에서 우리 지갑으로 자산 입금 (온체인 트랜잭션)
    /// 2. 어드민 또는 이벤트로 인한 서비스 내 잔액 업데이트
    /// 
    /// # Arguments
    /// * `user_id` - 사용자 ID
    /// * `mint` - 자산 종류 (예: "SOL", "USDT")
    /// * `available_delta` - available 증감량 (양수: 입금, 음수: 출금)
    /// 
    /// # Returns
    /// * `Ok(())` - 업데이트 성공
    /// * `Err` - 업데이트 실패
    /// 
    /// # 처리 과정
    /// 1. BalanceCache에서 잔고 조회/생성
    /// 2. available 업데이트 (기존 + delta)
    /// 3. WAL 메시지 발행 (BalanceUpdated)
    /// 4. DB 명령 전송 (UpdateBalance) → DB Writer가 배치로 처리
    /// 
    /// # Examples
    /// ```rust
    /// // 100 USDT 입금
    /// engine.update_balance(
    ///     123,  // user_id
    ///     "USDT",
    ///     Decimal::new(100, 0),  // +100 USDT
    /// ).await?;
    /// 
    /// // 50 USDT 출금
    /// engine.update_balance(
    ///     123,  // user_id
    ///     "USDT",
    ///     Decimal::new(-50, 0),  // -50 USDT
    /// ).await?;
    /// ```
    async fn update_balance(
        &self,
        user_id: u64,
        mint: &str,
        available_delta: Decimal,
    ) -> Result<()>;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 시스템 관리 (System Management)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /// 엔진 시작
    /// Start engine
    /// 
    /// 엔진을 초기화하고 시작합니다.
    /// 
    /// # 처리 과정
    /// 1. 메모리 오더북 구성
    /// 2. DB에서 잔고 로드
    /// 3. BalanceCache에 로드
    /// 4. WAL 스레드 시작
    /// 5. 엔진 스레드 시작
    /// 
    /// # Note
    /// 이 메서드는 서버 시작 시 한 번만 호출됩니다.
    /// `&mut self`를 사용하여 필드를 직접 수정합니다.
    async fn start(&mut self) -> Result<()>;

    /// 엔진 정지
    /// Stop engine
    /// 
    /// 엔진을 안전하게 종료합니다.
    /// 
    /// # 처리 과정
    /// 1. 실행 플래그 해제
    /// 2. 채널 닫기 (스레드 루프 종료)
    /// 3. 엔진 스레드 종료 대기
    /// 4. WAL 스레드 종료 대기
    /// 5. 최종 WAL 동기화
    /// 
    /// # Note
    /// 이 메서드는 서버 종료 시 한 번만 호출됩니다.
    /// `&mut self`를 사용하여 필드를 직접 수정합니다.
    async fn stop(&mut self) -> Result<()>;
}

// =====================================================
// 유틸리티 함수
// Utility Functions
// =====================================================

/// DB Order 모델을 Engine OrderEntry로 변환
/// Convert DB Order to Engine OrderEntry
/// 
/// # Arguments
/// * `order` - DB Order 모델
/// 
/// # Returns
/// * `OrderEntry` - 엔진 내부 주문 엔트리
/// 
/// # Note
/// Service 계층에서 DB Order를 엔진에 제출할 때 사용합니다.
pub fn order_to_entry(order: &crate::domains::cex::models::order::Order) -> OrderEntry {
    OrderEntry {
        id: order.id,
        user_id: order.user_id,
        order_type: order.order_type.clone(),
        order_side: order.order_side.clone(),
        base_mint: order.base_mint.clone(),
        quote_mint: order.quote_mint.clone(),
        price: order.price,
        amount: order.amount,
        quote_amount: None, // DB에서 로드한 주문은 수량 기반으로 가정
        filled_amount: order.filled_amount,
        remaining_amount: order.amount - order.filled_amount,
        remaining_quote_amount: None,
        created_at: order.created_at,
    }
}

/// Engine OrderEntry를 DB Order 모델로 변환
/// Convert Engine OrderEntry to DB Order
/// 
/// # Arguments
/// * `entry` - 엔진 내부 주문 엔트리
/// 
/// # Returns
/// * `Order` - DB Order 모델
/// 
/// # Note
/// 엔진에서 처리된 주문을 DB에 저장할 때 사용합니다.
pub fn entry_to_order(entry: &OrderEntry) -> crate::domains::cex::models::order::Order {
    // 주문 상태 결정
    let status = if entry.filled_amount == Decimal::ZERO {
        "pending".to_string()
    } else if entry.is_fully_filled() {
        "filled".to_string()
    } else {
        "partial".to_string()
    };

    crate::domains::cex::models::order::Order {
        id: entry.id,
        user_id: entry.user_id,
        order_type: entry.order_type.clone(),
        order_side: entry.order_side.clone(),
        base_mint: entry.base_mint.clone(),
        quote_mint: entry.quote_mint.clone(),
        price: entry.price,
        amount: entry.amount,
        filled_amount: entry.filled_amount,
        filled_quote_amount: Decimal::ZERO, // OrderEntry에는 filled_quote_amount가 없으므로 0으로 설정 (DB에서 조회 시 실제 값 사용)
        status,
        created_at: entry.created_at,
        updated_at: Utc::now(),
    }
}
