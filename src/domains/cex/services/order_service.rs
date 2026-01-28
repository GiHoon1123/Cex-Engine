use std::sync::Arc;
use crate::shared::database::{Database, OrderRepository};
use crate::domains::cex::models::order::{Order, CreateOrderRequest};
use crate::domains::cex::engine::{Engine, TradingPair, OrderEntry, entry_to_order, runtime::HighPerformanceEngine};
use anyhow::{Context, Result, bail};
use rust_decimal::Decimal;
use chrono::Utc;

/// 주문 서비스
/// Order Service
/// 
/// 역할:
/// - 주문 생성/취소/조회 비즈니스 로직 담당
/// - 잔고 확인 및 검증
/// - 체결 엔진과 통신
/// 
/// 아키텍처:
/// - OrderService → Engine trait → 실제 Engine 구현체
/// - Service는 Engine의 구체적 구현을 몰라도 됨 (Dependency Inversion)
/// - 나중에 Engine 구현체만 교체하면 자동으로 연동
/// 
/// 처리 흐름:
/// 1. API Handler → OrderService
/// 2. OrderService → 검증 (잔고, 권한 등)
/// 3. OrderService → Repository (DB 저장)
/// 4. OrderService → Engine (매칭 엔진 제출)
/// 5. Engine → 매칭 & 체결 처리
/// 
/// # Examples
/// ```
/// let service = OrderService::new(db, engine);
/// 
/// // 주문 생성
/// let order = service.create_order(user_id, request).await?;
/// 
/// // 주문 취소
/// service.cancel_order(user_id, order_id).await?;
/// ```
#[derive(Clone)]
pub struct OrderService {
    /// 데이터베이스 연결
    /// Database connection
    db: Database,
    
    /// 체결 엔진 (구체 타입 직접 사용)
    /// Matching engine (concrete type)
    /// 
    /// 엔진은 하나만 존재하므로 구체 타입을 직접 사용합니다.
    /// Wrapper 없이 동일한 인스턴스를 공유합니다.
    engine: Arc<tokio::sync::Mutex<HighPerformanceEngine>>,
}

impl OrderService {
    /// 생성자
    /// Constructor
    /// 
    /// # Arguments
    /// * `db` - 데이터베이스 연결
    /// * `engine` - 체결 엔진 (trait 객체)
    /// 
    /// # Returns
    /// OrderService 인스턴스
    /// 
    /// # Examples
    /// ```
    /// let engine = Arc::new(SimpleEngine::new(...));
    /// let service = OrderService::new(db, engine);
    /// ```
    pub fn new(db: Database, engine: Arc<tokio::sync::Mutex<HighPerformanceEngine>>) -> Self {
        Self { db, engine }
    }

    /// 주문 생성
    /// Create order
    /// 
    /// 새로운 주문을 생성하고 체결 엔진에 제출합니다.
    /// 
    /// # Arguments
    /// * `user_id` - 주문을 생성하는 사용자 ID
    /// * `request` - 주문 생성 요청 (가격, 수량, 타입 등)
    /// 
    /// # Returns
    /// * `Ok(Order)` - 생성된 주문 정보
    /// * `Err` - 주문 생성 실패 (잔고 부족, 유효하지 않은 주문 등)
    /// 
    /// # 처리 과정
    /// 1. 주문 유효성 검증 (가격, 수량, 타입)
    /// 2. 잔고 확인 (필요한 금액이 있는지)
    /// 3. DB에 주문 저장
    /// 4. 엔진에 잔고 잠금 요청
    /// 5. 엔진에 주문 제출 (매칭 시도)
    /// 6. 생성된 주문 반환
    /// 
    /// # Errors
    /// - 잔고 부족 시
    /// - 유효하지 않은 주문 파라미터
    /// - 데이터베이스 오류
    /// - 엔진 오류
    /// 
    /// # Examples
    /// ```
    /// let request = CreateOrderRequest {
    ///     order_type: "buy".to_string(),
    ///     order_side: "limit".to_string(),
    ///     base_mint: "SOL".to_string(),
    ///     quote_mint: "USDT".to_string(),
    ///     price: Some(Decimal::new(100, 0)),
    ///     amount: Decimal::new(1, 0),
    /// };
    /// 
    /// let order = service.create_order(user_id, request).await?;
    /// println!("주문 생성: ID {}", order.id);
    /// ```
    pub async fn create_order(
        &self,
        user_id: u64,
        request: CreateOrderRequest,
    ) -> Result<Order> {
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 1. 주문 유효성 검증
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        self.validate_order_request(&request)?;

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 2. 필요한 잔고 계산 및 확인
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        let (required_mint, required_amount) = self.calculate_required_balance(&request)?;
        
        self.check_balance(user_id, &required_mint, required_amount).await?;

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 3. 주문 파라미터 준비
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // quote_mint 미리 결정 (여러 곳에서 사용되므로)
        let quote_mint = request.quote_mint.unwrap_or_else(|| "USDT".to_string());

        // 시장가 매수: 금액 기반만 지원 (quote_amount 필수, amount는 매칭 시 계산됨)
        // 지정가 매수/모든 매도: 수량 기반 (amount 필수)
        let (amount, remaining_amount, quote_amount, remaining_quote_amount) = 
            if request.order_type == "buy" && request.order_side == "market" {
                // 시장가 매수 금액 기반: amount는 0으로 시작 (매칭 시 계산됨)
                let quote_amt = request.quote_amount.unwrap();
                (Decimal::ZERO, Decimal::ZERO, Some(quote_amt), Some(quote_amt))
            } else {
                // 수량 기반 주문 (지정가 매수, 모든 매도)
                let amt = request.amount.unwrap();
                (amt, amt, None, None)
            };

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 3. 주문 ID 결정 (Java에서 전달된 ID 필수)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // Java API 서버에서 생성한 주문 ID를 반드시 사용해야 함
        // 모든 주문은 Java API를 통해 생성되므로 order_id가 필수임
        let order_id = if let Some(java_order_id) = request.order_id {
            eprintln!("[OrderService] Java에서 전달된 order_id 사용: {}", java_order_id);
            java_order_id
        } else {
            // Java에서 order_id를 전달하지 않은 경우 에러 발생
            eprintln!("[OrderService] ERROR: Java에서 order_id를 전달하지 않음! 주문 생성 실패");
            return Err(anyhow::anyhow!("order_id is required. All orders must be created through Java API first."));
        };

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 4. 엔진에 주문 즉시 제출 (블로킹 없음!)
        // 주의: 잔고 잠금은 process_submit_order에서 처리됨 (중복 방지)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        let order_entry = OrderEntry {
            id: order_id,  // Java에서 전달된 주문 ID
            user_id,
            order_type: request.order_type.clone(),
            order_side: request.order_side.clone(),
            base_mint: request.base_mint.clone(),
            quote_mint: quote_mint.clone(),
            price: request.price,
            amount,
            quote_amount,
            filled_amount: Decimal::ZERO,
            remaining_amount,
            remaining_quote_amount,
            created_at: Utc::now(),
        };
        
        // 엔진에 제출 (비동기 처리, 백그라운드에서 처리)
        // 실제 거래소와 동일하게 주문을 즉시 반환하고 백그라운드에서 처리
        // 엔진이 내부적으로 WAL 기록 + DB 동기화 처리
        // 주의: DB에 이미 주문이 있으므로, 엔진의 DB Writer는 InsertOrder를 보내지 않음
        let order_id_for_log = order_entry.id;
        eprintln!("[OrderService] 엔진에 주문 제출 시작: order_id={}, user_id={}, order_type={}, order_side={}, price={:?}, amount={}", 
                 order_id_for_log, user_id, request.order_type, request.order_side, request.price, amount);
        let engine_clone = self.engine.clone();
        tokio::spawn(async move {
            eprintln!("[OrderService] 엔진에 주문 제출 중: order_id={}", order_entry.id);
            let engine_guard = engine_clone.lock().await;
            eprintln!("[OrderService] 엔진 락 획득 완료: order_id={}", order_entry.id);
            let order_id_after_submit = order_entry.id;
            match engine_guard.submit_order(order_entry).await {
                Ok(_) => {
                    eprintln!("[OrderService] 엔진에 주문 제출 성공: order_id={}", order_id_after_submit);
                }
                Err(e) => {
                    eprintln!("[OrderService] 엔진에 주문 제출 실패: order_id={}, error={}", order_id_after_submit, e);
                }
            }
        });

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 6. Order 객체 반환
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // DB 저장은 엔진의 백그라운드 워커가 처리
        // (WAL → DB 동기화)
        // 
        // Note: 시장가 매수 금액 기반의 경우, amount는 매칭 후 계산됨
        // 일단 0으로 설정 (엔진에서 매칭 후 업데이트)
        let order_amount = if request.order_type == "buy" && request.order_side == "market" && request.quote_amount.is_some() {
            Decimal::ZERO // 매칭 후 계산됨
        } else {
            request.amount.unwrap()
        };
        
        let order = Order {
            id: order_id,  // Java에서 전달된 주문 ID
            user_id,
            order_type: request.order_type,
            order_side: request.order_side,
            base_mint: request.base_mint,
            quote_mint,
            price: request.price,
            amount: order_amount,
            filled_amount: Decimal::ZERO,
            filled_quote_amount: Decimal::ZERO,
            status: "pending".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        Ok(order)
    }

    /// 주문 취소
    /// Cancel order
    /// 
    /// 대기 중이거나 부분 체결된 주문을 취소합니다.
    /// 
    /// # Arguments
    /// * `user_id` - 주문을 취소하려는 사용자 ID (권한 확인용)
    /// * `order_id` - 취소할 주문 ID
    /// 
    /// # Returns
    /// * `Ok(Order)` - 취소된 주문 정보
    /// * `Err` - 취소 실패 (존재하지 않음, 권한 없음, 이미 체결됨 등)
    /// 
    /// # 처리 과정
    /// 1. 주문 존재 확인
    /// 2. 권한 확인 (본인 주문인지)
    /// 3. 취소 가능 상태 확인 (pending/partial만 가능)
    /// 4. 엔진에 취소 요청
    /// 5. DB에서 주문 상태 업데이트 (cancelled)
    /// 6. 잠긴 잔고 해제
    /// 
    /// # Errors
    /// - 주문이 존재하지 않음
    /// - 권한 없음 (다른 사용자의 주문)
    /// - 이미 전량 체결됨
    /// - 이미 취소됨
    /// 
    /// # Examples
    /// ```
    /// let cancelled = service.cancel_order(user_id, order_id).await?;
    /// println!("주문 취소: {}", cancelled.id);
    /// ```
    pub async fn cancel_order(
        &self,
        user_id: u64,
        order_id: u64,
    ) -> Result<Order> {
        let order_repo = OrderRepository::new(self.db.pool().clone());

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 1. 주문 조회
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        let order = order_repo
            .get_by_id(order_id)
            .await
            .context("Failed to fetch order from database")?
            .ok_or_else(|| anyhow::anyhow!("Order not found: {}", order_id))?;

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 2. 권한 확인 (본인 주문인지)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if order.user_id != user_id {
            bail!("Unauthorized: You don't own this order");
        }

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 3. 취소 가능 상태 확인
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        match order.status.as_str() {
            "filled" => bail!("Cannot cancel: Order already fully filled"),
            "cancelled" => bail!("Cannot cancel: Order already cancelled"),
            "pending" | "partial" => {
                // 취소 가능
            }
            _ => bail!("Invalid order status: {}", order.status),
        }

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 4. 엔진에 취소 요청
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        let trading_pair = TradingPair::new(
            order.base_mint.clone(),
            order.quote_mint.clone(),
        );

        let _cancelled_entry = {
            let engine_guard = self.engine.lock().await;
            engine_guard
                .cancel_order(order_id, user_id, &trading_pair)
                .await
                .context("Failed to cancel order in engine")?
        };

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 5. DB에서 주문 상태 업데이트
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        let updated_order = order_repo
            .update_status(order_id, "cancelled")
            .await
            .context("Failed to update order status to cancelled")?;

        Ok(updated_order)
    }

    /// 특정 주문 조회
    /// Get order by ID
    /// 
    /// 활성 주문(pending, partial)은 엔진 메모리에서 실시간 조회합니다.
    /// 완료 주문(filled, cancelled)은 DB에서 조회합니다.
    /// 
    /// # Arguments
    /// * `user_id` - 조회하는 사용자 ID (권한 확인용)
    /// * `order_id` - 조회할 주문 ID
    /// 
    /// # Returns
    /// * `Ok(Order)` - 주문 정보
    /// * `Err` - 조회 실패 (존재하지 않음, 권한 없음)
    pub async fn get_order(
        &self,
        user_id: u64,
        order_id: u64,
    ) -> Result<Order> {
        let order_repo = OrderRepository::new(self.db.pool().clone());

        // DB에서 주문 조회
        let mut order = order_repo
            .get_by_id(order_id)
            .await
            .context("Failed to fetch order from database")?
            .ok_or_else(|| anyhow::anyhow!("Order not found: {}", order_id))?;

        // 권한 확인 (본인 주문만 조회 가능)
        if order.user_id != user_id {
            bail!("Unauthorized: You don't own this order");
        }

        // 활성 주문(pending, partial)인 경우 엔진 메모리에서 최신 정보 확인
        if order.status == "pending" || order.status == "partial" {
            // 엔진 메모리 OrderBook에서 해당 주문 찾기
            let trading_pair = TradingPair::new(
                order.base_mint.clone(),
                order.quote_mint.clone(),
            );
            
            let (buy_orders, sell_orders) = {
                let engine_guard = self.engine.lock().await;
                engine_guard
                    .get_orderbook(&trading_pair, None)
                    .await
                    .context("Failed to get orderbook from engine")?
            };

            // 엔진 메모리에서 주문 찾기
            let engine_order = buy_orders.iter()
                .chain(sell_orders.iter())
                .find(|o| o.id == order_id);

            if let Some(engine_order_entry) = engine_order {
                // 엔진 메모리 값으로 업데이트 (실시간 정확성 보장)
                order = entry_to_order(engine_order_entry);
            }
            // 엔진 메모리에 없으면 DB 값 사용 (이미 체결되었을 수 있음)
        }

        Ok(order)
    }

    /// 사용자의 모든 주문 조회
    /// Get all orders for user
    /// 
    /// 활성 주문(pending, partial)은 엔진 메모리에서 실시간 조회합니다.
    /// 완료 주문(filled, cancelled)은 DB에서 조회합니다.
    /// 
    /// # Arguments
    /// * `user_id` - 사용자 ID
    /// * `status` - 주문 상태 필터 (None이면 전체)
    /// * `limit` - 최대 조회 개수
    /// * `offset` - 페이지네이션 오프셋
    /// 
    /// # Returns
    /// * `Ok(Vec<Order>)` - 주문 목록 (최신순)
    pub async fn get_my_orders(
        &self,
        user_id: u64,
        status: Option<&str>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<Order>> {
        let order_repo = OrderRepository::new(self.db.pool().clone());

        // DB에서 주문 목록 조회
        let mut orders = if let Some(status) = status {
            order_repo
                .get_by_user_and_status(user_id, status, limit, offset)
                .await
                .context("Failed to fetch user orders from database")?
        } else {
            order_repo
                .get_all_by_user(user_id, limit, offset)
                .await
                .context("Failed to fetch user orders from database")?
        };

        // 활성 주문(pending, partial)인 경우 엔진 메모리에서 최신 정보 확인
        // 모든 거래쌍의 OrderBook을 조회하여 활성 주문 업데이트
        let engine_guard = self.engine.lock().await;
        
        // 활성 주문만 필터링
        let active_orders: Vec<_> = orders.iter()
            .filter(|o| o.status == "pending" || o.status == "partial")
            .map(|o| (o.id, TradingPair::new(o.base_mint.clone(), o.quote_mint.clone())))
            .collect();

        // 각 활성 주문에 대해 엔진 메모리에서 최신 정보 확인
        for order in &mut orders {
            if order.status == "pending" || order.status == "partial" {
                let trading_pair = TradingPair::new(
                    order.base_mint.clone(),
                    order.quote_mint.clone(),
                );
                
                // 엔진 메모리 OrderBook에서 해당 주문 찾기
                let (buy_orders, sell_orders) = match engine_guard
                    .get_orderbook(&trading_pair, None)
                    .await
                {
                    Ok(orders) => orders,
                    Err(_) => continue, // 실패 시 DB 값 사용
                };

                // 엔진 메모리에서 주문 찾기
                if let Some(engine_order_entry) = buy_orders.iter()
                    .chain(sell_orders.iter())
                    .find(|o| o.id == order.id)
                {
                    // 엔진 메모리 값으로 업데이트 (실시간 정확성 보장)
                    *order = entry_to_order(engine_order_entry);
                }
                // 엔진 메모리에 없으면 DB 값 사용 (이미 체결되었을 수 있음)
            }
        }

        Ok(orders)
    }

    /// 오더북 조회 (호가창)
    /// Get orderbook (bid/ask orders)
    /// 
    /// 특정 거래쌍의 매수/매도 호가를 조회합니다.
    /// 
    /// # Arguments
    /// * `base_mint` - 기준 자산 (예: "SOL")
    /// * `quote_mint` - 기준 통화 (예: "USDT")
    /// * `depth` - 조회할 가격 레벨 개수 (None이면 전체)
    /// 
    /// # Returns
    /// * `(매수 주문 목록, 매도 주문 목록)`
    /// * 매수: 가격 내림차순 (높은 가격 우선)
    /// * 매도: 가격 오름차순 (낮은 가격 우선)
    /// 
    /// # Examples
    /// ```
    /// let (bids, asks) = service.get_orderbook("SOL", "USDT", Some(10)).await?;
    /// println!("매수 호가: {} 건", bids.len());
    /// println!("매도 호가: {} 건", asks.len());
    /// ```
    pub async fn get_orderbook(
        &self,
        base_mint: &str,
        quote_mint: &str,
        depth: Option<usize>,
    ) -> Result<(Vec<Order>, Vec<Order>)> {
        let trading_pair = TradingPair::new(
            base_mint.to_string(),
            quote_mint.to_string(),
        );

        // 엔진에서 오더북 조회
        let (buy_entries, sell_entries) = {
            let engine_guard = self.engine.lock().await;
            engine_guard
                .get_orderbook(&trading_pair, depth)
                .await
                .context("Failed to get orderbook from engine")?
        };

        // OrderEntry → Order 변환
        let buy_orders: Vec<Order> = buy_entries
            .into_iter()
            .map(|entry| entry_to_order(&entry))
            .collect();

        let sell_orders: Vec<Order> = sell_entries
            .into_iter()
            .map(|entry| entry_to_order(&entry))
            .collect();

        Ok((buy_orders, sell_orders))
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 내부 헬퍼 함수들 (Private Helper Methods)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /// 주문 요청 유효성 검증
    /// Validate order request
    fn validate_order_request(&self, request: &CreateOrderRequest) -> Result<()> {
        // 주문 타입 확인
        if request.order_type != "buy" && request.order_type != "sell" {
            bail!("Invalid order_type: must be 'buy' or 'sell'");
        }

        // 주문 방식 확인
        if request.order_side != "limit" && request.order_side != "market" {
            bail!("Invalid order_side: must be 'limit' or 'market'");
        }

        // 지정가 주문은 가격 필수
        if request.order_side == "limit" && request.price.is_none() {
            bail!("Limit order must have price");
        }

        // 시장가 주문은 가격 없음
        if request.order_side == "market" && request.price.is_some() {
            bail!("Market order cannot have price");
        }

        // 매도 주문: amount 필수
        if request.order_type == "sell" {
            if request.amount.is_none() {
                bail!("Sell orders must have amount");
            }
            if let Some(amount) = request.amount {
                if amount <= Decimal::ZERO {
                    bail!("Amount must be positive");
                }
            }
            if request.quote_amount.is_some() {
                bail!("Sell orders cannot use quote_amount");
            }
        }

        // 지정가 매수: amount + price 필수
        if request.order_type == "buy" && request.order_side == "limit" {
            if request.amount.is_none() {
                bail!("Limit buy orders must have amount");
            }
            if let Some(amount) = request.amount {
                if amount <= Decimal::ZERO {
                    bail!("Amount must be positive");
                }
            }
            if request.quote_amount.is_some() {
                bail!("Limit buy orders cannot use quote_amount");
            }
        }

        // 시장가 매수: quote_amount만 필수 (금액 기반만 지원)
        if request.order_type == "buy" && request.order_side == "market" {
            if request.quote_amount.is_none() {
                bail!("Market buy orders must have quote_amount (amount-based market buy is not supported)");
            }
            if request.amount.is_some() {
                bail!("Market buy orders cannot use amount, use quote_amount instead");
            }
            if let Some(quote_amount) = request.quote_amount {
                if quote_amount <= Decimal::ZERO {
                    bail!("Quote amount must be positive");
                }
            }
        }

        // 가격은 양수여야 함 (지정가인 경우)
        if let Some(price) = request.price {
            if price <= Decimal::ZERO {
                bail!("Price must be positive");
            }
        }

        Ok(())
    }

    /// 필요한 잔고 계산
    /// Calculate required balance
    /// 
    /// # Returns
    /// * `(자산 종류, 필요 수량)`
    /// * 매수: (quote_mint, price * amount) - USDT 필요
    /// * 매도: (base_mint, amount) - SOL 등 필요
    fn calculate_required_balance(
        &self,
        request: &CreateOrderRequest,
    ) -> Result<(String, Decimal)> {
        match request.order_type.as_str() {
            "buy" => {
                let quote_mint = request.quote_mint.clone().unwrap_or_else(|| "USDT".to_string());
                
                if request.order_side == "market" {
                    // 시장가 매수: 금액 기반만 지원 (quote_amount 필수)
                    let quote_amount = request.quote_amount.unwrap();
                    Ok((quote_mint, quote_amount))
                } else {
                    // 지정가 매수: price * amount
                    let price = request.price.unwrap();
                    let amount = request.amount.unwrap();
                    let required_amount = price * amount;
                    Ok((quote_mint, required_amount))
                }
            }
            "sell" => {
                // 매도: base_mint 필요 (항상 수량 기반)
                let amount = request.amount.unwrap();
                Ok((request.base_mint.clone(), amount))
            }
            _ => bail!("Invalid order_type"),
        }
    }

    /// 잔고 확인
    /// Check balance
    async fn check_balance(
        &self,
        user_id: u64,
        mint: &str,
        required_amount: Decimal,
    ) -> Result<()> {
        // 엔진에서 잔고 조회
        let (available, _locked) = {
            let engine_guard = self.engine.lock().await;
            engine_guard
                .get_balance(user_id, mint)
                .await
                .context("Failed to get balance from engine")?
        };

        // 잔고 부족 체크
        if available < required_amount {
            bail!(
                "Insufficient balance: required {}, but only {} available",
                required_amount,
                available
            );
        }

        Ok(())
    }
}

