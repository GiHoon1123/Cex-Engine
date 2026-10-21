use crate::shared::database::{Database, OrderRepository};
use crate::domains::cex::models::order::{Order, CreateOrderRequest};
use crate::domains::cex::engine::{Engine, TradingPair, OrderEntry, entry_to_order, runtime::EngineHandle};
use anyhow::{Context, Result, bail};
use rust_decimal::Decimal;
use chrono::Utc;
use super::order_receipt::{self, AcceptanceResult};

/// 플랫폼 원본과 엔진 영속 접수·실행 큐의 경계를 연결한다.
///
/// 생성은 DB 접수 커밋 뒤 최초 요청만 큐로 전달한다. 여기서 반환하는 접수 증명은
/// 체결 완료·주문장 복구 가능성을 뜻하지 않는다. 취소·조회는 기존 메서드가 담당한다.
/// 명령 핸들만 복제한다. 요청 전체를 잠그거나 거래 상태를 공유하지 않는다.
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
    engine: EngineHandle,
}

impl OrderService {
    /// 공유 DB 풀과 실행 엔진을 빌려 쓴다. 서비스 복제 자체는 접수 기록을 복제하지 않는다.
    pub fn new(db: Database, engine: EngineHandle) -> Self {
        Self { db, engine }
    }

    /// 플랫폼 원본과 같은 명령을 먼저 영속 접수하고, 최초 접수만 실행 큐로 보낸다.
    /// 반환은 체결 완료가 아니다. 커밋 후 큐 전달 전 종료 가능성을 감추지 않는다.
    pub async fn create_order(
        &self, user_id: u64, request: CreateOrderRequest,
    ) -> Result<AcceptanceResult> {
        anyhow::ensure!(user_id == request.user_id, super::order_receipt::ReceiptError::Invalid);
        self.validate_order_request(&request)
            .map_err(|_| super::order_receipt::ReceiptError::Invalid)?;
        let accepted = order_receipt::accept(self.db.pool(), &request).await?;
        if !accepted.created {
            // 잔액·주문장이 이미 바뀌었어도 접수 원본은 유지된다. 재전송을 실행하지 않는다.
            return Ok(accepted);
        }
        let market_buy = request.order_type == "buy" && request.order_side == "market";
        let amount = if market_buy { Decimal::ZERO } else { request.amount.unwrap() };
        let entry = OrderEntry {
            id: request.order_id.unwrap(), user_id,
            order_type: request.order_type, order_side: request.order_side,
            base_mint: request.base_mint,
            quote_mint: request.quote_mint.unwrap_or_else(|| "USDT".into()),
            price: request.price, amount, quote_amount: request.quote_amount,
            filled_amount: Decimal::ZERO, remaining_amount: amount,
            remaining_quote_amount: if market_buy { request.quote_amount } else { None },
            created_at: accepted.receipt.accepted_at,
        };
        // 분리된 tokio::spawn은 제거한다. 이 HTTP 작업이 영속 접수의 삽입 승자이며
        // 큐 전달을 한 번 시도한다. 실패해도 접수 기록을 삭제하거나 거짓 거부로 바꾸지 않는다.
        if let Err(error) = self.engine.submit_order(entry).await {
            eprintln!("[Order] 접수 저장 후 큐 전달 실패: order_id={}, error={}", accepted.receipt.order_id, error);
        }
        Ok(accepted)
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
            self.engine
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

}
