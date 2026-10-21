use super::order_receipt::{self, AcceptanceResult};
use crate::domains::cex::engine::{
    entry_to_order, runtime::EngineHandle, Engine, OrderEntry, TradingPair,
};
use crate::domains::cex::models::order::{CreateOrderRequest, Order};
use crate::shared::database::{Database, OrderRepository};
use anyhow::{bail, Context, Result};
use chrono::Utc;
use rust_decimal::Decimal;

/// 플랫폼 주문 원본을 검증하고, 엔진 자체 저널의 영속 확인을 기다린다.
/// 디비 접수만 남고 실행 큐 전달이 실패한 경우에는 성공으로 응답하지 않는다.
/// 같은 원본 재전송은 코어가 복원한 주문 식별 인덱스로 중복 실행을 막는다.
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

    /// 원본 검증 뒤 저널 확인을 기다린다. 기존 접수도 재확인하며 체결 완료와 구분한다.
    pub async fn create_order(
        &self,
        user_id: u64,
        request: CreateOrderRequest,
    ) -> Result<AcceptanceResult> {
        anyhow::ensure!(
            user_id == request.user_id,
            super::order_receipt::ReceiptError::Invalid
        );
        self.validate_order_request(&request)
            .map_err(|_| super::order_receipt::ReceiptError::Invalid)?;
        let accepted = order_receipt::accept(self.db.pool(), &request).await?;
        let market_buy = request.order_type == "buy" && request.order_side == "market";
        let amount = if market_buy {
            Decimal::ZERO
        } else {
            request.amount.unwrap()
        };
        let entry = OrderEntry {
            id: request.order_id.unwrap(),
            user_id,
            order_type: request.order_type,
            order_side: request.order_side,
            base_mint: request.base_mint,
            quote_mint: request.quote_mint.unwrap_or_else(|| "USDT".into()),
            price: request.price,
            amount,
            quote_amount: request.quote_amount,
            filled_amount: Decimal::ZERO,
            remaining_amount: amount,
            remaining_quote_amount: if market_buy {
                request.quote_amount
            } else {
                None
            },
            created_at: accepted.receipt.accepted_at,
        };
        // 기존 DB 접수 여부와 무관하게 같은 명령을 전달한다. 코어의 저널 복원
        // 인덱스가 중복 실행을 막고, 저널 커밋이 확인돼야 HTTP 성공을 반환한다.
        self.engine.submit_order(entry).await?;
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
    /// 5. 실행 결과 반환, 플랫폼 상태 반영은 결과 소비자의 책임
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
    pub async fn cancel_order(&self, user_id: u64, order_id: u64) -> Result<Order> {
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
        let trading_pair = TradingPair::new(order.base_mint.clone(), order.quote_mint.clone());

        let cancelled_entry = {
            self.engine
                .cancel_order(order_id, user_id, &trading_pair)
                .await
                .context("Failed to cancel order in engine")?
        };

        // 엔진은 실행 결과만 반환한다. 플랫폼 주문 상태를 여기서 먼저
        // 변경하면 취소 소비자가 중복으로 오인해 예약 해제를 건너뛸 수 있다.
        let mut result = entry_to_order(&cancelled_entry);
        result.status = "cancelled".into();
        Ok(result)
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
