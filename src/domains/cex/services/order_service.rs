use super::order_receipt::{self, AcceptanceResult};
use crate::domains::cex::engine::{
    entry_to_order, runtime::EngineHandle, Engine, OrderEntry, TradingPair,
};
use crate::domains::cex::models::order::{CreateOrderRequest, Order};
use crate::shared::database::Database;
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
            fee_rate: accepted.fee_rate,
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
    /// let cancelled = service.cancel_order(user_id, order_id, &pair).await?;
    /// println!("주문 취소: {}", cancelled.id);
    /// ```
    pub async fn cancel_order(
        &self,
        user_id: u64,
        order_id: u64,
        pair: &TradingPair,
    ) -> Result<Order> {
        // 현재 오더북·취소 이력과 소유자는 코어가 검증한다. 플랫폼의 비동기 반영
        // 상태를 다시 읽으면 앞선 체결 결과보다 오래된 행을 권위로 삼게 된다.
        // 쿼리의 거래쌍은 탐색 키이며, 잘못된 키나 소유자는 코어에서 거부한다.
        let cancelled = self
            .engine
            .cancel_order(order_id, user_id, pair)
            .await
            .context("Failed to cancel order in engine")?;
        let mut result = entry_to_order(&cancelled);
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

#[cfg(test)]
mod cancellation_boundary_tests {
    use super::*;
    use crate::domains::cex::engine::runtime::HighPerformanceEngine;
    /// 실제 코어 루프를 사용하되 접속할 수 없는 디비를 주입한다. 취소가 플랫폼
    /// 조회 상태나 시간 타입에 의존하지 않는지, 본인 확인과 중복 처리는 유지되는지 확인한다.
    #[tokio::test]
    async fn cancellation_uses_owned_core_without_platform_database() -> Result<()> {
        let mut engine = HighPerformanceEngine::new_bench();
        engine.bench_set_balance(1, "BTC", Decimal::ONE, Decimal::ZERO);
        engine.start().await?;
        let handle = engine.handle();
        handle
            .submit_order(OrderEntry {
                fee_rate: rust_decimal::Decimal::ZERO,
                id: 1,
                user_id: 1,
                order_type: "sell".into(),
                order_side: "limit".into(),
                base_mint: "BTC".into(),
                quote_mint: "USDT".into(),
                price: Some(Decimal::from(60000)),
                amount: Decimal::ONE,
                quote_amount: None,
                filled_amount: Decimal::ZERO,
                remaining_amount: Decimal::ONE,
                remaining_quote_amount: None,
                created_at: Utc::now(),
            })
            .await?;
        let service = OrderService::new(
            Database::lazy("postgres://root:1234@127.0.0.1:1/cex")?,
            handle.clone(),
        );
        let pair = TradingPair::new("BTC".into(), "USDT".into());
        assert!(service.cancel_order(2, 1, &pair).await.is_err());
        assert!(service
            .cancel_order(1, 1, &TradingPair::new("ETH".into(), "USDT".into()))
            .await
            .is_err());
        assert_eq!(handle.get_orderbook(&pair, None).await?.1.len(), 1);
        assert_eq!(service.cancel_order(1, 1, &pair).await?.status, "cancelled");
        assert_eq!(service.cancel_order(1, 1, &pair).await?.status, "cancelled");
        assert!(handle.get_orderbook(&pair, None).await?.1.is_empty());
        assert_eq!(
            handle.get_balance(1, "BTC").await?,
            (Decimal::ONE, Decimal::ZERO)
        );
        engine.stop().await?;
        Ok(())
    }
}
