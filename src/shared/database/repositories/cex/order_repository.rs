use sqlx::{PgPool, Row};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use crate::domains::cex::models::order::{Order, OrderCreate};
use crate::shared::database::exact_decimal::{read_decimal, read_optional_decimal};

pub struct OrderRepository {
    pool: PgPool,
}

impl OrderRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// 주문 생성
    /// Create order
    pub async fn create(&self, order_create: &OrderCreate) -> Result<Order> {
        let row = sqlx::query(
            r#"
            INSERT INTO orders (
                user_id, order_type, order_side, base_mint, quote_mint,
                price, amount, filled_amount, filled_quote_amount, status, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            RETURNING id, user_id, order_type, order_side, base_mint, quote_mint,
                      price::text AS exact_price, amount::text AS exact_amount, filled_amount::text AS exact_filled_amount, filled_quote_amount::text AS exact_filled_quote_amount, status, created_at, updated_at
            "#,
        )
        .bind(order_create.user_id as i64)
        .bind(&order_create.order_type)
        .bind(&order_create.order_side)
        .bind(&order_create.base_mint)
        .bind(&order_create.quote_mint)
        .bind(&order_create.price)
        .bind(&order_create.amount)
        .bind(Decimal::ZERO) // filled_amount 초기값은 0
        .bind(Decimal::ZERO) // filled_quote_amount 초기값은 0
        .bind("pending") // status 초기값은 pending
        .bind(Utc::now())
        .bind(Utc::now())
        .fetch_one(&self.pool)
        .await
        .context("Failed to create order")?;

        self.row_to_order(&row)
    }

    /// 주문 ID로 조회
    /// Get order by ID
    pub async fn get_by_id(&self, order_id: u64) -> Result<Option<Order>> {
        let row = sqlx::query(
            r#"
            SELECT id, user_id, order_type, order_side, base_mint, quote_mint,
                   price::text AS exact_price, amount::text AS exact_amount, filled_amount::text AS exact_filled_amount, filled_quote_amount::text AS exact_filled_quote_amount, status, created_at, updated_at
            FROM orders
            WHERE id = $1
            "#,
        )
        .bind(order_id as i64)
        .fetch_optional(&self.pool)
        .await
        .context("Failed to fetch order by id")?;

        row.map(|r| self.row_to_order(&r)).transpose()
    }

    /// 사용자 ID로 모든 주문 조회
    /// Get all orders by user ID
    pub async fn get_all_by_user(
        &self,
        user_id: u64,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<Order>> {
        let limit = limit.unwrap_or(100);
        let offset = offset.unwrap_or(0);

        let rows = sqlx::query(
            r#"
            SELECT id, user_id, order_type, order_side, base_mint, quote_mint,
                   price::text AS exact_price, amount::text AS exact_amount, filled_amount::text AS exact_filled_amount, filled_quote_amount::text AS exact_filled_quote_amount, status, created_at, updated_at
            FROM orders
            WHERE user_id = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(user_id as i64)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
        .context("Failed to fetch orders by user")?;

        rows.iter().map(|r| self.row_to_order(r)).collect()
    }

    /// 사용자 ID와 상태로 주문 조회
    /// Get orders by user ID and status
    pub async fn get_by_user_and_status(
        &self,
        user_id: u64,
        status: &str,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<Order>> {
        let limit = limit.unwrap_or(100);
        let offset = offset.unwrap_or(0);

        let rows = sqlx::query(
            r#"
            SELECT id, user_id, order_type, order_side, base_mint, quote_mint,
                   price::text AS exact_price, amount::text AS exact_amount, filled_amount::text AS exact_filled_amount, filled_quote_amount::text AS exact_filled_quote_amount, status, created_at, updated_at
            FROM orders
            WHERE user_id = $1 AND status = $2
            ORDER BY created_at DESC
            LIMIT $3 OFFSET $4
            "#,
        )
        .bind(user_id as i64)
        .bind(status)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
        .context("Failed to fetch orders by user and status")?;

        rows.iter().map(|r| self.row_to_order(r)).collect()
    }

    /// 오더북 조회 (거래쌍별, 상태별, 주문 타입별, 가격순)
    /// Get orderbook (by trading pair, status, order type, sorted by price)
    pub async fn get_orderbook(
        &self,
        base_mint: &str,
        quote_mint: &str,
        limit: Option<i64>,
    ) -> Result<(Vec<Order>, Vec<Order>)> {
        let limit = limit.unwrap_or(50);

        // 매수 주문: 가격 내림차순 (높은 가격 우선)
        let buy_orders = sqlx::query(
            r#"
            SELECT id, user_id, order_type, order_side, base_mint, quote_mint,
                   price::text AS exact_price, amount::text AS exact_amount, filled_amount::text AS exact_filled_amount, filled_quote_amount::text AS exact_filled_quote_amount, status, created_at, updated_at
            FROM orders
            WHERE base_mint = $1 
              AND quote_mint = $2 
              AND order_type = 'buy'
              AND status IN ('pending', 'partial')
              AND price IS NOT NULL
            ORDER BY price DESC, created_at ASC
            LIMIT $3
            "#,
        )
        .bind(base_mint)
        .bind(quote_mint)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .context("Failed to fetch buy orders for orderbook")?;

        // 매도 주문: 가격 오름차순 (낮은 가격 우선)
        let sell_orders = sqlx::query(
            r#"
            SELECT id, user_id, order_type, order_side, base_mint, quote_mint,
                   price::text AS exact_price, amount::text AS exact_amount, filled_amount::text AS exact_filled_amount, filled_quote_amount::text AS exact_filled_quote_amount, status, created_at, updated_at
            FROM orders
            WHERE base_mint = $1 
              AND quote_mint = $2 
              AND order_type = 'sell'
              AND status IN ('pending', 'partial')
              AND price IS NOT NULL
            ORDER BY price ASC, created_at ASC
            LIMIT $3
            "#,
        )
        .bind(base_mint)
        .bind(quote_mint)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .context("Failed to fetch sell orders for orderbook")?;

        Ok((
            buy_orders.iter().map(|r| self.row_to_order(r)).collect::<Result<Vec<_>>>()?,
            sell_orders.iter().map(|r| self.row_to_order(r)).collect::<Result<Vec<_>>>()?,
        ))
    }

    /// 특정 거래쌍의 활성 주문 조회 (매칭 엔진용)
    /// Get active orders for a trading pair (for matching engine)
    pub async fn get_active_orders(
        &self,
        base_mint: &str,
        quote_mint: &str,
        order_type: Option<&str>,
    ) -> Result<Vec<Order>> {
        let query = if let Some(ot) = order_type {
            sqlx::query(
                r#"
                SELECT id, user_id, order_type, order_side, base_mint, quote_mint,
                       price::text AS exact_price, amount::text AS exact_amount, filled_amount::text AS exact_filled_amount, filled_quote_amount::text AS exact_filled_quote_amount, status, created_at, updated_at
                FROM orders
                WHERE base_mint = $1 
                  AND quote_mint = $2 
                  AND order_type = $3
                  AND status IN ('pending', 'partial')
                ORDER BY 
                  CASE WHEN order_type = 'buy' THEN price END DESC,
                  CASE WHEN order_type = 'sell' THEN price END ASC,
                  created_at ASC
                "#,
            )
            .bind(base_mint)
            .bind(quote_mint)
            .bind(ot)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
                r#"
                SELECT id, user_id, order_type, order_side, base_mint, quote_mint,
                       price::text AS exact_price, amount::text AS exact_amount, filled_amount::text AS exact_filled_amount, filled_quote_amount::text AS exact_filled_quote_amount, status, created_at, updated_at
                FROM orders
                WHERE base_mint = $1 
                  AND quote_mint = $2 
                  AND status IN ('pending', 'partial')
                ORDER BY 
                  CASE WHEN order_type = 'buy' THEN price END DESC,
                  CASE WHEN order_type = 'sell' THEN price END ASC,
                  created_at ASC
                "#,
            )
            .bind(base_mint)
            .bind(quote_mint)
            .fetch_all(&self.pool)
            .await
        }
        .context("Failed to fetch active orders")?;

        query.iter().map(|r| self.row_to_order(r)).collect()
    }

    /// 주문 상태 업데이트
    /// Update order status
    pub async fn update_status(
        &self,
        order_id: u64,
        status: &str,
    ) -> Result<Order> {
        let row = sqlx::query(
            r#"
            UPDATE orders
            SET status = $1, updated_at = $2
            WHERE id = $3
            RETURNING id, user_id, order_type, order_side, base_mint, quote_mint,
                      price::text AS exact_price, amount::text AS exact_amount, filled_amount::text AS exact_filled_amount, filled_quote_amount::text AS exact_filled_quote_amount, status, created_at, updated_at
            "#,
        )
        .bind(status)
        .bind(Utc::now())
        .bind(order_id as i64)
        .fetch_one(&self.pool)
        .await
        .context("Failed to update order status")?;

        self.row_to_order(&row)
    }

    /// 주문 체결량 업데이트
    /// Update order filled amount
    pub async fn update_filled_amount(
        &self,
        order_id: u64,
        filled_amount: Decimal,
    ) -> Result<Order> {
        // 체결량이 주문량과 같거나 크면 상태를 'filled'로 변경
        let row = sqlx::query(
            r#"
            UPDATE orders
            SET 
                filled_amount = $1,
                status = CASE 
                    WHEN $1 >= amount THEN 'filled'
                    WHEN $1 > 0 THEN 'partial'
                    ELSE status
                END,
                updated_at = $2
            WHERE id = $3
            RETURNING id, user_id, order_type, order_side, base_mint, quote_mint,
                      price::text AS exact_price, amount::text AS exact_amount, filled_amount::text AS exact_filled_amount, filled_quote_amount::text AS exact_filled_quote_amount, status, created_at, updated_at
            "#,
        )
        .bind(filled_amount)
        .bind(Utc::now())
        .bind(order_id as i64)
        .fetch_one(&self.pool)
        .await
        .context("Failed to update order filled amount")?;

        self.row_to_order(&row)
    }

    /// 주문 취소
    /// Cancel order
    pub async fn cancel_order(&self, order_id: u64) -> Result<Order> {
        let row = sqlx::query(
            r#"
            UPDATE orders
            SET status = 'cancelled', updated_at = $1
            WHERE id = $2 AND status IN ('pending', 'partial')
            RETURNING id, user_id, order_type, order_side, base_mint, quote_mint,
                      price::text AS exact_price, amount::text AS exact_amount, filled_amount::text AS exact_filled_amount, filled_quote_amount::text AS exact_filled_quote_amount, status, created_at, updated_at
            "#,
        )
        .bind(Utc::now())
        .bind(order_id as i64)
        .fetch_one(&self.pool)
        .await
        .context("Failed to cancel order. Order may not exist or already be filled/cancelled")?;

        self.row_to_order(&row)
    }

    /// 모든 활성 주문 조회 (엔진 시작 시 사용)
    /// Get all active orders (used when engine starts)
    /// 
    /// # 사용 목적
    /// 서버 재시작 시 모든 활성 주문을 메모리(OrderBook)에 로드
    /// 
    /// # 반환값
    /// 모든 거래쌍의 활성 주문 목록 (status IN ('pending', 'partial'))
    pub async fn get_all_active_orders(&self) -> Result<Vec<Order>> {
        let rows = sqlx::query(
            r#"
            SELECT id, user_id, order_type, order_side, base_mint, quote_mint,
                   price::text AS exact_price, amount::text AS exact_amount, filled_amount::text AS exact_filled_amount, filled_quote_amount::text AS exact_filled_quote_amount, status, created_at, updated_at
            FROM orders
            WHERE status IN ('pending', 'partial')
            ORDER BY base_mint, quote_mint,
                     CASE WHEN order_type = 'buy' THEN price END DESC NULLS LAST,
                     CASE WHEN order_type = 'sell' THEN price END ASC NULLS LAST,
                     created_at ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .context("Failed to fetch all active orders")?;

        rows.iter().map(|r| self.row_to_order(r)).collect()
    }

    /// Row를 Order로 변환하는 헬퍼 메서드
    /// Helper method to convert Row to Order
    // 조회·취소 경로도 접수와 같은 정확한 변환을 사용한다. 별칭 exact_price는
    // ORDER BY price가 text 정렬로 바뀌는 것을 막는다. row.get은 해독 실패 때
    // panic하므로 try_get과 Result를 통해 HTTP 서비스까지 오류를 전달한다.
    fn row_to_order(&self, row: &sqlx::postgres::PgRow) -> Result<Order> {
        Ok(Order {
            id: row.try_get::<i64, _>("id")? as u64,
            user_id: row.try_get::<i64, _>("user_id")? as u64,
            order_type: row.try_get("order_type")?,
            order_side: row.try_get("order_side")?,
            base_mint: row.try_get("base_mint")?,
            quote_mint: row.try_get("quote_mint")?,
            price: read_optional_decimal(row, "exact_price")?,
            amount: read_decimal(row, "exact_amount")?,
            filled_amount: read_decimal(row, "exact_filled_amount")?,
            filled_quote_amount: read_decimal(row, "exact_filled_quote_amount")?,
            status: row.try_get("status")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}


#[cfg(test)]
mod exact_amount_tests;
