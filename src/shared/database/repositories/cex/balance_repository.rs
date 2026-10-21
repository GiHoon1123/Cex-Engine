use sqlx::{PgPool, Row};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use crate::domains::cex::models::balance::{UserBalance, UserBalanceCreate, UserBalanceUpdate};

pub struct UserBalanceRepository {
    pool: PgPool,
}

impl UserBalanceRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// 잔고 생성 (또는 기존 잔고 조회)
    /// Create balance (or get existing balance)
    pub async fn create_or_get(
        &self,
        balance_create: &UserBalanceCreate,
    ) -> Result<UserBalance> {
        let row = sqlx::query(
            r#"
            INSERT INTO user_balances (user_id, mint_address, available, locked, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (user_id, mint_address) 
            DO UPDATE SET updated_at = $6
            RETURNING id, user_id, mint_address, available, locked, created_at, updated_at
            "#,
        )
        .bind(balance_create.user_id as i64)
        .bind(&balance_create.mint_address)
        .bind(&balance_create.available)
        .bind(&balance_create.locked)
        .bind(Utc::now())
        .bind(Utc::now())
        .fetch_one(&self.pool)
        .await
        .context("Failed to create or get balance")?;

        Ok(UserBalance {
            id: row.get::<i64, _>("id") as u64,
            user_id: row.get::<i64, _>("user_id") as u64,
            mint_address: row.get("mint_address"),
            available: row.get("available"),
            locked: row.get("locked"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
    }

    /// 사용자 ID와 자산으로 잔고 조회
    /// Get balance by user ID and mint address
    pub async fn get_by_user_and_mint(
        &self,
        user_id: u64,
        mint_address: &str,
    ) -> Result<Option<UserBalance>> {
        let row = sqlx::query(
            r#"
            SELECT id, user_id, mint_address, available, locked, created_at, updated_at
            FROM user_balances
            WHERE user_id = $1 AND mint_address = $2
            "#,
        )
        .bind(user_id as i64)
        .bind(mint_address)
        .fetch_optional(&self.pool)
        .await
        .context("Failed to fetch balance by user and mint")?;

        let row = match row {
            Some(r) => r,
            None => return Ok(None),
        };

        Ok(Some(UserBalance {
            id: row.get::<i64, _>("id") as u64,
            user_id: row.get::<i64, _>("user_id") as u64,
            mint_address: row.get("mint_address"),
            available: row.get("available"),
            locked: row.get("locked"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        }))
    }

    /// 사용자 ID로 모든 잔고 조회
    /// Get all balances by user ID
    pub async fn get_all_by_user(&self, user_id: u64) -> Result<Vec<UserBalance>> {
        let rows = sqlx::query(
            r#"
            SELECT id, user_id, mint_address, available, locked, created_at, updated_at
            FROM user_balances
            WHERE user_id = $1
            ORDER BY mint_address ASC
            "#,
        )
        .bind(user_id as i64)
        .fetch_all(&self.pool)
        .await
        .context("Failed to fetch all balances by user")?;

        Ok(rows
            .into_iter()
            .map(|row| UserBalance {
                id: row.get::<i64, _>("id") as u64,
                user_id: row.get::<i64, _>("user_id") as u64,
                mint_address: row.get("mint_address"),
                available: row.get("available"),
                locked: row.get("locked"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            })
            .collect())
    }

    /// 잔고 업데이트 (available/locked 증감)
    /// Update balance (increment/decrement available/locked)
    pub async fn update_balance(
        &self,
        user_id: u64,
        mint_address: &str,
        update: &UserBalanceUpdate,
    ) -> Result<UserBalance> {
        // 업데이트할 값이 없으면 기존 잔고 조회만
        if update.available_delta.is_none() && update.locked_delta.is_none() {
            return self
                .get_by_user_and_mint(user_id, mint_address)
                .await
                .and_then(|opt| opt.context("Balance not found"));
        }

        // 쿼리 실행
        let row = if let (Some(available_delta), Some(locked_delta)) = (&update.available_delta, &update.locked_delta) {
            sqlx::query(
                r#"
                UPDATE user_balances
                SET 
                    available = available + $1,
                    locked = locked + $2,
                    updated_at = $3
                WHERE user_id = $4 AND mint_address = $5
                RETURNING id, user_id, mint_address, available, locked, created_at, updated_at
                "#,
            )
            .bind(available_delta)
            .bind(locked_delta)
            .bind(Utc::now())
            .bind(user_id as i64)
            .bind(mint_address)
            .fetch_one(&self.pool)
            .await
            .context("Failed to update balance")?
        } else if let Some(available_delta) = &update.available_delta {
            // INSERT ... ON CONFLICT로 레코드가 없으면 생성, 있으면 업데이트
            sqlx::query(
                r#"
                INSERT INTO user_balances (user_id, mint_address, available, locked, created_at, updated_at)
                VALUES ($1, $2, $3, 0, $4, $4)
                ON CONFLICT (user_id, mint_address) 
                DO UPDATE SET 
                    available = user_balances.available + $3,
                    updated_at = $4
                RETURNING id, user_id, mint_address, available, locked, created_at, updated_at
                "#,
            )
            .bind(user_id as i64)
            .bind(mint_address)
            .bind(available_delta)
            .bind(Utc::now())
            .fetch_one(&self.pool)
            .await
            .context("Failed to update balance")?
        } else if let Some(locked_delta) = &update.locked_delta {
            sqlx::query(
                r#"
                UPDATE user_balances
                SET 
                    locked = locked + $1,
                    updated_at = $2
                WHERE user_id = $3 AND mint_address = $4
                RETURNING id, user_id, mint_address, available, locked, created_at, updated_at
                "#,
            )
            .bind(locked_delta)
            .bind(Utc::now())
            .bind(user_id as i64)
            .bind(mint_address)
            .fetch_one(&self.pool)
            .await
            .context("Failed to update balance")?
        } else {
            return self
                .get_by_user_and_mint(user_id, mint_address)
                .await
                .and_then(|opt| {
                    opt.context("Balance not found")
                });
        };

        Ok(UserBalance {
            id: row.get::<i64, _>("id") as u64,
            user_id: row.get::<i64, _>("user_id") as u64,
            mint_address: row.get("mint_address"),
            available: row.get("available"),
            locked: row.get("locked"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
    }

    /// 잔고가 충분한지 확인 (available >= required)
    /// Check if balance is sufficient (available >= required)
    pub async fn check_sufficient_balance(
        &self,
        user_id: u64,
        mint_address: &str,
        required: Decimal,
    ) -> Result<bool> {
        let row = sqlx::query(
            r#"
            SELECT available >= $1 as sufficient
            FROM user_balances
            WHERE user_id = $2 AND mint_address = $3
            "#,
        )
        .bind(required)
        .bind(user_id as i64)
        .bind(mint_address)
        .fetch_optional(&self.pool)
        .await
        .context("Failed to check balance sufficiency")?;

        Ok(row.map(|r| r.get("sufficient")).unwrap_or(false))
    }

}

