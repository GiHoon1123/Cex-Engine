//! 플랫폼 원본과 비교한 접수 증명을 보관한다. 이 디비 기록만으로 엔진의
//! 실행·복구를 확정하지 않는다. 호출자는 최초·중복 모두 엔진 저널 확인을
//! 기다려야 하며, 기록 뒤 큐 전달 실패를 접수 성공으로 숨기지 않는다.

use crate::shared::database::exact_decimal::read_optional_decimal;
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use sqlx::{postgres::PgRow, FromRow, PgPool, Row};
use utoipa::ToSchema;

use crate::domains::cex::models::order::CreateOrderRequest;

#[derive(Debug, thiserror::Error)]
pub enum ReceiptError {
    #[error("Invalid order submission")]
    Invalid,
    #[error("Order id is already bound to another command")]
    Conflict,
}

/// ID를 문자열로 반환한다. receipt_status는 체결 상태가 아닌 영속 접수의 존재다.
#[derive(Debug, Serialize, ToSchema)]
pub struct OrderAcceptance {
    pub order_id: String,
    pub user_id: String,
    pub receipt_status: String,
    pub accepted_at: DateTime<Utc>,
}

pub struct AcceptanceResult {
    pub receipt: OrderAcceptance,
    pub created: bool,
    pub fee_rate: Decimal,
}

struct StoredCommand {
    fee_rate: Decimal,
    order_id: i64,
    user_id: i64,
    order_type: String,
    order_side: String,
    base_mint: String,
    quote_mint: String,
    price: Option<Decimal>,
    amount: Option<Decimal>,
    quote_amount: Option<Decimal>,
    accepted_at: DateTime<Utc>,
}

// NUMERIC을 직접 Decimal로 decode하지 않는다. SELECT/RETURNING의 text 컬럼을
// 검사해 변환하고, DB에는 저장돼도 Rust에 정확히 표현되지 않는 원본은 접수하지 않는다.
impl<'r> FromRow<'r, PgRow> for StoredCommand {
    fn from_row(row: &'r PgRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(Self {
            fee_rate: read_optional_decimal(row, "fee_rate")?
                .ok_or_else(|| sqlx::Error::ColumnNotFound("fee_rate".into()))?,
            order_id: row.try_get("order_id")?,
            user_id: row.try_get("user_id")?,
            order_type: row.try_get("order_type")?,
            order_side: row.try_get("order_side")?,
            base_mint: row.try_get("base_mint")?,
            quote_mint: row.try_get("quote_mint")?,
            price: read_optional_decimal(row, "price")?,
            amount: read_optional_decimal(row, "amount")?,
            quote_amount: read_optional_decimal(row, "quote_amount")?,
            accepted_at: row.try_get("accepted_at")?,
        })
    }
}

impl StoredCommand {
    fn matches(&self, request: &CreateOrderRequest) -> bool {
        // Decimal의 수치 비교는 소수 scale이 달라도 값이 같으면 같다.
        Some(self.order_id as u64) == request.order_id
            && self.user_id as u64 == request.user_id
            && self.order_type == request.order_type
            && self.order_side == request.order_side
            && self.base_mint == request.base_mint
            && self.quote_mint == request.quote_mint.as_deref().unwrap_or("USDT")
            && self.price == request.price
            && self.amount == request.amount
            && self.quote_amount == request.quote_amount
    }

    fn acceptance(&self) -> OrderAcceptance {
        OrderAcceptance {
            order_id: self.order_id.to_string(),
            user_id: self.user_id.to_string(),
            receipt_status: "accepted".into(),
            accepted_at: self.accepted_at,
        }
    }
}

/// 트랜잭션 커밋이 성공해야 created=true를 돌려준다. DB 오류는 접수 성공으로 바꾸지 않는다.
pub async fn accept(pool: &PgPool, request: &CreateOrderRequest) -> Result<AcceptanceResult> {
    let order_id = request
        .order_id
        .and_then(|id| i64::try_from(id).ok())
        .filter(|id| *id > 0)
        .ok_or(ReceiptError::Invalid)?;
    let user_id = i64::try_from(request.user_id)
        .ok()
        .filter(|id| *id > 0)
        .ok_or(ReceiptError::Invalid)?;
    let mut transaction = pool.begin().await.context("Begin engine acceptance")?;
    // 충돌 뒤 새 SELECT에서 승자의 커밋을 읽는 계약을 세션 기본값에 맡기지 않는다.
    sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .execute(&mut *transaction)
        .await?;
    // 비동기 커밋 설정에서는 응답 뒤 DB 프로세스 장애로 기록을 잃을 수 있다.
    // 이 접수 트랜잭션은 WAL 로컬 디스크 반영을 기다린다. 복제·백업 보장은 별도다.
    sqlx::query("SET LOCAL synchronous_commit = on")
        .execute(&mut *transaction)
        .await?;
    let existing =
        sqlx::query_as::<_, StoredCommand>("SELECT order_id,user_id,order_type,order_side,base_mint,quote_mint,price::text AS price,amount::text AS amount,quote_amount::text AS quote_amount,fee_rate::text AS fee_rate,accepted_at FROM engine_order_receipts WHERE order_id=$1")
            .bind(order_id)
            .fetch_optional(&mut *transaction)
            .await?;
    if let Some(existing) = existing {
        ensure!(existing.matches(request), ReceiptError::Conflict);
        transaction.commit().await?;
        return Ok(AcceptanceResult {
            fee_rate: existing.fee_rate,
            receipt: existing.acceptance(),
            created: false,
        });
    }

    // 원본을 읽는 동안 직접 SQL 갱신과도 경합하지 않게 공유 락을 잡는다.
    // 현재 상태 테이블의 수량으로 시장가 매수 예산을 재구성하지 않는다.
    let original = sqlx::query_as::<_, StoredCommand>(
        "SELECT s.order_id,s.user_id,s.order_type,s.order_side,s.base_mint,s.quote_mint,
                s.price::text AS price,s.amount::text AS amount,s.quote_amount::text AS quote_amount,s.fee_rate::text AS fee_rate,now() AS accepted_at
         FROM order_submissions s JOIN orders o ON o.id=s.order_id
         WHERE s.order_id=$1 AND s.user_id=$2 AND o.user_id=$2
           AND o.status='pending' AND o.filled_amount=0
         FOR SHARE OF s, o",
    )
    .bind(order_id)
    .bind(user_id)
    .fetch_optional(&mut *transaction)
    .await?;
    let original = match original {
        Some(original) => original,
        None => {
            // 첫 조회 직후 경쟁 요청이 접수·처리해 주문을 종료했을 수 있다.
            // 변경된 주문 상태만 보고 거부하지 않고 새 스냅샷에서 접수를 다시 확인한다.
            let existing = sqlx::query_as::<_, StoredCommand>(
                "SELECT order_id,user_id,order_type,order_side,base_mint,quote_mint,price::text AS price,amount::text AS amount,quote_amount::text AS quote_amount,fee_rate::text AS fee_rate,accepted_at FROM engine_order_receipts WHERE order_id=$1",
            )
            .bind(order_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(ReceiptError::Invalid)?;
            ensure!(existing.matches(request), ReceiptError::Conflict);
            transaction.commit().await?;
            return Ok(AcceptanceResult {
                fee_rate: existing.fee_rate,
                receipt: existing.acceptance(),
                created: false,
            });
        }
    };
    ensure!(original.matches(request), ReceiptError::Conflict);
    crate::domains::cex::engine::amount::fee(Decimal::ZERO, original.fee_rate)?;

    // 사전 조회가 모두 빈 결과여도 DB PK가 승자를 정한다. DO UPDATE로 기존 접수의
    // 내용·시간을 덮어쓰지 않는다. 패자는 새 명령을 큐로 보내면 안 된다.
    let inserted = sqlx::query_as::<_, StoredCommand>(
        "INSERT INTO engine_order_receipts
         (order_id,user_id,order_type,order_side,base_mint,quote_mint,price,amount,quote_amount,fee_rate)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
         ON CONFLICT (order_id) DO NOTHING RETURNING order_id,user_id,order_type,order_side,base_mint,quote_mint,price::text AS price,amount::text AS amount,quote_amount::text AS quote_amount,fee_rate::text AS fee_rate,accepted_at",
    )
    .bind(order_id)
    .bind(user_id)
    .bind(&original.order_type)
    .bind(&original.order_side)
    .bind(&original.base_mint)
    .bind(&original.quote_mint)
    .bind(original.price)
    .bind(original.amount)
    .bind(original.quote_amount)
    .bind(original.fee_rate)
    .fetch_optional(&mut *transaction)
    .await?;
    let created = inserted.is_some();
    let stored = match inserted {
        Some(row) => row,
        None => {
            sqlx::query_as::<_, StoredCommand>(
                "SELECT order_id,user_id,order_type,order_side,base_mint,quote_mint,price::text AS price,amount::text AS amount,quote_amount::text AS quote_amount,fee_rate::text AS fee_rate,accepted_at FROM engine_order_receipts WHERE order_id=$1",
            )
            .bind(order_id)
            .fetch_one(&mut *transaction)
            .await?
        }
    };
    ensure!(stored.matches(request), ReceiptError::Conflict);
    transaction
        .commit()
        .await
        .context("Commit durable engine acceptance")?;
    Ok(AcceptanceResult {
        fee_rate: stored.fee_rate,
        receipt: stored.acceptance(),
        created,
    })
}

#[cfg(test)]
mod tests;
