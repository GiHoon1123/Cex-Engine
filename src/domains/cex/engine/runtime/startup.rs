//! 저널 시작점이 없는 최초 기동에서만 플랫폼 잔액을 검증한다.
//! 기존 접수·활성 주문·예약금이 있으면 이를 새 시작점으로 추측해 변환하지 않는다.
//! 자체 저널이 존재하는 재시작은 이 함수를 호출하지 않고 기록을 재생한다.

use anyhow::{bail, ensure, Context, Result};
use rust_decimal::Decimal;
use sqlx::{PgPool, Row};

use crate::domains::cex::engine::balance_cache::BalanceCache;

/// 같은 시점의 주문·잔액을 검사하고, 전체 검증이 성공한 캐시만 반환한다.
///
/// `REPEATABLE READ`는 두 SELECT가 서로 다른 커밋 시점을 읽는 것을 막는다.
/// `READ ONLY`는 이 함수가 정정·취소·예약 해제 등의 금융 동작을 하지 못하게 한다.
/// 단, 이 스냅샷은 다른 플랫폼 프로세스의 이후 쓰기를 막는 리더 선출·펜싱은 아니다.
pub(super) async fn load_balances(pool: &PgPool) -> Result<BalanceCache> {
    let mut transaction = pool
        .begin()
        .await
        .context("Failed to begin startup inspection")?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *transaction)
        .await
        .context("Failed to configure startup inspection snapshot")?;

    // 접수 기록은 체결 완료·재생 위치가 아니다. DB 주문이 우연히 종료 상태여도
    // 현재는 이 접수를 안전하게 복원할 결과 로그가 없어 재개를 차단한다.
    let receipt: Option<i64> =
        sqlx::query_scalar("SELECT order_id FROM engine_order_receipts ORDER BY order_id LIMIT 1")
            .fetch_optional(&mut *transaction)
            .await
            .context("Failed to inspect durable engine receipts")?;
    if let Some(order_id) = receipt {
        bail!(
            "Engine startup blocked: durable receipt order_id={}; authoritative replay is required",
            order_id
        );
    }

    // 현재 플랫폼은 주문/입금/공개 호가의 수신 키를 보존한다. 공개 호가만 있어도
    // 이미 엔진이 저널 위치를 발행했다는 뜻이므로 새 경로를 최초 기동으로 추측하지 않는다.
    // 잔액 투영에는 입금/주문 멱등 원본과 처리 순서가 없어 자체 저널을 대신할 수 없다.
    let received: Option<String> =
        sqlx::query_scalar("SELECT event_id FROM engine_event_receipts ORDER BY event_id LIMIT 1")
            .fetch_optional(&mut *transaction)
            .await
            .context("Failed to inspect current engine event history")?;
    ensure!(
        received.is_none(),
        "Engine startup blocked: current engine event history exists; restore the authoritative journal"
    );

    // 결과 불명·이번 미접수도 과거 미실행의 증거가 아니다. 입금 전송 시도가 있으면
    // 새 시작점에 처리 식별자를 잃어버린 채 잔액만 반입하지 않고 원래 저널 복구를 요구한다.
    // 두 이력 표가 없거나 읽을 수 없는 경우에도 빈 이력으로 우회하지 않는다.
    let attempted: Option<String> = sqlx::query_scalar(
        "SELECT id::text FROM bitcoin_credit_dispatch_attempts ORDER BY id LIMIT 1",
    )
    .fetch_optional(&mut *transaction)
    .await
    .context("Failed to inspect native deposit dispatch history")?;
    ensure!(
        attempted.is_none(),
        "Engine startup blocked: native deposit dispatch history exists; restore the authoritative journal"
    );

    // 종료 상태만 명시적으로 허용한다. pending·partial뿐 아니라 알 수 없는
    // 상태도 복구 판단 없이 빠뜨리지 않는다. NULL 역시 종료로 간주하지 않는다.
    let unresolved_order: Option<(i64, Option<String>)> = sqlx::query_as(
        "SELECT id, status FROM orders
         WHERE status IS NULL OR status NOT IN ('filled', 'cancelled', 'rejected', 'expired')
         ORDER BY id LIMIT 1",
    )
    .fetch_optional(&mut *transaction)
    .await
    .context("Failed to inspect unresolved orders during startup")?;

    if let Some((order_id, status)) = unresolved_order {
        bail!(
            "Engine startup blocked: unresolved order id={}, status={}; authoritative replay is required",
            order_id,
            status.as_deref().unwrap_or("NULL")
        );
    }

    let rows = sqlx::query(
        "SELECT user_id, mint_address, available::text AS available, locked::text AS locked FROM user_balances
         ORDER BY user_id, mint_address",
    )
    .fetch_all(&mut *transaction)
    .await
    .context("Failed to read balances during startup")?;

    // 실제 Executor를 한 행씩 수정하면 뒤쪽의 오류 때문에 일부만 반영될 수 있다.
    // 별도 캐시에 구성하고 모든 행·트랜잭션 검증이 끝난 다음 한 번에 전달한다.
    let mut balances = BalanceCache::with_capacity(rows.len());
    for row in rows {
        let stored_user_id: i64 = row.try_get("user_id")?;
        ensure!(
            stored_user_id > 0,
            "Engine startup blocked: invalid user id"
        );
        let user_id = u64::try_from(stored_user_id)?;
        let mint: String = row.try_get("mint_address")?;
        let available = crate::shared::database::exact_decimal::read_decimal(&row, "available")?;
        let locked = crate::shared::database::exact_decimal::read_decimal(&row, "locked")?;

        ensure!(
            !mint.is_empty() && mint.trim() == mint,
            "Engine startup blocked: invalid asset identifier for user={}",
            user_id
        );
        ensure!(
            available >= Decimal::ZERO && locked >= Decimal::ZERO,
            "Engine startup blocked: negative balance for user={}, mint={}",
            user_id,
            mint
        );
        // 저장 스키마가 잘못 확장돼도 실행 캐시와 다른 금액 계약을 허용하지 않는다.
        let upper_bound = Decimal::from_i128_with_scale(1_000_000_000_000_000_000_000, 0);
        ensure!(
            available < upper_bound && available.normalize().scale() <= 9,
            "Engine startup blocked: available exceeds NUMERIC(30,9)"
        );
        ensure!(
            locked == Decimal::ZERO,
            "Engine startup blocked: unresolved locked balance for user={}, mint={}; authoritative replay is required",
            user_id,
            mint
        );
        ensure!(
            balances.get_balance(user_id, &mint).is_none(),
            "Engine startup blocked: duplicate balance for user={}, mint={}",
            user_id,
            mint
        );

        // 읽는 과정에서 가산·합산·반올림을 하지 않고 DB의 십진수 값을 그대로 둔다.
        balances.set_balance(user_id, &mint, available, locked);
    }

    transaction
        .commit()
        .await
        .context("Failed to finish startup inspection")?;
    Ok(balances)
}
