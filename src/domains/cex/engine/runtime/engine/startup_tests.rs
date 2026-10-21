//! 실제 PostgreSQL 스냅샷과 표준 엔진 시작 경계를 검증한다.
//!
//! SQLx 0.6의 테스트 기능으로 함수마다 새 DB를 만든다. 기존 통합 테스트의
//! localhost:5432·공유 테이블 전체 삭제 헬퍼를 사용하지 않는다.

use super::*;
use sqlx::{Executor as _, PgPool};
use std::path::PathBuf;

async fn create_schema(pool: &PgPool) -> Result<()> {
    // 테스트 대상이 읽는 계약만 구성한다. 운영 마이그레이션이나 다른 도메인을
    // 실행하지 않으며, 제약조건을 느슨하게 두어 손상 상태도 직접 시험한다.
    pool.execute(
        "CREATE TABLE user_balances (
            id BIGINT PRIMARY KEY,
            user_id BIGINT NOT NULL,
            mint_address TEXT NOT NULL,
            available NUMERIC NOT NULL,
            locked NUMERIC NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
    )
    .await?;
    pool.execute("CREATE TABLE orders (id BIGINT PRIMARY KEY, status TEXT)")
        .await?;
    Ok(())
}

fn create_engine(pool: PgPool) -> (HighPerformanceEngine, PathBuf) {
    let mut engine = HighPerformanceEngine::new(Database::noop(pool));
    // 테스트 명령의 Kafka 주소는 사용하지 않는 루프백 포트로 설정한다.
    // 생성 시 연결하던 기존 코드에서도 실제 브로커에 접근하지 않게 하고,
    // 각 테스트의 WAL 경로를 분리해 실행 중인 엔진 파일과 섞이지 않게 한다.
    engine.kafka_producer = None;
    let wal_dir = std::env::temp_dir().join(format!("cex-startup-{}", uuid::Uuid::new_v4()));
    engine.wal_dir = wal_dir.clone();
    (engine, wal_dir)
}

async fn finish_engine(engine: &mut HighPerformanceEngine, wal_dir: &PathBuf) -> Result<()> {
    // 회귀를 재현하는 구버전은 잘못 시작될 수 있으므로 assert 전에 정리한다.
    // 실패한 시작의 정지도 안전해야 하며, 테스트 DB 외의 파일을 지우지 않는다.
    engine.stop().await?;
    if wal_dir.exists() {
        std::fs::remove_dir_all(wal_dir)?;
    }
    Ok(())
}

async fn assert_start_blocked(pool: PgPool, expected_reason: &str) -> Result<()> {
    let (mut engine, wal_dir) = create_engine(pool);
    let result = engine.start().await;
    let started_worker = engine.engine_thread.is_some()
        || engine.wal_thread.is_some()
        || engine.db_writer_thread.is_some();
    let published_balance = engine
        .executor
        .lock()
        .balance_cache()
        .get_balance(101, "USDT")
        .is_some();
    finish_engine(&mut engine, &wal_dir).await?;

    assert!(result.is_err(), "불완전한 상태에서 시작을 허용했다");
    let reason = format!("{:#}", result.unwrap_err());
    assert!(
        reason.contains(expected_reason),
        "예상하지 못한 오류: {reason}"
    );
    assert!(!started_worker, "검증 전에 작업 스레드가 시작됐다");
    assert!(!published_balance, "검증되지 않은 잔액이 캐시에 반영됐다");
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn startup_rejects_locked_balance_without_replay(pool: PgPool) -> Result<()> {
    create_schema(&pool).await?;
    pool.execute(
        "INSERT INTO user_balances (id, user_id, mint_address, available, locked)
         VALUES (1, 101, 'USDT', 70, 30)",
    )
    .await?;
    assert_start_blocked(pool, "locked balance").await
}

#[sqlx::test(migrations = false)]
async fn startup_rejects_pending_order_even_without_locked_balance(pool: PgPool) -> Result<()> {
    create_schema(&pool).await?;
    pool.execute("INSERT INTO orders VALUES (90001, 'pending')")
        .await?;
    assert_start_blocked(pool, "unresolved order").await
}

#[sqlx::test(migrations = false)]
async fn startup_rejects_negative_available_balance(pool: PgPool) -> Result<()> {
    create_schema(&pool).await?;
    pool.execute(
        "INSERT INTO user_balances (id, user_id, mint_address, available, locked)
         VALUES (1, 101, 'USDT', -1, 0)",
    )
    .await?;
    assert_start_blocked(pool, "negative balance").await
}

#[sqlx::test(migrations = false)]
async fn startup_rejects_negative_locked_balance(pool: PgPool) -> Result<()> {
    create_schema(&pool).await?;
    pool.execute(
        "INSERT INTO user_balances (id, user_id, mint_address, available, locked)
         VALUES (1, 101, 'USDT', 100, -1)",
    )
    .await?;
    assert_start_blocked(pool, "negative balance").await
}

#[sqlx::test(migrations = false)]
async fn startup_rejects_partial_unknown_and_null_order_statuses(pool: PgPool) -> Result<()> {
    create_schema(&pool).await?;
    for status in [Some("partial"), Some("unrecognized"), None] {
        sqlx::query("INSERT INTO orders VALUES (90001, $1)")
            .bind(status)
            .execute(&pool)
            .await?;
        assert_start_blocked(pool.clone(), "unresolved order").await?;
        // SQLx가 이 테스트만을 위해 만든 DB의 해당 행만 정리한다.
        pool.execute("DELETE FROM orders WHERE id = 90001").await?;
    }
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn failed_validation_does_not_publish_earlier_valid_rows(pool: PgPool) -> Result<()> {
    create_schema(&pool).await?;
    pool.execute(
        "INSERT INTO user_balances (id, user_id, mint_address, available, locked)
         VALUES (1, 101, 'USDT', 100, 0), (2, 102, 'SOL', 5, 1)",
    )
    .await?;
    assert_start_blocked(pool.clone(), "locked balance").await?;

    // 실패는 예약 해제나 잔액 정정을 동반하지 않는다. 정상 첫 행도 실제
    // Executor에 반영되지 않았는지는 공통 시작 검증에서 함께 확인했다.
    let stored: (Decimal, Decimal) =
        sqlx::query_as("SELECT available, locked FROM user_balances WHERE user_id = 102")
            .fetch_one(&pool)
            .await?;
    assert_eq!(stored, (Decimal::new(5, 0), Decimal::ONE));
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn startup_rejects_duplicate_balance_rows(pool: PgPool) -> Result<()> {
    create_schema(&pool).await?;
    pool.execute(
        "INSERT INTO user_balances (id, user_id, mint_address, available, locked)
         VALUES (1, 101, 'USDT', 100, 0), (2, 101, 'USDT', 200, 0)",
    )
    .await?;
    assert_start_blocked(pool, "duplicate balance").await
}

#[sqlx::test(migrations = false)]
async fn startup_rejects_unrepresentable_decimal_without_panicking(pool: PgPool) -> Result<()> {
    create_schema(&pool).await?;
    for amount in ["NaN", "999999999999999999999999999999999999999999999999"] {
        sqlx::query(
            "INSERT INTO user_balances (id, user_id, mint_address, available, locked)
             VALUES (1, 101, 'USDT', $1::text::numeric, 0)",
        )
        .bind(amount)
        .execute(&pool)
        .await?;
        // PostgreSQL NUMERIC은 NaN과 Decimal 범위 밖의 값을 저장할 수 있다.
        // 잘못된 값을 0으로 바꾸거나 panic하지 않고 해독 오류로 전파해야 한다.
        assert_start_blocked(pool.clone(), "available").await?;
        pool.execute("DELETE FROM user_balances WHERE id = 1")
            .await?;
    }
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn startup_rejects_invalid_user_and_asset_identifiers(pool: PgPool) -> Result<()> {
    create_schema(&pool).await?;
    for (user_id, asset, reason) in [
        (-1_i64, "USDT", "invalid user id"),
        (0_i64, "USDT", "invalid user id"),
        (101_i64, "", "invalid asset identifier"),
        (101_i64, " USDT", "invalid asset identifier"),
    ] {
        sqlx::query(
            "INSERT INTO user_balances (id, user_id, mint_address, available, locked)
             VALUES (1, $1, $2, 100, 0)",
        )
        .bind(user_id)
        .bind(asset)
        .execute(&pool)
        .await?;
        assert_start_blocked(pool.clone(), reason).await?;
        pool.execute("DELETE FROM user_balances WHERE id = 1")
            .await?;
    }
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn startup_propagates_database_inspection_failure(pool: PgPool) -> Result<()> {
    create_schema(&pool).await?;
    pool.execute("DROP TABLE orders").await?;
    assert_start_blocked(pool, "Failed to inspect unresolved orders").await
}

#[sqlx::test(migrations = false)]
async fn startup_preserves_exact_balances_with_terminal_order_history(pool: PgPool) -> Result<()> {
    create_schema(&pool).await?;
    pool.execute("INSERT INTO orders VALUES (1, 'filled'), (2, 'cancelled'), (3, 'rejected')")
        .await?;
    pool.execute(
        "INSERT INTO user_balances (id, user_id, mint_address, available, locked)
         VALUES (1, 101, 'USDT', 100.250000001, 0), (2, 102, 'SOL', 0, 0)",
    )
    .await?;

    let (mut engine, wal_dir) = create_engine(pool.clone());
    engine.start().await?;
    let balance = engine.get_balance(101, "USDT").await?;
    let zero_balance = engine.get_balance(102, "SOL").await?;
    let orders = engine
        .get_orderbook(&TradingPair::new("SOL".into(), "USDT".into()), None)
        .await?;
    finish_engine(&mut engine, &wal_dir).await?;

    assert_eq!(balance, (Decimal::new(100250000001, 9), Decimal::ZERO));
    assert_eq!(zero_balance, (Decimal::ZERO, Decimal::ZERO));
    assert!(orders.0.is_empty() && orders.1.is_empty());
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM orders")
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 3, "종료 주문 기록을 삭제하면 안 된다");
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn startup_reads_one_repeatable_read_readonly_snapshot(pool: PgPool) -> Result<()> {
    create_schema(&pool).await?;
    pool.execute(
        "INSERT INTO user_balances (id, user_id, mint_address, available, locked)
         VALUES (1, 101, 'USDT', 100, 0)",
    )
    .await?;
    pool.execute("ALTER TABLE user_balances RENAME TO stored_balances")
        .await?;
    // 로더가 실제로 사용하는 연결의 격리 수준·읽기 전용 여부를 DB에서 검사한다.
    // 쿼리 문자열을 비교하는 대신 잘못된 격리 수준에서는 유효 잔액이 나오지 않게 한다.
    pool.execute(
        "CREATE VIEW user_balances AS
         SELECT user_id, mint_address,
           CASE WHEN current_setting('transaction_isolation') = 'repeatable read'
                  AND current_setting('transaction_read_only') = 'on'
                THEN available ELSE -1::numeric END AS available,
           locked FROM stored_balances",
    )
    .await?;

    let (mut engine, wal_dir) = create_engine(pool);
    engine.start().await?;
    let balance = engine.get_balance(101, "USDT").await?;
    finish_engine(&mut engine, &wal_dir).await?;
    assert_eq!(balance, (Decimal::new(100, 0), Decimal::ZERO));
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn duplicate_start_does_not_reload_or_replace_running_state(pool: PgPool) -> Result<()> {
    create_schema(&pool).await?;
    pool.execute(
        "INSERT INTO user_balances (id, user_id, mint_address, available, locked)
         VALUES (1, 101, 'USDT', 100, 0)",
    )
    .await?;

    let (mut engine, wal_dir) = create_engine(pool);
    engine.start().await?;
    // 엔진의 현재 상태를 DB와 다르게 만들어 중복 start가 이를 덮지 않는지 확인한다.
    // 이는 이 테스트 안에서만 수행하는 조작이며 외부 증감 API를 호출하지 않는다.
    engine.executor.lock().balance_cache_mut().set_balance(
        101,
        "USDT",
        Decimal::new(90, 0),
        Decimal::new(10, 0),
    );
    let result = engine.start().await;
    let balance = engine.get_balance(101, "USDT").await?;
    finish_engine(&mut engine, &wal_dir).await?;

    assert!(result.unwrap_err().to_string().contains("already running"));
    assert_eq!(balance, (Decimal::new(90, 0), Decimal::new(10, 0)));
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn empty_database_starts_and_stopped_instance_cannot_restart(pool: PgPool) -> Result<()> {
    create_schema(&pool).await?;
    let (mut engine, wal_dir) = create_engine(pool);
    engine.start().await?;
    let balance = engine.get_balance(101, "USDT").await?;
    finish_engine(&mut engine, &wal_dir).await?;
    let result = engine.start().await;

    assert_eq!(balance, (Decimal::ZERO, Decimal::ZERO));
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("create a new instance"));
    Ok(())
}
