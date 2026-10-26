//! 실제 PostgreSQL 스냅샷과 표준 엔진 시작 경계를 검증한다.
//!
//! SQLx 0.6의 테스트 기능으로 함수마다 새 DB를 만든다. 기존 통합 테스트의
//! localhost:5432·공유 테이블 전체 삭제 헬퍼를 사용하지 않는다.

use super::*;
use crate::domains::cex::engine::Engine;
use sqlx::{Executor as _, PgPool};

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
    // 플랫폼이 현재 기록하는 수신/전송 이력의 읽기 계약이다. 금융 효과를 만드는
    // 서비스/트리거를 재구현하지 않고 최초 기동이 알아야 할 행의 존재를 검증한다.
    pool.execute("CREATE TABLE engine_event_receipts (event_id VARCHAR(120) PRIMARY KEY, engine_id VARCHAR(36) NOT NULL, payload_hash VARCHAR(64) NOT NULL)")
        .await?;
    pool.execute("CREATE TABLE bitcoin_credit_dispatch_attempts (id UUID PRIMARY KEY, receipt_id UUID NOT NULL, position BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE)")
        .await?;
    pool.execute("CREATE TABLE bitcoin_withdrawals (id UUID PRIMARY KEY)")
        .await?;
    pool.execute("CREATE TABLE bitcoin_withdrawal_dispatch_attempts (id UUID PRIMARY KEY, withdrawal_id UUID)").await?;
    pool.execute("CREATE TABLE orders (id BIGINT PRIMARY KEY, status TEXT)")
        .await?;
    pool.execute("CREATE TABLE order_submissions (order_id BIGINT UNIQUE)")
        .await?;
    pool.execute(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/schema/order_receipts.sql"
    )))
    .await?;
    Ok(())
}

fn create_engine(pool: PgPool) -> HighPerformanceEngine {
    // 검증 실행기가 지정한 격리 디비와 사용하지 않는 루프백 브로커만 사용한다.
    {
        let directory = tempfile::tempdir().expect("isolated startup directory");
        let mut engine =
            HighPerformanceEngine::new(Database::noop(pool)).with_data_dir(directory.path());
        engine.test_directory = Some(directory);
        engine
    }
}

async fn finish_engine(engine: &mut HighPerformanceEngine) -> Result<()> {
    // 회귀가 잘못 시작되더라도 검증 전에 이 실행의 워커를 회수한다.
    engine.stop().await
}

async fn assert_start_blocked(pool: PgPool, expected_reason: &str) -> Result<()> {
    let mut engine = create_engine(pool);
    let result = engine.start().await;
    let started_worker = engine.engine_thread.is_some()
        || engine.output_thread.is_some()
        || engine.journal_thread.is_some();
    let published_balance = engine
        .state
        .as_mut()
        .unwrap()
        .executor
        .balance_cache()
        .get_balance(101, "USDT")
        .is_some();
    finish_engine(&mut engine).await?;

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
async fn startup_rejects_receipt_even_with_terminal_order(pool: PgPool) -> Result<()> {
    create_schema(&pool).await?;
    pool.execute("INSERT INTO orders VALUES (1,'filled'); INSERT INTO order_submissions VALUES (1);
        INSERT INTO engine_order_receipts(order_id,user_id,order_type,order_side,base_mint,quote_mint,price,amount,fee_rate)
        VALUES(1,101,'buy','limit','SOL','USDT',100,2,0)").await?;
    assert_start_blocked(pool, "durable receipt").await
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
    pool.execute("INSERT INTO orders VALUES (1, 'filled'), (2, 'cancelled'), (3, 'rejected'), (4, 'expired')")
        .await?;
    pool.execute(
        "INSERT INTO user_balances (id, user_id, mint_address, available, locked)
         VALUES (1, 101, 'USDT', 100.250000001, 0), (2, 102, 'SOL', 0, 0)",
    )
    .await?;

    let mut engine = create_engine(pool.clone());
    engine.start().await?;
    let balance = engine.handle().get_balance(101, "USDT").await?;
    let zero_balance = engine.handle().get_balance(102, "SOL").await?;
    let orders = engine
        .handle()
        .get_orderbook(&TradingPair::new("SOL".into(), "USDT".into()), None)
        .await?;
    finish_engine(&mut engine).await?;

    assert_eq!(balance, (Decimal::new(100250000001, 9), Decimal::ZERO));
    assert_eq!(zero_balance, (Decimal::ZERO, Decimal::ZERO));
    assert!(orders.0.is_empty() && orders.1.is_empty());
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM orders")
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 4, "만료를 포함한 종료 주문 기록을 삭제하면 안 된다");
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

    let mut engine = create_engine(pool);
    engine.start().await?;
    let balance = engine.handle().get_balance(101, "USDT").await?;
    finish_engine(&mut engine).await?;
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

    let mut engine = create_engine(pool);
    engine.start().await?;
    // 엔진의 현재 상태를 DB와 다르게 만들어 중복 start가 이를 덮지 않는지 확인한다.
    // 워커 소유 상태는 직접 만지지 않고, 직렬화된 예약 명령으로 90/10을 만든다.
    engine
        .handle()
        .lock_balance(101, "USDT", Decimal::from(10))
        .await?;
    let result = engine.start().await;
    let balance = engine.handle().get_balance(101, "USDT").await?;
    finish_engine(&mut engine).await?;

    assert!(result.unwrap_err().to_string().contains("already running"));
    assert_eq!(balance, (Decimal::new(90, 0), Decimal::new(10, 0)));
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn empty_database_starts_and_stopped_instance_cannot_restart(pool: PgPool) -> Result<()> {
    create_schema(&pool).await?;
    let mut engine = create_engine(pool);
    engine.start().await?;
    let balance = engine.handle().get_balance(101, "USDT").await?;
    finish_engine(&mut engine).await?;
    let result = engine.start().await;

    assert_eq!(balance, (Decimal::ZERO, Decimal::ZERO));
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("create a new instance"));
    Ok(())
}

// 큰 값도 Decimal에 정확히 표현되면 그대로 복원해야 한다. 표시상 끝의 0을
// 제거하되 정수부와 소수부의 유효 숫자를 잃지 않는지 실제 DB 경계를 검사한다.
#[sqlx::test(migrations = false)]
async fn startup_preserves_large_exact_fraction_without_decoder_panic(pool: PgPool) -> Result<()> {
    create_schema(&pool).await?;
    pool.execute(
        "INSERT INTO user_balances (id,user_id,mint_address,available,locked)
        VALUES (1,101,'USDT',99999999999999999999.12345678,0)",
    )
    .await?;
    let cache = super::super::startup::load_balances(&pool).await?;
    assert_eq!(
        cache.get_balance(101, "USDT").unwrap().available,
        Decimal::from_str_exact("99999999999999999999.12345678")?
    );
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn startup_rejects_unrepresentable_decimal_without_panic(pool: PgPool) -> Result<()> {
    create_schema(&pool).await?;
    pool.execute(
        "INSERT INTO user_balances (id,user_id,mint_address,available,locked)
        VALUES (1,101,'USDT',999999999999999999999.123456789,0)",
    )
    .await?;
    assert_start_blocked(pool, "available").await
}

#[sqlx::test(migrations = false)]
async fn startup_rejects_available_outside_current_storage_contract(pool: PgPool) -> Result<()> {
    create_schema(&pool).await?;
    for value in ["1000000000000000000000", "0.0000000001"] {
        sqlx::query(
            "INSERT INTO user_balances(id,user_id,mint_address,available,locked)
            VALUES(1,101,'USDT',$1::text::numeric,0)",
        )
        .bind(value)
        .execute(&pool)
        .await?;
        assert_start_blocked(pool.clone(), "available").await?;
        pool.execute("DELETE FROM user_balances WHERE id=1").await?;
    }
    Ok(())
}

// 현재 플랫폼 이력은 예전 접수 표가 비어 있어도 새 저널 초기화를 막아야 한다.
#[sqlx::test(migrations = false)]
async fn startup_rejects_current_financial_receipt_with_terminal_orders(
    pool: PgPool,
) -> Result<()> {
    create_schema(&pool).await?;
    pool.execute("INSERT INTO orders VALUES(1,'filled'); INSERT INTO user_balances(id,user_id,mint_address,available,locked) VALUES(1,101,'USDT',100,0)").await?;
    let engine_id = uuid::Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO engine_event_receipts VALUES($1,$2,$3)")
        .bind(format!("{engine_id}:1:0"))
        .bind(engine_id)
        .bind("a".repeat(64))
        .execute(&pool)
        .await?;
    assert_start_blocked(pool.clone(), "current engine event history").await?;
    let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM engine_event_receipts")
        .fetch_one(&pool)
        .await?;
    assert_eq!(remaining, 1, "복구 거부가 수신 원본을 삭제해서는 안 된다");
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn startup_rejects_public_receipt_as_prior_engine_activity(pool: PgPool) -> Result<()> {
    create_schema(&pool).await?;
    let engine_id = uuid::Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO engine_event_receipts VALUES($1,$2,$3)")
        .bind(format!("{engine_id}:1:market"))
        .bind(engine_id)
        .bind("a".repeat(64))
        .execute(&pool)
        .await?;
    assert_start_blocked(pool, "current engine event history").await
}

#[sqlx::test(migrations = false)]
async fn startup_does_not_treat_unknown_receipt_identity_as_empty_history(
    pool: PgPool,
) -> Result<()> {
    create_schema(&pool).await?;
    pool.execute("INSERT INTO engine_event_receipts VALUES('damaged','unknown','not-a-hash')")
        .await?;
    assert_start_blocked(pool, "current engine event history").await
}

#[sqlx::test(migrations = false)]
async fn startup_rejects_deposit_dispatch_without_any_received_result(pool: PgPool) -> Result<()> {
    create_schema(&pool).await?;
    sqlx::query("INSERT INTO bitcoin_credit_dispatch_attempts(id,receipt_id) VALUES($1,$2)")
        .bind(uuid::Uuid::new_v4())
        .bind(uuid::Uuid::new_v4())
        .execute(&pool)
        .await?;
    assert_start_blocked(pool.clone(), "native deposit dispatch history").await?;
    let remaining: i64 =
        sqlx::query_scalar("SELECT count(*) FROM bitcoin_credit_dispatch_attempts")
            .fetch_one(&pool)
            .await?;
    assert_eq!(
        remaining, 1,
        "불명 전송을 미실행으로 추측해 삭제해서는 안 된다"
    );
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn startup_blocks_current_withdrawal_history_without_journal(pool: PgPool) -> Result<()> {
    create_schema(&pool).await?;
    pool.execute(
        "INSERT INTO bitcoin_withdrawals(id) VALUES('00000000-0000-0000-0000-000000000001')",
    )
    .await?;
    assert_start_blocked(pool, "native withdrawal history exists").await
}

#[sqlx::test(migrations = false)]
async fn startup_blocks_unconfirmed_withdrawal_dispatch_without_journal(
    pool: PgPool,
) -> Result<()> {
    create_schema(&pool).await?;
    pool.execute("INSERT INTO bitcoin_withdrawal_dispatch_attempts(id,withdrawal_id) VALUES('00000000-0000-0000-0000-000000000002','00000000-0000-0000-0000-000000000001')").await?;
    assert_start_blocked(pool, "native withdrawal dispatch history exists").await
}

#[sqlx::test(migrations = false)]
async fn startup_refuses_missing_withdrawal_history_tables(pool: PgPool) -> Result<()> {
    create_schema(&pool).await?;
    pool.execute("DROP TABLE bitcoin_withdrawals").await?;
    assert_start_blocked(pool, "Failed to inspect native withdrawal history").await
}

#[sqlx::test(migrations = false)]
async fn startup_refuses_missing_withdrawal_dispatch_table(pool: PgPool) -> Result<()> {
    create_schema(&pool).await?;
    pool.execute("DROP TABLE bitcoin_withdrawal_dispatch_attempts")
        .await?;
    assert_start_blocked(pool, "Failed to inspect native withdrawal dispatch history").await
}

#[sqlx::test(migrations = false)]
async fn startup_refuses_missing_current_receipt_table(pool: PgPool) -> Result<()> {
    create_schema(&pool).await?;
    // 조회 불가를 빈 이력으로 바꾸지 않는지 시험 소유 DB의 실패 조건으로 확인한다.
    pool.execute("DROP TABLE engine_event_receipts").await?;
    assert_start_blocked(pool, "Failed to inspect current engine event history").await
}

#[sqlx::test(migrations = false)]
async fn startup_refuses_missing_deposit_dispatch_table(pool: PgPool) -> Result<()> {
    create_schema(&pool).await?;
    pool.execute("DROP TABLE bitcoin_credit_dispatch_attempts")
        .await?;
    assert_start_blocked(pool, "Failed to inspect native deposit dispatch history").await
}

#[sqlx::test(migrations = false)]
async fn current_history_probes_use_the_same_readonly_repeatable_snapshot(
    pool: PgPool,
) -> Result<()> {
    create_schema(&pool).await?;
    pool.execute("ALTER TABLE engine_event_receipts RENAME TO stored_event_receipts;
        CREATE VIEW engine_event_receipts AS SELECT 'wrong-context'::varchar event_id
        WHERE current_setting('transaction_isolation')<>'repeatable read' OR current_setting('transaction_read_only')<>'on';
        ALTER TABLE bitcoin_credit_dispatch_attempts RENAME TO stored_deposit_attempts;
        CREATE VIEW bitcoin_credit_dispatch_attempts AS SELECT '00000000-0000-0000-0000-000000000001'::uuid id
        WHERE current_setting('transaction_isolation')<>'repeatable read' OR current_setting('transaction_read_only')<>'on'").await?;
    let cache = super::super::startup::load_balances(&pool).await?;
    assert!(cache.get_balance(101, "USDT").is_none());
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn existing_journal_recovers_while_new_path_refuses_current_history(
    pool: PgPool,
) -> Result<()> {
    create_schema(&pool).await?;
    pool.execute("INSERT INTO user_balances(id,user_id,mint_address,available,locked) VALUES(1,101,'USDT',100,0)").await?;
    let directory = tempfile::tempdir()?;
    let mut original =
        HighPerformanceEngine::new(Database::noop(pool.clone())).with_data_dir(directory.path());
    original.start().await?;
    original
        .handle()
        .lock_balance(101, "USDT", Decimal::from(10))
        .await?;
    finish_engine(&mut original).await?;
    // 다른 플랫폼 투영과 현재 이력이 생긴 상황을 만든다. 원본을 삭제하거나
    // 새로운 디렉터리를 복구 경로라고 가정해서는 안 된다.
    let engine_id = uuid::Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO engine_event_receipts VALUES($1,$2,$3)")
        .bind(format!("{engine_id}:1:0"))
        .bind(engine_id)
        .bind("a".repeat(64))
        .execute(&pool)
        .await?;
    pool.execute("UPDATE user_balances SET available=999 WHERE user_id=101")
        .await?;
    assert_start_blocked(pool.clone(), "current engine event history").await?;
    // 풀을 닫아 DB 읽기가 불가능해도 원래 저널은 90/10을 정확히 복원해야 한다.
    // 복구가 플랫폼 DB의 999나 현재 이력을 조회했다면 이 시작은 실패한다.
    pool.close().await;
    let mut recovered =
        HighPerformanceEngine::new(Database::noop(pool)).with_data_dir(directory.path());
    recovered.start().await?;
    let balance = recovered.handle().get_balance(101, "USDT").await?;
    finish_engine(&mut recovered).await?;
    assert_eq!(balance, (Decimal::from(90), Decimal::from(10)));
    Ok(())
}
