//! SQLx가 매 테스트 생성한 DB만 사용한다. 벤치 엔진은 실행 워커 없이 큐를 관찰한다.
use super::*;
use crate::domains::cex::engine::runtime::HighPerformanceEngine;
use crate::domains::cex::services::OrderService;
use crate::shared::database::Database;
use sqlx::Executor;
use std::sync::Arc;
use tokio::sync::Mutex;

async fn schema(pool: &PgPool) -> Result<()> {
    pool.execute("CREATE TABLE orders (id BIGINT PRIMARY KEY,user_id BIGINT,status TEXT,filled_amount NUMERIC DEFAULT 0)").await?;
    pool.execute(
        "CREATE TABLE order_submissions (order_id BIGINT UNIQUE,user_id BIGINT,
        order_type TEXT,order_side TEXT,base_mint TEXT,quote_mint TEXT,price NUMERIC(30,9),
        amount NUMERIC(30,9),quote_amount NUMERIC(30,9))",
    )
    .await?;
    pool.execute(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/schema/order_receipts.sql"
    )))
    .await?;
    pool.execute(
        "INSERT INTO orders VALUES (1,101,'pending',0);
        INSERT INTO order_submissions VALUES (1,101,'buy','limit','SOL','USDT',100,2,NULL)",
    )
    .await?;
    Ok(())
}

fn request() -> CreateOrderRequest {
    serde_json::from_value(
        serde_json::json!({"order_id":1,"user_id":101,"order_type":"buy",
        "order_side":"limit","base_mint":"SOL","price":"100","amount":"2"}),
    )
    .unwrap()
}

fn service(pool: PgPool) -> (OrderService, Arc<Mutex<HighPerformanceEngine>>) {
    let engine = Arc::new(Mutex::new(HighPerformanceEngine::new_bench()));
    (
        OrderService::new(Database::noop(pool), engine.clone()),
        engine,
    )
}

#[sqlx::test(migrations = false)]
async fn receipt_is_durable_before_dispatch_and_duplicate_keeps_timestamp(
    pool: PgPool,
) -> Result<()> {
    schema(&pool).await?;
    let (service, engine) = service(pool.clone());
    let first = service.create_order(101, request()).await?;
    let duplicate = service.create_order(101, request()).await?;
    assert!(first.created);
    assert!(!duplicate.created);
    assert_eq!(first.receipt.accepted_at, duplicate.receipt.accepted_at);
    assert_eq!(engine.lock().await.test_queued_orders(), 1);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM engine_order_receipts")
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 1);
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn concurrent_receipts_dispatch_exactly_one_command(pool: PgPool) -> Result<()> {
    schema(&pool).await?;
    let (service, engine) = service(pool);
    let results =
        futures_util::future::join_all((0..12).map(|_| service.create_order(101, request()))).await;
    let mut created = 0;
    for result in results {
        created += usize::from(result?.created);
    }
    assert_eq!(created, 1);
    assert_eq!(engine.lock().await.test_queued_orders(), 1);
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn changed_command_or_owner_cannot_reuse_receipt(pool: PgPool) -> Result<()> {
    schema(&pool).await?;
    accept(&pool, &request()).await?;
    for field in [
        "user_id",
        "order_type",
        "order_side",
        "base_mint",
        "quote_mint",
        "price",
        "amount",
        "quote_amount",
    ] {
        let mut value = serde_json::json!({"order_id":1,"user_id":101,"order_type":"buy",
            "order_side":"limit","base_mint":"SOL","price":"100","amount":"2"});
        value[field] = match field {
            "user_id" => serde_json::json!(102),
            "order_type" => serde_json::json!("sell"),
            "order_side" => serde_json::json!("market"),
            "base_mint" => serde_json::json!("BTC"),
            "quote_mint" => serde_json::json!("USDC"),
            _ => serde_json::json!("3"),
        };
        let changed: CreateOrderRequest = serde_json::from_value(value)?;
        let error = accept(&pool, &changed)
            .await
            .err()
            .expect("conflict expected");
        assert!(
            matches!(
                error.downcast_ref::<ReceiptError>(),
                Some(ReceiptError::Conflict)
            ),
            "{field}: {error}"
        );
    }
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn unknown_or_terminal_platform_order_is_not_accepted(pool: PgPool) -> Result<()> {
    schema(&pool).await?;
    let mut unknown = request();
    unknown.order_id = Some(2);
    assert!(accept(&pool, &unknown).await.is_err());
    pool.execute("UPDATE orders SET status='filled',filled_amount=2")
        .await?;
    assert!(accept(&pool, &request()).await.is_err());
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM engine_order_receipts")
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 0);
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn platform_original_mismatch_is_rejected_before_receipt(pool: PgPool) -> Result<()> {
    schema(&pool).await?;
    let mut changed = request();
    changed.amount = Some(Decimal::from(3));
    assert!(matches!(
        accept(&pool, &changed)
            .await
            .err()
            .unwrap()
            .downcast_ref::<ReceiptError>(),
        Some(ReceiptError::Conflict)
    ));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM engine_order_receipts")
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 0);
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn current_order_changes_do_not_change_existing_receipt(pool: PgPool) -> Result<()> {
    schema(&pool).await?;
    let (service, engine) = service(pool.clone());
    service.create_order(101, request()).await?;
    pool.execute("UPDATE orders SET status='filled',filled_amount=2")
        .await?;
    assert!(!service.create_order(101, request()).await?.created);
    assert_eq!(engine.lock().await.test_queued_orders(), 1);
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn new_service_and_executor_do_not_dispatch_existing_receipt(pool: PgPool) -> Result<()> {
    schema(&pool).await?;
    let (first, _) = service(pool.clone());
    first.create_order(101, request()).await?;
    let (fresh, engine) = service(pool);
    assert!(!fresh.create_order(101, request()).await?.created);
    assert_eq!(engine.lock().await.test_queued_orders(), 0);
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn commit_failure_does_not_dispatch_or_report_acceptance(pool: PgPool) -> Result<()> {
    schema(&pool).await?;
    pool.execute(
        "CREATE TABLE commit_gate (id BIGINT PRIMARY KEY);
        ALTER TABLE engine_order_receipts ADD CONSTRAINT deferred_failure
        FOREIGN KEY(user_id) REFERENCES commit_gate(id) DEFERRABLE INITIALLY DEFERRED",
    )
    .await?;
    let (service, engine) = service(pool.clone());
    assert!(service.create_order(101, request()).await.is_err());
    assert_eq!(engine.lock().await.test_queued_orders(), 0);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM engine_order_receipts")
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 0);
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn closed_dispatch_channel_keeps_receipt_and_does_not_panic(pool: PgPool) -> Result<()> {
    use crate::domains::cex::engine::Engine;
    schema(&pool).await?;
    let (service, engine) = service(pool.clone());
    engine.lock().await.stop().await?;
    assert!(service.create_order(101, request()).await?.created);
    assert!(!service.create_order(101, request()).await?.created);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM engine_order_receipts")
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 1);
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn market_buy_preserves_exact_budget_and_null_amount(pool: PgPool) -> Result<()> {
    schema(&pool).await?;
    pool.execute("UPDATE order_submissions SET order_side='market',price=NULL,amount=NULL,quote_amount=123.250000001").await?;
    let mut market = request();
    market.order_side = "market".into();
    market.price = None;
    market.amount = None;
    market.quote_amount = Some("123.250000001".parse()?);
    let (service, engine) = service(pool.clone());
    assert!(service.create_order(101, market).await?.created);
    let (amount, budget): (Option<Decimal>, Decimal) =
        sqlx::query_as("SELECT amount,quote_amount FROM engine_order_receipts")
            .fetch_one(&pool)
            .await?;
    assert!(amount.is_none());
    assert_eq!(budget, "123.250000001".parse::<Decimal>()?);
    assert_eq!(engine.lock().await.test_queued_orders(), 1);
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn invalid_identifiers_and_fields_never_dispatch(pool: PgPool) -> Result<()> {
    schema(&pool).await?;
    let (service, engine) = service(pool);
    for id in [None, Some(0), Some(u64::MAX)] {
        let mut invalid = request();
        invalid.order_id = id;
        assert!(service.create_order(101, invalid).await.is_err());
    }
    let mut invalid = request();
    invalid.amount = Some(Decimal::ZERO);
    assert!(service.create_order(101, invalid).await.is_err());
    assert_eq!(engine.lock().await.test_queued_orders(), 0);
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn receipt_foreign_key_protects_platform_original(pool: PgPool) -> Result<()> {
    schema(&pool).await?;
    accept(&pool, &request()).await?;
    assert!(pool
        .execute("DELETE FROM order_submissions WHERE order_id=1")
        .await
        .is_err());
    Ok(())
}

fn router(pool: PgPool) -> axum::Router {
    use crate::domains::{
        cex::services::CexState, swap::services::state::SwapState,
        wallet::services::state::WalletState,
    };
    use crate::shared::services::AppState;
    let db = Database::noop(pool);
    let engine = Arc::new(Mutex::new(HighPerformanceEngine::new_bench()));
    crate::routes::create_router().with_state(AppState {
        db: db.clone(),
        engine: engine.clone(),
        cex_state: CexState::new(db.clone(), engine),
        wallet_state: WalletState::new(db.clone()).unwrap(),
        swap_state: SwapState::new(db),
    })
}

fn http_request() -> axum::http::Request<axum::body::Body> {
    axum::http::Request::post("/api/cex/orders").header("Content-Type","application/json")
        .body(axum::body::Body::from(r#"{"order_id":1,"user_id":101,"order_type":"buy","order_side":"limit","base_mint":"SOL","price":"100","amount":"2"}"#)).unwrap()
}

#[sqlx::test(migrations = false)]
async fn http_contract_distinguishes_new_and_existing_receipt(pool: PgPool) -> Result<()> {
    use tower::ServiceExt;
    schema(&pool).await?;
    let router = router(pool);
    for status in [axum::http::StatusCode::ACCEPTED, axum::http::StatusCode::OK] {
        let response = router.clone().oneshot(http_request()).await?;
        assert_eq!(response.status(), status);
        let body = axum::body::to_bytes(response.into_body(), 4096).await?;
        let receipt: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(receipt["order_id"], "1");
        assert_eq!(receipt["user_id"], "101");
        assert_eq!(receipt["receipt_status"], "accepted");
        assert!(receipt["accepted_at"].as_str().is_some());
        assert!(
            receipt.get("status").is_none(),
            "체결 상태를 접수 응답에 만들지 않는다"
        );
    }
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn missing_receipt_storage_returns_unavailable_without_sql_details(
    pool: PgPool,
) -> Result<()> {
    use tower::ServiceExt;
    schema(&pool).await?;
    pool.execute("DROP TABLE engine_order_receipts").await?;
    let response = router(pool).oneshot(http_request()).await?;
    assert_eq!(
        response.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    let body = axum::body::to_bytes(response.into_body(), 4096).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;
    assert_eq!(
        json,
        serde_json::json!({"code":"ORDER_ACCEPTANCE_UNAVAILABLE"})
    );
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn acceptance_forces_synchronous_commit_even_if_session_default_is_off(
    pool: PgPool,
) -> Result<()> {
    schema(&pool).await?;
    pool.execute("CREATE FUNCTION check_sync() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
        IF current_setting('synchronous_commit') <> 'on' OR
           current_setting('transaction_isolation') <> 'read committed'
        THEN RAISE EXCEPTION 'unexpected receipt transaction settings'; END IF;
        RETURN NEW; END $$;
        CREATE TRIGGER check_sync BEFORE INSERT ON engine_order_receipts FOR EACH ROW EXECUTE FUNCTION check_sync()").await?;
    let asynchronous = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET synchronous_commit=off")
                .execute(&mut *connection)
                .await?;
            sqlx::query("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect_with(pool.connect_options().clone())
        .await?;
    let setting: String = sqlx::query_scalar("SHOW synchronous_commit")
        .fetch_one(&asynchronous)
        .await?;
    assert_eq!(setting, "off");
    let isolation: String = sqlx::query_scalar("SHOW default_transaction_isolation")
        .fetch_one(&asynchronous).await?;
    assert_eq!(isolation,"repeatable read");
    assert!(accept(&asynchronous, &request()).await?.created);
    asynchronous.close().await;
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn concurrent_completion_keeps_duplicate_acceptance_valid(pool: PgPool) -> Result<()> {
    schema(&pool).await?;
    let outcomes = futures_util::future::join_all((0..12).map(|_| async {
        let outcome = accept(&pool, &request()).await?;
        if outcome.created {
            pool.execute("UPDATE orders SET status='filled',filled_amount=2")
                .await?;
        }
        Ok::<_, anyhow::Error>(outcome.created)
    }))
    .await;
    let mut created = 0;
    for outcome in outcomes {
        created += usize::from(outcome?);
    }
    assert_eq!(created, 1);
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn receipt_preserves_large_exact_amount_without_decoder_panic(pool: PgPool) -> Result<()> {
    schema(&pool).await?;
    pool.execute("UPDATE order_submissions SET price=1,amount=99999999999999999999.123456780").await?;
    let mut command=request();
    command.price=Some(Decimal::ONE);
    command.amount=Some(Decimal::from_str_exact("99999999999999999999.12345678")?);
    assert!(accept(&pool,&command).await?.created);
    assert!(!accept(&pool,&command).await?.created);
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn receipt_rejects_unrepresentable_original_without_panic(pool: PgPool) -> Result<()> {
    schema(&pool).await?;
    pool.execute("UPDATE order_submissions SET amount=999999999999999999999.123456789").await?;
    assert!(accept(&pool,&request()).await.is_err());
    let count:i64=sqlx::query_scalar("SELECT count(*) FROM engine_order_receipts").fetch_one(&pool).await?;
    assert_eq!(count,0);
    Ok(())
}
