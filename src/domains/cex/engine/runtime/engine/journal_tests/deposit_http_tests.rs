//! 실제 단일 코어·기록 워커·출력 워커를 HTTP 라우터에 연결한다.
//! 공개 브로커 대신 이벤트 수신 대역을 쓰며 저장·복구는 실제 임시 SQLite 저널이다.
use super::*;
use crate::shared::{database::Database, services::AppState};
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Router,
};
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;

fn router(engine: &HighPerformanceEngine) -> Router {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgresql://unused:unused@127.0.0.1:1/unused")
        .unwrap();
    crate::routes::create_router("isolated-engine-service-token-32-bytes")
        .with_state(AppState::new(Database::noop(pool), engine.handle()))
}
fn request(id: uuid::Uuid, amount: &str) -> Request<Body> {
    Request::post("/api/cex/deposits/credits")
        .header(
            "x-engine-service-token",
            "isolated-engine-service-token-32-bytes",
        )
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({"deposit_id":id,"user_id":3,"asset":"BTC","amount":amount})
                .to_string(),
        ))
        .unwrap()
}
#[tokio::test]
async fn deposit_http_commits_once_and_restart_preserves_original() -> Result<()> {
    let directory = Directory::new();
    let store = initialize(&directory.0);
    let mut engine = HighPerformanceEngine::new_bench();
    let (sent, events) = bounded(10);
    attach(&mut engine, store, move |batch, position| {
        output::visit_events(batch, position, |_, payload| {
            sent.send(payload).unwrap();
            Ok(())
        })
    });
    let app = router(&engine);
    let id = uuid::Uuid::new_v4();
    for _ in 0..2 {
        let response = app.clone().oneshot(request(id, "0.12345678")).await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["cache-control"], "no-store");
    }
    engine.stop().await?;
    let original: Vec<_> = events.try_iter().collect();
    assert_eq!(original.len(), 1);
    let event: serde_json::Value = serde_json::from_str(&original[0])?;
    assert_eq!(event["deposit_id"], id.to_string());
    assert_eq!(event["amount"], "0.12345678");
    let store = Store::open(&directory.0)?;
    assert_eq!(store.last_sequence()?, 2);
    let mut restarted = HighPerformanceEngine::new_bench();
    let (sent, events) = bounded(10);
    attach(&mut restarted, store, move |batch, position| {
        output::visit_events(batch, position, |_, payload| {
            sent.send(payload).unwrap();
            Ok(())
        })
    });
    assert_eq!(
        router(&restarted)
            .oneshot(request(id, "0.12345678"))
            .await?
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        restarted.handle().get_balance(3, "BTC").await?.0,
        Decimal::from_str_exact("0.12345678")?
    );
    restarted.stop().await?;
    assert_eq!(events.try_iter().count(), 0);
    Ok(())
}
#[tokio::test]
async fn deposit_http_changed_amount_is_a_durable_conflict() -> Result<()> {
    let directory = Directory::new();
    let store = initialize(&directory.0);
    let mut engine = HighPerformanceEngine::new_bench();
    attach(&mut engine, store, |_, _| Ok(()));
    let app = router(&engine);
    let id = uuid::Uuid::new_v4();
    assert_eq!(
        app.clone().oneshot(request(id, "1")).await?.status(),
        StatusCode::OK
    );
    let response = app.oneshot(request(id, "2")).await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await?)?;
    assert_eq!(body["code"], "DURABLY_REJECTED");
    engine.stop().await?;
    let store = Store::open(&directory.0)?;
    assert_eq!(store.last_sequence()?, 2);
    let (state, _) = store.recover()?;
    assert_eq!(
        state
            .executor
            .balance_cache()
            .get_balance(3, "BTC")
            .unwrap()
            .available,
        Decimal::ONE
    );
    Ok(())
}
#[tokio::test]
async fn deposit_http_failed_recording_is_unknown_and_never_published() -> Result<()> {
    let directory = Directory::new();
    let store = initialize(&directory.0);
    store.connection.execute_batch("CREATE TRIGGER reject_http_deposit BEFORE INSERT ON records BEGIN SELECT RAISE(ABORT,'격리 HTTP 기록 실패'); END;")?;
    let mut engine = HighPerformanceEngine::new_bench();
    let (sent, events) = bounded(10);
    attach(&mut engine, store, move |batch, position| {
        output::visit_events(batch, position, |_, payload| {
            sent.send(payload).unwrap();
            Ok(())
        })
    });
    let id = uuid::Uuid::new_v4();
    let response = router(&engine).oneshot(request(id, "1")).await?;
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await?)?;
    assert_eq!(body["code"], "OUTCOME_UNKNOWN");
    assert!(!body.to_string().contains("기록 실패"));
    assert!(engine.stop().await.is_err());
    assert!(events.try_recv().is_err());
    let store = Store::open(&directory.0)?;
    assert_eq!(store.last_sequence()?, 0);
    let (state, _) = store.recover()?;
    assert!(state
        .executor
        .balance_cache()
        .get_balance(3, "BTC")
        .is_none());
    Ok(())
}
#[tokio::test]
async fn deposit_http_cannot_acknowledge_while_the_journal_is_blocked() -> Result<()> {
    let directory = Directory::new();
    let store = initialize(&directory.0);
    let mut engine = HighPerformanceEngine::new_bench();
    let (sent, events) = bounded(10);
    attach(&mut engine, store, move |batch, position| {
        output::visit_events(batch, position, |_, payload| {
            sent.send(payload).unwrap();
            Ok(())
        })
    });
    let blocker = rusqlite::Connection::open(directory.0.join("journal.sqlite3"))?;
    blocker.execute_batch("BEGIN IMMEDIATE")?;
    let response = router(&engine).oneshot(request(uuid::Uuid::new_v4(), "1"));
    tokio::pin!(response);
    // 막힌 것은 기록 워커다. 요청의 비동기 대기 중에도 금융 출력과 성공은 아직 없어야 한다.
    tokio::select! { result=&mut response=>panic!("저널이 막힌 동안 응답했습니다: {:?}",result), _=tokio::time::sleep(std::time::Duration::from_millis(100))=>{} }
    assert!(events.try_recv().is_err());
    blocker.execute_batch("ROLLBACK")?;
    assert_eq!(response.await?.status(), StatusCode::OK);
    engine.stop().await?;
    assert_eq!(events.try_iter().count(), 1);
    Ok(())
}
#[tokio::test]
async fn deposit_http_calculation_rejection_survives_restart_without_reason_leak() -> Result<()> {
    let directory = Directory::new();
    let mut store = Store::open(&directory.0)?;
    let mut initial = seeded();
    let maximum = Decimal::from_str_exact("999999999999999999999")?;
    initial
        .executor
        .balance_cache_mut()
        .set_balance(3, "BTC", maximum, Decimal::ZERO);
    store.initialize(&initial)?;
    let id = uuid::Uuid::new_v4();
    let mut engine = HighPerformanceEngine::new_bench();
    attach(&mut engine, store, |_, _| Ok(()));
    let response = router(&engine).oneshot(request(id, "1")).await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let bytes = to_bytes(response.into_body(), 4096).await?;
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&bytes)?["code"],
        "DURABLY_REJECTED"
    );
    assert!(!String::from_utf8_lossy(&bytes).contains("NUMERIC"));
    engine.stop().await?;
    let mut restarted = HighPerformanceEngine::new_bench();
    attach(&mut restarted, Store::open(&directory.0)?, |_, _| Ok(()));
    assert_eq!(
        router(&restarted).oneshot(request(id, "1")).await?.status(),
        StatusCode::CONFLICT
    );
    assert_eq!(restarted.handle().get_balance(3, "BTC").await?.0, maximum);
    restarted.stop().await?;
    Ok(())
}
