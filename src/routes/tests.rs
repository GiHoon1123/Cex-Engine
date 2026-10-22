use super::create_router;
use crate::domains::cex::{engine::runtime::HighPerformanceEngine, services::state::CexState};
use crate::domains::swap::services::state::SwapState;
use crate::domains::wallet::services::state::WalletState;
use crate::shared::{database::Database, services::AppState};
use axum::{
    body::{to_bytes, Body},
    http::{Method, Request, StatusCode},
    Router,
};
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;

/// 외부 서버 없이 실제 HTTP 라우터를 구성한다.
///
/// `connect_lazy`는 설정만 가진 풀을 만든다. 쿼리가 실행될 때까지 DB에 연결하지
/// 않으며, 여기에 사용하는 1번 포트에는 테스트 DB를 띄우지 않는다.
/// Bench 엔진은 Kafka와 저장 워커를 생성하지 않는다. 아래 테스트는 경로 선택과
/// 입력 검증까지만 실행하므로 DB·Kafka·Solana 서버의 상태에 의존하지 않는다.
async fn app() -> (Router, HighPerformanceEngine) {
    let mut engine = HighPerformanceEngine::new_bench();
    engine.start().await.unwrap();
    (app_with_engine(engine.handle()), engine)
}

fn app_with_engine(engine: crate::domains::cex::engine::runtime::EngineHandle) -> Router {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgresql://unused:unused@127.0.0.1:1/unused")
        .expect("valid lazy pool configuration");
    let db = Database::noop(pool);
    let state = AppState {
        db: db.clone(),
        wallet_state: WalletState::new(db.clone()).expect("wallet client configuration"),
        swap_state: SwapState::new(db.clone()),
        cex_state: CexState::new(db, engine.clone()),
        engine,
    };
    create_router("isolated-engine-service-token-32-bytes").with_state(state)
}

/// 인증 코드를 제거해도 외부 의존성 없이 헬스체크에 응답해야 한다.
#[tokio::test]
async fn health_check_remains_available() {
    let (router, _engine) = app().await;
    let response = router
        .oneshot(Request::get("/api/health").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "ok");
}

/// 사용자가 토큰을 보내더라도 엔진에서 로그인·토큰 발급 API를 제공하지 않는다.
/// 401이 아니라 404를 기대한다. 인증 실패가 아니라 경로 자체가 없다는 계약이다.
#[tokio::test]
async fn removed_auth_routes_return_not_found() {
    let (router, _engine) = app().await;
    for (method, path) in [
        (Method::POST, "/api/auth/signup"),
        (Method::POST, "/api/auth/signin"),
        (Method::POST, "/api/auth/refresh"),
        (Method::POST, "/api/auth/logout"),
        (Method::GET, "/api/auth/me"),
    ] {
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header("Authorization", "Bearer retired-token")
            .header("Content-Type", "application/json")
            .body(Body::from("{}"))
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
    }
}

/// 인증 의존성을 끊기 위해 제거한 지갑 핸들러가 공개 API로 다시 연결되지 않아야 한다.
#[tokio::test]
async fn retired_wallet_routes_return_not_found() {
    let (router, _engine) = app().await;
    for (method, path) in [
        (Method::POST, "/api/wallets"),
        (Method::GET, "/api/wallets/1"),
        (Method::GET, "/api/wallets/my"),
        (Method::GET, "/api/wallets/1/balance"),
        (Method::POST, "/api/wallets/1/transfer"),
        (Method::GET, "/api/wallets/transaction/example"),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
    }
}

/// 인증 도메인의 삭제가 거래 명령 라우트까지 없애지 않았는지 확인한다.
/// 필수 필드가 없는 JSON은 Axum에서 422로 거부된다. 경로가 사라지면 404가 된다.
#[tokio::test]
async fn trading_routes_still_validate_request_bodies() {
    let (router, _engine) = app().await;
    for path in ["/api/cex/orders", "/api/cex/balances/sync"] {
        let request = Request::post(path)
            .header("x-engine-service-token", "isolated-engine-service-token-32-bytes")
            .header("Content-Type", "application/json")
            .body(Body::from("{}"))
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "{path}"
        );
    }
}

/// 취소 명령에 사용자와 거래쌍이 없으면 코어에 전달하기 전에 거부한다.
#[tokio::test]
async fn cancellation_still_requires_user_id() {
    let (router, _engine) = app().await;
    let response = router
        .oneshot(
            Request::delete("/api/cex/orders/1")
                .header("x-engine-service-token", "isolated-engine-service-token-32-bytes")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"], "Invalid cancellation query: user_id, base_mint and quote_mint are required");
}

#[tokio::test]
async fn output_failure_closed_handle_reports_unavailable_health() {
    let mut engine = HighPerformanceEngine::new_bench();
    let handle = engine.handle();
    engine.stop().await.unwrap();
    let response = app_with_engine(handle)
        .oneshot(
            Request::builder()
                .uri("/api/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

/// 사용자 JWT나 감사 키는 플랫폼 명령 키를 대신할 수 없다. 본문 파싱보다 먼저 차단한다.
#[tokio::test]
async fn command_boundary_requires_platform_identity() {
    let (router, _engine) = app().await;
    for key in [None, Some("wrong-service-token")] {
        let mut request = Request::post("/api/cex/orders")
            .header("Authorization", "Bearer customer-token")
            .header("x-engine-audit-token", "isolated-engine-service-token-32-bytes")
            .header("Content-Type", "application/json");
        if let Some(key) = key { request = request.header("x-engine-service-token", key); }
        let response = router.clone().oneshot(request.body(Body::from("{}")).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
