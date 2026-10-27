use axum::Router;
use tokio::net::TcpListener;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use cex_engine::routes::create_router;
use cex_engine::shared::database::Database;
use cex_engine::shared::services::AppState;

// 실제 공개하는 거래 API 모델만 스키마에 등록한다.
use cex_engine::domains::cex::models::*;

// 주문 생성·취소·상태 확인에 필요한 공개 스키마만 문서에 포함한다.
#[derive(OpenApi)]
#[openapi(
    paths(
        cex_engine::shared::handlers::health::health_check,
        cex_engine::domains::cex::handlers::order_handler::create_order,
        cex_engine::domains::cex::handlers::order_handler::cancel_order,
    ),
    components(schemas(
        Order,
        CreateOrderRequest,
        cex_engine::domains::cex::services::order_receipt::OrderAcceptance,
        cex_engine::shared::handlers::health::HealthResponse,
    )),
    tags(
        (name = "CEX Orders", description = "CEX Exchange order API endpoints"),
        (name = "Health", description = "Health check API endpoints")
    ),
    info(
        title = "CEX Engine",
        description = "단일 스레드 주문·체결 엔진",
        version = "1.0.0"
    )
)]
struct ApiDoc;

#[tokio::main]
async fn main() {
    // 필수 서비스 신원을 엔진 워커 시작 전에 검사한다. 설정 누락 시 명령 수신을 시작하지 않는다.
    let service_token = std::env::var("ENGINE_SERVICE_TOKEN").expect("ENGINE_SERVICE_TOKEN이 필요합니다");
    assert!((32..=1024).contains(&service_token.len()), "잘못된 서비스 키 길이");

    // 플랫폼 디비는 주문 접수 어댑터가 지연 연결한다. 접속 정보에는 자격 증명이
    // 포함되므로 호스트 조각을 조립하거나 개발용 비밀번호로 대체하지 않는다.
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL이 필요합니다");
    let db = Database::lazy(&database_url).expect("DATABASE_URL 형식이 올바르지 않습니다");

    // 생명주기 제어 객체는 main에 남긴다. 검증 후 거래 상태를 워커로 이동하고,
    // HTTP·UDP에는 복제 가능한 송신 핸들만 주입한다.
    let mut engine = cex_engine::domains::cex::engine::runtime::HighPerformanceEngine::new(db.clone());
    // 고객 명령과 내부 감사 조회를 조립한다. 내부 키는 고객 인증과 별도로 주입한다.
    let audit = cex_engine::domains::cex::engine::runtime::audit::AuditReader::new(
        engine.journal_directory(),
        std::env::var("ENGINE_AUDIT_TOKEN").ok(),
    ).expect("잘못된 감사 조회 설정");
    engine.start().await.expect("Failed to start engine");
    let app_state = AppState::new(db.clone(), engine.handle());

    let app = Router::new()
        .merge(create_router(&service_token))
        // 내부 운영자가 현재 계약을 확인할 수 있도록 문서 화면을 함께 제공한다.
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .with_state(app_state)
        .layer(axum::Extension(audit));

    // 기본은 내부 루프백이다. 컨테이너 배포·격리 실행 검증에서만 명시적으로 주소를 준다.
    let http_address =
        std::env::var("ENGINE_HTTP_ADDRESS").unwrap_or_else(|_| "127.0.0.1:3002".into());
    let listener = TcpListener::bind(&http_address).await.unwrap();

    eprintln!("엔진 서버가 http://{} 에서 실행 중입니다", http_address);

    // 종료 신호가 오면 신규 접수를 막고 출력 워커까지 회수한다.
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("HTTP server failed");
    engine
        .stop()
        .await
        .expect("Engine shutdown left pending work");
}

/// 종료 신호를 받으면 HTTP 신규 접수를 닫고 진행 중 요청을 마친 뒤
/// 엔진의 저널·출력 워커를 순서대로 회수한다. 강제 종료는 로그 재생으로 복구한다.
async fn shutdown_signal() {
    let interrupt = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Signal handler failed");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Termination handler failed")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! { _ = interrupt => {}, _ = terminate => {} }
}
