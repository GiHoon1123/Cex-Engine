use axum::Router;
use tokio::net::TcpListener;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

// 실행하는 도메인과 HTTP 경계를 조립한다.
mod domains;
mod routes;
mod shared;

use crate::shared::database::Database;
use crate::shared::services::AppState;
use routes::create_router;

// 실제 공개하는 거래 API 모델만 스키마에 등록한다.
use crate::domains::cex::models::*;

// OpenAPI 스키마 정의: Swagger 문서 자동 생성 (최소화: 주문 생성/취소/헬스체크만)
#[derive(OpenApi)]
#[openapi(
    paths(
        crate::shared::handlers::health::health_check,
        crate::domains::cex::handlers::order_handler::create_order,
        crate::domains::cex::handlers::order_handler::cancel_order,
    ),
    components(schemas(
        Order,
        CreateOrderRequest,
        crate::domains::cex::services::order_receipt::OrderAcceptance,
        crate::shared::handlers::health::HealthResponse,
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

    // DB 연결
    // Docker 컨테이너 환경에서는 환경 변수로 호스트 지정 가능
    // 기본값: 프로덕션은 컨테이너 이름, 개발은 localhost
    let db_host = std::env::var("DATABASE_HOST").unwrap_or_else(|_| {
        if std::env::var("RUST_ENV").unwrap_or_else(|_| "dev".to_string()) == "prod" {
            "cex-postgres".to_string() // Docker 컨테이너 이름
        } else {
            "localhost".to_string() // 로컬 개발 환경
        }
    });
    let db_url = format!("postgresql://root:1234@{}/cex", db_host);
    let db = Database::lazy(&db_url).expect("Failed to connect to database");

    // DB 초기화 (마이그레이션) - 주문 생성/취소에 필요
    // 마이그레이션 체크섬 불일치 문제로 인해 일단 주석 처리 (테스트용)
    // db.initialize()
    //     .await
    //     .expect("Failed to initialize database");
    eprintln!("[Main] Skipping database migrations (test mode)");

    // 생명주기 제어 객체는 main에 남긴다. 검증 후 거래 상태를 워커로 이동하고,
    // HTTP·UDP에는 복제 가능한 송신 핸들만 주입한다.
    let mut engine = crate::domains::cex::engine::runtime::HighPerformanceEngine::new(db.clone());
    // 고객 명령과 내부 감사 조회를 조립한다. 내부 키는 고객 인증과 별도로 주입한다.
    let audit = crate::domains::cex::engine::runtime::audit::AuditReader::new(
        engine.journal_directory(),
        std::env::var("ENGINE_AUDIT_TOKEN").ok(),
    ).expect("잘못된 감사 조회 설정");
    engine.start().await.expect("Failed to start engine");
    let app_state = AppState::new(db.clone(), engine.handle());

    let app = Router::new()
        .merge(create_router(&service_token))
        // Swagger UI 활성화
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .with_state(app_state)
        .layer(axum::Extension(audit));

    // 기본은 내부 루프백이다. 컨테이너 배포·격리 실행 검증에서만 명시적으로 주소를 준다.
    let http_address =
        std::env::var("ENGINE_HTTP_ADDRESS").unwrap_or_else(|_| "127.0.0.1:3002".into());
    let listener = TcpListener::bind(&http_address).await.unwrap();

    eprintln!("[Main] Server running on http://{}", http_address);
    eprintln!("[Main] Database: PostgreSQL (cex)");

    // 서버 실행
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
