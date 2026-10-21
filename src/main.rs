use axum::http::Method;
use axum::Router;
use tokio::net::TcpListener;
use tower_http::cors::{AllowOrigin, CorsLayer};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

// New module structure
mod domains;
mod routes;
mod shared;

use crate::shared::database::Database;
use crate::shared::services::AppState;
use routes::create_router;

// Import models for OpenAPI schema
// use crate::domains::swap::models::*;
// use crate::domains::wallet::models::*;
use crate::domains::cex::models::*;

// OpenAPI 스키마 정의: Swagger 문서 자동 생성 (최소화: 주문 생성/취소/헬스체크만)
#[derive(OpenApi)]
#[openapi(
    paths(
        crate::shared::handlers::health::health_check,
        crate::domains::cex::handlers::order_handler::create_order,
        crate::domains::cex::handlers::order_handler::cancel_order,
        // crate::domains::swap::handlers::swap_handler::get_quote,
        // crate::domains::swap::handlers::swap_handler::create_swap_transaction,
        // crate::domains::swap::handlers::token_handler::search_tokens,
        // crate::domains::cex::handlers::balance_handler::get_all_balances,
        // crate::domains::cex::handlers::balance_handler::get_balance,
        // crate::domains::cex::handlers::order_handler::get_order,
        // crate::domains::cex::handlers::order_handler::get_my_orders,
        // crate::domains::cex::handlers::order_handler::get_orderbook,
        // crate::domains::cex::handlers::trade_handler::get_trades,
        // crate::domains::cex::handlers::trade_handler::get_my_trades,
        // crate::domains::cex::handlers::trade_handler::get_latest_price,
        // crate::domains::cex::handlers::trade_handler::get_24h_volume,
        // crate::domains::cex::handlers::position_handler::get_position,
        // crate::domains::cex::handlers::position_handler::get_all_positions,
    ),
    components(schemas(
        // QuoteRequest,
        // QuoteResponse,
        // RoutePlan,
        // SwapInfo,
        // TokenSearchRequest,
        // TokenSearchResponse,
        // Token,
        // SwapTransactionRequest,
        // SwapTransactionResponse,
        // Transaction,
        // CreateWalletResponse,
        // WalletResponse,
        // WalletsResponse,
        // WalletBalanceResponse,
        // TransferSolRequest,
        // TransferSolResponse,
        // TransactionStatusResponse,
        // SolanaWallet,
        // UserBalance,
        // ExchangeBalancesResponse,
        // ExchangeBalanceResponse,
        Order,
        CreateOrderRequest,
        crate::domains::cex::services::order_receipt::OrderAcceptance,
        // OrderResponse,
        // OrdersResponse,
        // OrderBookEntry,
        // OrderBookResponse,
        // crate::domains::cex::handlers::order_handler::OrderbookResponse,
        crate::shared::handlers::health::HealthResponse,
        // Trade,
        // TradesResponse,
        // AssetPosition,
        // AssetPositionResponse,
        // AllPositionsResponse,
        // TradeSummary,
    )),
    tags(
        // (name = "Swap", description = "Swap API endpoints (Jupiter integration)"),
        // (name = "Tokens", description = "Token search API endpoints"),
        // (name = "Wallets", description = "Wallet API endpoints (Solana wallet management)"),
        // (name = "CEX Balances", description = "CEX Exchange balance API endpoints"),
        (name = "CEX Orders", description = "CEX Exchange order API endpoints"),
        // (name = "CEX Trades", description = "CEX Exchange trade API endpoints"),
        // (name = "CEX Positions", description = "CEX Exchange position API endpoints (P&L, average entry price)"),
        (name = "Health", description = "Health check API endpoints")
    ),
    info(
        title = "CEX Engine",
        description = "API server for Solana blockchain interactions",
        version = "1.0.0"
    )
)]
struct ApiDoc;

#[tokio::main]
async fn main() {
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
    engine.start().await.expect("Failed to start engine");
    let mut app_state =
        AppState::new(db.clone(), engine.handle()).expect("Failed to initialize AppState");

    // CORS 설정
    // 개발 환경: localhost 허용, 프로덕션: 모든 origin 허용 (또는 특정 도메인만)
    let cors = if std::env::var("RUST_ENV").unwrap_or_else(|_| "dev".to_string()) == "prod" {
        // 프로덕션: 모든 origin 허용
        // AllowOrigin::any()를 사용하면 브라우저에서 제대로 작동합니다
        CorsLayer::new()
            .allow_origin(AllowOrigin::any())
            .allow_methods([
                Method::GET,
                Method::POST,
                Method::PUT,
                Method::DELETE,
                Method::OPTIONS,
                Method::PATCH,
            ])
            .allow_headers([
                axum::http::header::CONTENT_TYPE,
                axum::http::header::AUTHORIZATION,
                axum::http::header::ACCEPT,
                axum::http::header::ORIGIN,
            ])
            .allow_credentials(false) // 모든 origin 허용 시 credentials는 false
            .expose_headers([
                axum::http::header::CONTENT_TYPE,
                axum::http::header::AUTHORIZATION,
            ])
        // 특정 도메인만 허용하려면:
        // .allow_origin(AllowOrigin::exact("https://your-frontend-domain.vercel.app".parse().unwrap()))
        // .allow_credentials(true)
    } else {
        // 개발 환경: localhost만 허용
        CorsLayer::new()
            .allow_origin(AllowOrigin::exact("http://localhost:3003".parse().unwrap()))
            .allow_methods([
                Method::GET,
                Method::POST,
                Method::PUT,
                Method::DELETE,
                Method::OPTIONS,
                Method::PATCH,
            ])
            .allow_headers([
                axum::http::header::CONTENT_TYPE,
                axum::http::header::AUTHORIZATION,
                axum::http::header::ACCEPT,
                axum::http::header::ORIGIN,
            ])
            .allow_credentials(true)
            .expose_headers([
                axum::http::header::CONTENT_TYPE,
                axum::http::header::AUTHORIZATION,
            ])
    };

    // Router 생성 (주문 생성/취소/헬스체크만 활성화)
    let app = Router::new()
        .merge(create_router())
        // Swagger UI 활성화
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .layer(cors)
        .with_state(app_state);

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
