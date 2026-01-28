use axum::Router;
use axum::http::Method;
use tokio::net::TcpListener;
use tower_http::cors::{CorsLayer, AllowOrigin};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

// New module structure
mod domains;
mod shared;
mod routes;

use routes::create_router;
use crate::shared::database::Database;
use crate::shared::services::AppState;

// Import models for OpenAPI schema
// use crate::domains::swap::models::*;
// use crate::domains::auth::models::*;
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
        // crate::domains::auth::handlers::auth_handler::signup,
        // crate::domains::auth::handlers::auth_handler::signin,
        // crate::domains::auth::handlers::auth_handler::refresh,
        // crate::domains::auth::handlers::auth_handler::logout,
        // crate::domains::auth::handlers::auth_handler::get_me,
        // crate::domains::wallet::handlers::wallet_handler::create_wallet,
        // crate::domains::wallet::handlers::wallet_handler::get_wallet,
        // crate::domains::wallet::handlers::wallet_handler::get_user_wallets,
        // crate::domains::wallet::handlers::wallet_handler::get_balance,
        // crate::domains::wallet::handlers::wallet_handler::transfer_sol,
        // crate::domains::wallet::handlers::wallet_handler::get_transaction_status,
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
        // SignupRequest,
        // SignupResponse,
        // SigninRequest,
        // SigninResponse,
        // RefreshTokenRequest,
        // RefreshTokenResponse,
        // LogoutRequest,
        // UserResponse,
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
    // modifiers(
    //     &SecurityAddon
    // ),
    tags(
        // (name = "Swap", description = "Swap API endpoints (Jupiter integration)"),
        // (name = "Tokens", description = "Token search API endpoints"),
        // (name = "Auth", description = "Authentication API endpoints"),
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

// Security scheme 정의: Swagger UI에서 "Authorize" 버튼 추가
// struct SecurityAddon;
// 
// impl utoipa::Modify for SecurityAddon {
//     fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
//         if let Some(components) = openapi.components.as_mut() {
//             components.add_security_scheme(
//                 "BearerAuth",
//                 utoipa::openapi::security::SecurityScheme::Http(
//                     utoipa::openapi::security::HttpBuilder::new()
//                         .scheme(utoipa::openapi::security::HttpAuthScheme::Bearer)
//                         .bearer_format("JWT")
//                         .build(),
//                 ),
//             )
//         }
//     }
// }

#[tokio::main]
async fn main() {
    // DB 연결
    // Docker 컨테이너 환경에서는 환경 변수로 호스트 지정 가능
    // 기본값: 프로덕션은 컨테이너 이름, 개발은 localhost
    let db_host = std::env::var("DATABASE_HOST").unwrap_or_else(|_| {
        if std::env::var("RUST_ENV").unwrap_or_else(|_| "dev".to_string()) == "prod" {
            "cex-postgres".to_string()  // Docker 컨테이너 이름
        } else {
            "localhost".to_string()     // 로컬 개발 환경
        }
    });
    let db_url = format!("postgresql://root:1234@{}/cex", db_host);
    let db = Database::new(&db_url)
        .await
        .expect("Failed to connect to database");

    // DB 초기화 (마이그레이션) - 주문 생성/취소에 필요
    // 마이그레이션 체크섬 불일치 문제로 인해 일단 주석 처리 (테스트용)
    // db.initialize()
    //     .await
    //     .expect("Failed to initialize database");
    eprintln!("[Main] Skipping database migrations (test mode)");

    // AppState 생성 (모든 Service 초기화)
    let mut app_state = AppState::new(db.clone())
        .expect("Failed to initialize AppState");
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 엔진 시작 (주문 처리에 필요)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    eprintln!("[Main] Starting engine...");
    {
        let mut engine_guard = app_state.engine.lock().await;
        engine_guard.start().await
            .expect("Failed to start engine");
    }
    eprintln!("[Main] Engine started successfully");
    

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
        .merge(
            SwaggerUi::new("/swagger-ui")
                .url("/api-docs/openapi.json", ApiDoc::openapi())
        )
        .layer(cors)
        .with_state(app_state);

    // 서버 시작: 3002 포트에서 리스닝
    let listener = TcpListener::bind("0.0.0.0:3002")
        .await
        .unwrap();
    
    eprintln!("[Main] Server running on http://localhost:3002");
    eprintln!("[Main] Swagger UI available at http://localhost:3002/api");
    eprintln!("[Main] Database: PostgreSQL (cex)");
    
    // 서버 실행
    axum::serve(listener, app)
        .await
        .unwrap();
}
