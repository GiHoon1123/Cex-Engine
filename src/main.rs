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
use crate::domains::swap::models::*;
use crate::domains::auth::models::*;
use crate::domains::wallet::models::*;
use crate::domains::cex::models::*;

// OpenAPI 스키마 정의: Swagger 문서 자동 생성
#[derive(OpenApi)]
#[openapi(
    paths(
        crate::shared::handlers::health::health_check,
        crate::domains::swap::handlers::swap_handler::get_quote,
        crate::domains::swap::handlers::swap_handler::create_swap_transaction,
        crate::domains::swap::handlers::token_handler::search_tokens,
        crate::domains::auth::handlers::auth_handler::signup,
        crate::domains::auth::handlers::auth_handler::signin,
        crate::domains::auth::handlers::auth_handler::refresh,
        crate::domains::auth::handlers::auth_handler::logout,
        crate::domains::auth::handlers::auth_handler::get_me,
        crate::domains::wallet::handlers::wallet_handler::create_wallet,
        crate::domains::wallet::handlers::wallet_handler::get_wallet,
        crate::domains::wallet::handlers::wallet_handler::get_user_wallets,
        crate::domains::wallet::handlers::wallet_handler::get_balance,
        crate::domains::wallet::handlers::wallet_handler::transfer_sol,
        crate::domains::wallet::handlers::wallet_handler::get_transaction_status,
        crate::domains::cex::handlers::balance_handler::get_all_balances,
        crate::domains::cex::handlers::balance_handler::get_balance,
        crate::domains::cex::handlers::order_handler::create_order,
        crate::domains::cex::handlers::order_handler::cancel_order,
        crate::domains::cex::handlers::order_handler::get_order,
        crate::domains::cex::handlers::order_handler::get_my_orders,
        crate::domains::cex::handlers::order_handler::get_orderbook,
        crate::domains::cex::handlers::trade_handler::get_trades,
        crate::domains::cex::handlers::trade_handler::get_my_trades,
        crate::domains::cex::handlers::trade_handler::get_latest_price,
        crate::domains::cex::handlers::trade_handler::get_24h_volume,
        crate::domains::cex::handlers::position_handler::get_position,
        crate::domains::cex::handlers::position_handler::get_all_positions,
        crate::domains::bot::handlers::bot_handler::delete_bot_data,
        crate::domains::bot::handlers::bot_handler::get_cleanup_scheduler_status,
        crate::domains::bot::handlers::bot_handler::enable_cleanup_scheduler,
        crate::domains::bot::handlers::bot_handler::disable_cleanup_scheduler
    ),
    components(schemas(
        QuoteRequest,
        QuoteResponse,
        RoutePlan,
        SwapInfo,
        TokenSearchRequest,
        TokenSearchResponse,
        Token,
        SwapTransactionRequest,
        SwapTransactionResponse,
        Transaction,
        SignupRequest,
        SignupResponse,
        SigninRequest,
        SigninResponse,
        RefreshTokenRequest,
        RefreshTokenResponse,
        LogoutRequest,
        UserResponse,
        CreateWalletResponse,
        WalletResponse,
        WalletsResponse,
        WalletBalanceResponse,
        TransferSolRequest,
        TransferSolResponse,
        TransactionStatusResponse,
        SolanaWallet,
        UserBalance,
        ExchangeBalancesResponse,
        ExchangeBalanceResponse,
        Order,
        CreateOrderRequest,
        OrderResponse,
        OrdersResponse,
        OrderBookEntry,
        OrderBookResponse,
        crate::domains::cex::handlers::order_handler::OrderbookResponse,
        crate::shared::handlers::health::HealthResponse,
        Trade,
        TradesResponse,
        AssetPosition,
        AssetPositionResponse,
        AllPositionsResponse,
        TradeSummary,
        crate::domains::bot::handlers::bot_handler::DeleteBotDataRequest,
        crate::domains::bot::handlers::bot_handler::DeleteBotDataResponse
    )),
    modifiers(
        &SecurityAddon
    ),
    tags(
        (name = "Swap", description = "Swap API endpoints (Jupiter integration)"),
        (name = "Tokens", description = "Token search API endpoints"),
        (name = "Auth", description = "Authentication API endpoints"),
        (name = "Wallets", description = "Wallet API endpoints (Solana wallet management)"),
        (name = "CEX Balances", description = "CEX Exchange balance API endpoints"),
        (name = "CEX Orders", description = "CEX Exchange order API endpoints"),
        (name = "CEX Trades", description = "CEX Exchange trade API endpoints"),
        (name = "CEX Positions", description = "CEX Exchange position API endpoints (P&L, average entry price)"),
        (name = "Bot", description = "Bot management API endpoints (delete bot data)"),
        (name = "Health", description = "Health check API endpoints")
    ),
    info(
        title = "Solana API Server",
        description = "API server for Solana blockchain interactions",
        version = "1.0.0"
    )
)]
struct ApiDoc;

// Security scheme 정의: Swagger UI에서 "Authorize" 버튼 추가
struct SecurityAddon;

impl utoipa::Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "BearerAuth",
                utoipa::openapi::security::SecurityScheme::Http(
                    utoipa::openapi::security::HttpBuilder::new()
                        .scheme(utoipa::openapi::security::HttpAuthScheme::Bearer)
                        .bearer_format("JWT")
                        .build(),
                ),
            )
        }
    }
}

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

    db.initialize()
        .await
        .expect("Failed to initialize database");

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // ID 생성기 초기화 (메모리 기반, DB 접근 없음)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    use crate::shared::utils::id_generator::{OrderIdGenerator, TradeIdGenerator};
    
    // ID 생성기 초기화 (타임스탬프 기반)
    OrderIdGenerator::initialize();
    TradeIdGenerator::initialize();
    
    eprintln!("[Main] ID generators initialized (timestamp-based, no DB access)");

    // AppState 생성 (모든 Service 초기화)
    let mut app_state = AppState::new(db.clone())
        .expect("Failed to initialize AppState");
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 봇 준비 (엔진 시작 전 - 계정 생성 및 데이터 삭제)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    eprintln!("[Main] Preparing bots (before engine start)...");
    
    use crate::domains::bot::models::BotConfig;
    use crate::domains::bot::services::{
        BotManager, BinanceClient, OrderbookSync,
    };
    use crate::domains::cex::services::order_service::OrderService;
    
    // 봇 설정 로드
    eprintln!("[Main] Loading bot config from environment...");
    let bot_config = BotConfig::from_env();
    eprintln!("[Main] Bot config loaded: ws_url={}, symbol={}, depth={}, quantity={}", 
              bot_config.binance_ws_url, bot_config.binance_symbol, 
              bot_config.orderbook_depth, bot_config.order_quantity);
    
    // 봇 관리자 생성 (엔진 참조는 필요하지만 아직 사용하지 않음)
    eprintln!("[Main] Creating BotManager...");
    let mut bot_manager = BotManager::new(
        db.clone(),
        app_state.engine.clone(),
        bot_config.clone(),
    );
    
    // 봇 계정 확인/생성 및 데이터 삭제 (엔진 불필요)
    eprintln!("[Main] Preparing bots (account creation and data cleanup)...");
    bot_manager.prepare_bots().await
        .expect("Failed to prepare bots");
    
    eprintln!("[Main] Bots prepared: bot1@bot.com (buy), bot2@bot.com (sell)");
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 봇 잔고를 DB에 직접 쓰기 (엔진 시작 전)
    // 엔진 시작 시 DB에서 자동으로 로드됩니다
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    eprintln!("[Main] Setting bot balances in database (before engine start)...");
    bot_manager.set_bot_balances_in_db().await
        .expect("Failed to set bot balances in DB");
    
    eprintln!("[Main] Bot balances set in database");
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 엔진 시작 (DB에서 데이터 로드 및 스레드 시작)
    // 봇 데이터 삭제 후 실행되므로 활성 주문 수가 크게 줄어듭니다
    // 봇 잔고는 이미 DB에 있으므로 엔진 시작 시 자동으로 로드됩니다
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    eprintln!("[Main] Starting engine (will load bot balances from DB)...");
    {
        let mut engine_guard = app_state.engine.lock().await;
        engine_guard.start().await
            .expect("Failed to start engine");
    }
    
    eprintln!("[Main] Engine started successfully (bot balances loaded from DB)");
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // UDP 오더북 피드 시작 (UDP 멀티캐스트)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    eprintln!("[Main] Starting UDP orderbook feed...");
    let udp_feed = crate::domains::cex::services::UdpOrderbookFeed::new(
        app_state.engine.clone(),
        None, // 기본 설정 사용 (224.0.0.1:5000, 100ms 간격)
    ).await.expect("Failed to create UDP orderbook feed");
    
    udp_feed.start();
    eprintln!("[Main] UDP orderbook feed started: multicast=224.0.0.1:5000, interval=100ms");
    
    // 바이낸스 클라이언트 생성
    let binance_client = BinanceClient::new(bot_config.binance_ws_url.clone());
    
    // 주문 서비스 생성
    let order_service = OrderService::new(
        db.clone(),
        app_state.engine.clone(),
    );
    
    // 오더북 동기화 서비스 생성
    let mut orderbook_sync = OrderbookSync::new(
        bot_manager.clone(),
        order_service.clone(),
        binance_client,
        db.clone(),
    );
    
    // 오더북 동기화 시작 (백그라운드 태스크)
    eprintln!("[Main] Starting orderbook synchronization...");
    tokio::spawn(async move {
        eprintln!("[Main] Orderbook sync task started");
        if let Err(e) = orderbook_sync.start().await {
            eprintln!("[Main] Orderbook sync error: {}", e);
        }
    });
    
    eprintln!("[Main] Bot orderbook synchronization started");
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 봇 데이터 정리 스케줄러 설정 및 시작
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    eprintln!("[Main] Setting up bot cleanup scheduler...");
    {
        let bot1_user_id = bot_manager.bot1_user_id();
        let bot2_user_id = bot_manager.bot2_user_id();
        
        // AppState에 스케줄러 설정 및 시작
        app_state.setup_bot_cleanup_scheduler(bot1_user_id, bot2_user_id);
        
        eprintln!("[Main] Bot cleanup scheduler started (disabled by default, use API to enable)");
    }

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

    // Router 생성
    let app = Router::new()
        .merge(create_router())
        .merge(
            SwaggerUi::new("/api")
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
