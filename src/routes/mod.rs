// Routes module: 라우팅 설정
// 역할: 모든 도메인의 라우터를 조합
// Routes module: combines all domain routers

use axum::{Router, routing::get};
use crate::shared::services::AppState;
use crate::shared::handlers::health::health_check;

// 각 도메인의 routes import (주문 생성/취소만 활성화)
// use crate::domains::auth::routes::create_auth_router;
// use crate::domains::wallet::routes::create_wallet_router;
// use crate::domains::swap::routes::{create_swap_router, create_tokens_router};
use crate::domains::cex::routes::create_cex_router;
// use crate::domains::bot::routes::create_bot_router;

/// Create main router (combines all domain routers)
/// 메인 라우터 생성 (주문 생성/취소/헬스체크만 활성화)
pub fn create_router() -> Router<AppState> {
    Router::new()
        // 헬스체크 엔드포인트 (인증 불필요)
        .route("/api/health", get(health_check))
        // 주문 생성/취소 엔드포인트
        .nest("/api/cex", create_cex_router())
        // 나머지 라우터 주석 처리
        // .nest("/api/auth", create_auth_router())
        // .nest("/api/wallets", create_wallet_router())
        // .nest("/api/swap", create_swap_router())
        // .nest("/api/tokens", create_tokens_router())
        // .nest("/api/bot", create_bot_router())
}
