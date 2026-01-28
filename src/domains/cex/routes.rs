use axum::{
    routing::{get, post, delete},
    Router,
};
use crate::shared::services::AppState;

use super::handlers;

/// CEX 라우터 생성
/// Create CEX router
/// 
/// 거래소 관련 모든 API 엔드포인트를 등록합니다.
/// 
/// # Routes
/// 
/// ## Orders (주문)
/// - `POST   /api/cex/orders` - 주문 생성
/// - `DELETE /api/cex/orders/:id` - 주문 취소
/// - `GET    /api/cex/orders/:id` - 주문 조회
/// - `GET    /api/cex/orders/my` - 내 주문 목록
/// - `GET    /api/cex/orderbook` - 오더북 조회
/// 
/// ## Trades (체결)
/// - `GET    /api/cex/trades` - 거래쌍별 체결 내역
/// - `GET    /api/cex/trades/my` - 내 체결 내역
/// - `GET    /api/cex/price` - 최근 체결 가격
/// - `GET    /api/cex/volume` - 24시간 거래량
/// 
/// ## Balances (잔고)
/// - `GET    /api/cex/balances` - 내 잔고 조회
/// - `POST   /api/cex/balances` - 잔고 초기화
/// 
/// ## Positions (포지션)
/// - `GET    /api/cex/positions` - 모든 자산 포지션 조회
/// - `GET    /api/cex/positions/:mint` - 특정 자산 포지션 조회
pub fn create_cex_router() -> Router<AppState> {
    Router::new()
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // Orders (주문) - 주문 생성/취소만 활성화
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        // 주문 생성
        .route("/orders", post(handlers::create_order))
        
        // 주문 취소
        .route("/orders/:order_id", delete(handlers::cancel_order))
        
        // 나머지 주문 관련 엔드포인트 주석 처리
        // .route("/orders/:order_id", get(handlers::get_order))
        // .route("/orders/my", get(handlers::get_my_orders))
        // .route("/orderbook", get(handlers::get_orderbook))
        
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // Trades (체결) - 주석 처리
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // .route("/trades", get(handlers::get_trades))
        // .route("/trades/my", get(handlers::get_my_trades))
        // .route("/price", get(handlers::get_latest_price))
        // .route("/volume", get(handlers::get_24h_volume))
        
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // Balances (잔고) - 주석 처리
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // .route("/balances", get(handlers::get_all_balances))
        // .route("/balances/:mint", get(handlers::get_balance))
        
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // Positions (포지션) - 주석 처리
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // .route("/positions", get(handlers::get_all_positions))
        // .route("/positions/:mint", get(handlers::get_position))
}
