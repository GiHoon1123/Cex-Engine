//! 엔진의 활성 주문·원본 식별 입금 명령만 등록한다. 조회는 플랫폼과 독립 확정 저널 경계를 사용한다.
use super::handlers;
use crate::shared::services::AppState;
use axum::{
    routing::{delete, post},
    Router,
};

/// 서비스 신원 검증은 상위 라우터에서 적용한다. 잔액 변화량만 받는 외부 경로는 없다.
pub fn create_cex_router() -> Router<AppState> {
    Router::new()
        // 고정된 작은 요청 한도는 이 금융 경계에만 적용한다. 기존 주문의 크기 계약을 바꾸지 않는다.
        .route("/deposits/credits", post(handlers::credit_deposit).layer(axum::extract::DefaultBodyLimit::max(4096)))
        .route("/orders", post(handlers::create_order))
        .route("/orders/:order_id", delete(handlers::cancel_order))
}
