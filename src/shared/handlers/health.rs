// =====================================================
// Health Check Handler
// =====================================================
// 역할: 서버 상태 확인 API
//
// 엔드포인트: GET /api/health
// 응답: 200 OK with JSON body
// =====================================================

use crate::shared::services::AppState;
use axum::{extract::State, http::StatusCode, response::Json};
use serde::Serialize;
use utoipa::ToSchema;

/// 헬스체크 응답 모델
/// Health check response model
#[derive(Debug, Serialize, ToSchema)]
pub struct HealthResponse {
    /// 서버 상태
    /// Server status
    #[schema(example = "ok")]
    pub status: String,

    /// 서버 메시지
    /// Server message
    #[schema(example = "Server is running")]
    pub message: String,
}

/// 헬스체크 핸들러
/// Health check handler
///
/// 서버가 정상적으로 실행 중인지 확인하는 엔드포인트입니다.
///
/// # Response
/// - 200: 서버 정상 작동
#[utoipa::path(
    get,
    path = "/api/health",
    responses(
        (status = 200, description = "Server is healthy", body = HealthResponse),
        (status = 503, description = "Engine processing is closed", body = HealthResponse)
    ),
    tag = "Health"
)]
pub async fn health_check(State(state): State<AppState>) -> (StatusCode, Json<HealthResponse>) {
    // 출력 실패나 정지 뒤에도 정상 응답을 주어 운영자가 거래 가능하다고
    // 판단하지 않도록 한다. 외부 브로커 전체의 가용성 검사는 별도다.
    if !state.engine.is_accepting() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(HealthResponse {
                status: "unavailable".into(),
                message: "Engine processing is closed".into(),
            }),
        );
    }
    (
        StatusCode::OK,
        Json(HealthResponse {
            status: "ok".into(),
            message: "Server is running".into(),
        }),
    )
}
