// CEX Balance Handler
// 거래소 잔고 핸들러
// 역할: 잔고 동기화 API 엔드포인트 처리 (조회는 Java에서 처리)

use crate::shared::services::AppState;
use crate::domains::cex::engine::Engine;
use axum::{extract::State, http::StatusCode, Json};

/// 잔고 동기화 핸들러
/// Sync balance handler
/// 
/// Java API 서버에서 잔고 업데이트 시 엔진 메모리 잔고를 동기화합니다.
/// 
/// 경로: POST /api/cex/balances/sync
/// 인증: 불필요 (내부 서비스 간 통신)
/// 
/// # Request Body
/// ```json
/// {
///   "user_id": 123,
///   "mint": "USDT",
///   "available_delta": 100.0
/// }
/// ```
/// 
/// # Returns
/// * `200 OK` - 동기화 성공
/// * `400 Bad Request` - 잘못된 요청
/// * `500 Internal Server Error` - 서버 오류
#[utoipa::path(
    post,
    path = "/api/cex/balances/sync",
    request_body = SyncBalanceRequest,
    responses(
        (status = 200, description = "Balance synced successfully"),
        (status = 400, description = "Bad request"),
        (status = 500, description = "Internal server error")
    ),
    tag = "CEX Balances"
)]
pub async fn sync_balance(
    State(app_state): State<AppState>,
    Json(request): Json<SyncBalanceRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<serde_json::Value>)> {
    // 엔진에 잔고 업데이트 요청
    let engine = app_state.engine.lock().await;
    
    engine.update_balance(
        request.user_id,
        &request.mint,
        request.available_delta,
    )
    .await
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": format!("Failed to sync balance: {}", e)
            })),
        )
    })?;

    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "message": "Balance synced successfully",
            "user_id": request.user_id,
            "mint": request.mint,
            "available_delta": request.available_delta
        })),
    ))
}

/// 잔고 동기화 요청
#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
pub struct SyncBalanceRequest {
    /// 사용자 ID
    pub user_id: u64,
    /// 자산 종류 (예: "SOL", "USDT")
    pub mint: String,
    /// available 증감량 (양수: 입금, 음수: 출금)
    pub available_delta: rust_decimal::Decimal,
}

