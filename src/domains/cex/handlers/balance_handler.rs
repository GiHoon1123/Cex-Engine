// CEX Balance Handler
// 거래소 잔고 핸들러
// 역할: 잔고 조회 API 엔드포인트 처리

use crate::shared::services::AppState;
use crate::shared::middleware::auth::AuthenticatedUser;
use crate::domains::cex::models::balance::{ExchangeBalancesResponse, ExchangeBalanceResponse};
use crate::domains::cex::engine::Engine;
use axum::{extract::{Path, State}, http::StatusCode, Json};

/// 사용자의 모든 잔고 조회 핸들러
/// Get all balances for authenticated user
/// 
/// 경로: GET /api/cex/balances
/// 인증: 필요 (JWT 토큰)
/// 
/// # Returns
/// * `200 OK` - 잔고 목록 반환
/// * `401 Unauthorized` - 인증 실패
/// * `500 Internal Server Error` - 서버 오류
#[utoipa::path(
    get,
    path = "/api/cex/balances",
    responses(
        (status = 200, description = "Balances retrieved successfully", body = ExchangeBalancesResponse),
        (status = 401, description = "Unauthorized (missing or invalid token)"),
        (status = 500, description = "Internal server error")
    ),
    tag = "CEX Balances",
    security(("BearerAuth" = []))
)]
pub async fn get_all_balances(
    State(app_state): State<AppState>,
    authenticated_user: AuthenticatedUser,
) -> Result<Json<ExchangeBalancesResponse>, (StatusCode, Json<serde_json::Value>)> {
    // JWT 토큰에서 추출한 user_id 사용
    // Use user_id extracted from JWT token
    let user_id = authenticated_user.user_id;

    // BalanceService를 통해 모든 잔고 조회
    // Get all balances through BalanceService
    let balances = app_state
        .cex_state
        .balance_service
        .get_all_balances(user_id)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("Failed to fetch balances: {}", e)
                })),
            )
        })?;

    Ok(Json(ExchangeBalancesResponse { balances }))
}

/// 특정 자산의 잔고 조회 핸들러
/// Get balance for specific asset
/// 
/// 경로: GET /api/cex/balances/{mint}
/// 인증: 필요 (JWT 토큰)
/// 
/// # Path Parameters
/// * `mint` - 자산 식별자 (예: "SOL", "USDT")
/// 
/// # Returns
/// * `200 OK` - 잔고 정보 반환 (잔고가 있는 경우)
/// * `404 Not Found` - 잔고가 없는 경우
/// * `401 Unauthorized` - 인증 실패
/// * `500 Internal Server Error` - 서버 오류
#[utoipa::path(
    get,
    path = "/api/cex/balances/{mint}",
    params(
        ("mint" = String, Path, description = "Asset identifier (e.g., 'SOL', 'USDT')")
    ),
    responses(
        (status = 200, description = "Balance retrieved successfully", body = ExchangeBalanceResponse),
        (status = 404, description = "Balance not found"),
        (status = 401, description = "Unauthorized (missing or invalid token)"),
        (status = 500, description = "Internal server error")
    ),
    tag = "CEX Balances",
    security(("BearerAuth" = []))
)]
pub async fn get_balance(
    State(app_state): State<AppState>,
    authenticated_user: AuthenticatedUser,
    Path(mint): Path<String>,
) -> Result<Json<ExchangeBalanceResponse>, (StatusCode, Json<serde_json::Value>)> {
    // JWT 토큰에서 추출한 user_id 사용
    // Use user_id extracted from JWT token
    let user_id = authenticated_user.user_id;

    // BalanceService를 통해 특정 자산 잔고 조회
    // Get specific asset balance through BalanceService
    let balance = app_state
        .cex_state
        .balance_service
        .get_balance(user_id, &mint)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("Failed to fetch balance: {}", e)
                })),
            )
        })?;

    // 잔고가 없으면 404 반환
    // Return 404 if balance not found
    let balance = balance.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": format!("Balance not found for asset: {}", mint)
            })),
        )
    })?;

    Ok(Json(ExchangeBalanceResponse { balance }))
}

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

