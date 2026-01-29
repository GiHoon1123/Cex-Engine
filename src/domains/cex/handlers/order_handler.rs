use crate::domains::cex::models::order::{Order, CreateOrderRequest};
use crate::shared::services::AppState;
use axum::{
    extract::{State, Path, Query},
    http::StatusCode,
    Json,
};

// =====================================================
// Order Handler
// =====================================================
// 역할: 주문 관련 HTTP API 엔드포인트
// 
// 처리 흐름:
// HTTP Request → Handler → Service → Repository/Engine → Response
// =====================================================

/// 주문 생성 핸들러
/// Create order handler
/// 
/// 새로운 주문을 생성합니다.
/// 
/// # Authentication
/// JWT 토큰 필요 (Bearer token)
/// 
/// # Request Body
/// - order_type: "buy" 또는 "sell"
/// - order_side: "limit" 또는 "market"
/// - base_mint: 기준 자산 (예: "SOL")
/// - quote_mint: 기준 통화 (예: "USDT", 선택적)
/// - price: 지정가 가격 (limit 주문만)
/// - amount: 주문 수량
/// 
/// # Response
/// - 201: 주문 생성 성공
/// - 400: 잘못된 요청 (유효성 검증 실패)
/// - 401: 인증 실패
/// - 500: 서버 오류
#[utoipa::path(
    post,
    path = "/api/cex/orders",
    request_body = CreateOrderRequest,
    responses(
        (status = 201, description = "Order created successfully", body = Order),
        (status = 400, description = "Bad request (invalid order parameters)"),
        (status = 500, description = "Internal server error")
    ),
    tag = "CEX Orders"
)]
pub async fn create_order(
    State(app_state): State<AppState>,
    Json(request): Json<CreateOrderRequest>,
) -> Result<(StatusCode, Json<Order>), (StatusCode, Json<serde_json::Value>)> {
    eprintln!("[OrderHandler] HTTP 요청 수신: order_id={:?}, user_id={}, order_type={}, order_side={}", 
             request.order_id, request.user_id, request.order_type, request.order_side);
    std::io::Write::flush(&mut std::io::stderr()).ok();
    
    // Service 호출 (user_id는 요청 본문에서 가져옴)
    let user_id = request.user_id;
    let order = app_state
        .cex_state
        .order_service
        .create_order(user_id, request)
        .await
        .map_err(|e| {
            eprintln!("[OrderHandler] 주문 생성 실패: error={}", e);
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": format!("Failed to create order: {}", e)
                })),
            )
        })?;

    eprintln!("[OrderHandler] 주문 생성 성공: order_id={}", order.id);
    Ok((StatusCode::CREATED, Json(order)))
}

/// 주문 취소 핸들러
/// Cancel order handler
/// 
/// 대기 중이거나 부분 체결된 주문을 취소합니다.
/// 
/// # Authentication
/// JWT 토큰 필요 (본인 주문만 취소 가능)
/// 
/// # Path Parameters
/// - order_id: 취소할 주문 ID
/// 
/// # Response
/// - 200: 주문 취소 성공
/// - 401: 인증 실패 또는 권한 없음
/// - 404: 주문을 찾을 수 없음
/// - 500: 서버 오류
#[utoipa::path(
    delete,
    path = "/api/cex/orders/{order_id}",
    params(
        ("order_id" = u64, Path, description = "Order ID to cancel"),
        ("user_id" = u64, Query, description = "User ID")
    ),
    responses(
        (status = 200, description = "Order cancelled successfully", body = Order),
        (status = 404, description = "Order not found"),
        (status = 500, description = "Internal server error")
    ),
    tag = "CEX Orders"
)]
pub async fn cancel_order(
    State(app_state): State<AppState>,
    Path(order_id): Path<u64>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Order>, (StatusCode, Json<serde_json::Value>)> {
    // user_id를 쿼리 파라미터에서 가져옴
    let user_id = params
        .get("user_id")
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "Missing user_id query parameter"
                })),
            )
        })?
        .parse::<u64>()
        .map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "Invalid user_id format"
                })),
            )
        })?;
    
    // Service 호출
    let order = app_state
        .cex_state
        .order_service
        .cancel_order(user_id, order_id)
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": format!("Failed to cancel order: {}", e)
                })),
            )
        })?;

    Ok(Json(order))
}


