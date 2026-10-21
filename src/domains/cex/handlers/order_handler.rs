use crate::domains::cex::models::order::{Order, CreateOrderRequest};
use crate::shared::services::AppState;
use crate::domains::cex::services::order_receipt::{OrderAcceptance, ReceiptError};
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
/// Java 플랫폼이 사용자 인증을 담당하고 요청 본문의 user_id를 전달한다.
/// 엔진은 사용자 JWT를 검증하지 않으므로, 서비스 간 접근 제한이 별도로 필요하다.
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
/// - 202: 영속 접수 완료, 실행 결과 미확정
/// - 200: 기존 접수 확인, 실행 큐 재전송 없음
/// - 400: 잘못된 주문 원본
/// - 409: 주문 ID와 명령 내용 충돌
/// - 503: 영속 접수 저장소 오류
#[utoipa::path(
    post,
    path = "/api/cex/orders",
    request_body = CreateOrderRequest,
    responses(
        (status = 202, description = "Durable acceptance; execution is not confirmed", body = OrderAcceptance),
        (status = 200, description = "Existing receipt; command is not dispatched again", body = OrderAcceptance),
        (status = 409, description = "Order id conflicts with original command"),
        (status = 400, description = "Bad request (invalid order parameters)"),
        (status = 500, description = "Internal server error")
    ),
    tag = "CEX Orders"
)]
pub async fn create_order(
    State(app_state): State<AppState>,
    Json(request): Json<CreateOrderRequest>,
) -> Result<(StatusCode, Json<OrderAcceptance>), (StatusCode, Json<serde_json::Value>)> {
    let accepted = app_state.cex_state.order_service.create_order(request.user_id, request).await
        .map_err(|error| {
            // 입력/키 충돌과 DB 장애를 구분한다. SQL·연결 문자열 등 내부 원인은 응답에 싣지 않는다.
            let (status, code) = match error.downcast_ref::<ReceiptError>() {
                Some(ReceiptError::Invalid) => (StatusCode::BAD_REQUEST, "INVALID_ORDER_SUBMISSION"),
                Some(ReceiptError::Conflict) => (StatusCode::CONFLICT, "ORDER_SUBMISSION_CONFLICT"),
                None => (StatusCode::SERVICE_UNAVAILABLE, "ORDER_ACCEPTANCE_UNAVAILABLE"),
            };
            eprintln!("[Order] 접수 실패: {error:#}");
            (status, Json(serde_json::json!({"code": code})))
        })?;
    let status = if accepted.created { StatusCode::ACCEPTED } else { StatusCode::OK };
    Ok((status, Json(accepted.receipt)))
}

/// 주문 취소 핸들러
/// Cancel order handler
/// 
/// 대기 중이거나 부분 체결된 주문을 취소합니다.
/// 
/// # Authentication
/// Java 플랫폼이 인증한 사용자의 user_id를 쿼리로 전달한다.
/// 엔진 서비스는 해당 ID와 주문 소유자를 비교하지만 호출자 신원을 검증하지 않는다.
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


