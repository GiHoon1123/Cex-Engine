use crate::domains::cex::models::order::{Order, CreateOrderRequest};
use crate::shared::services::AppState;
use crate::shared::middleware::auth::AuthenticatedUser;
use axum::{
    extract::{State, Path, Query},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use utoipa::{ToSchema, IntoParams};

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
    // Service 호출 (user_id는 요청 본문에서 가져옴)
    let user_id = request.user_id;
    let order = app_state
        .cex_state
        .order_service
        .create_order(user_id, request)
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": format!("Failed to create order: {}", e)
                })),
            )
        })?;

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

/// 특정 주문 조회 핸들러
/// Get order by ID handler
/// 
/// # Authentication
/// JWT 토큰 필요 (본인 주문만 조회 가능)
/// 
/// # Path Parameters
/// - order_id: 조회할 주문 ID
/// 
/// # Response
/// - 200: 주문 조회 성공
/// - 401: 인증 실패 또는 권한 없음
/// - 404: 주문을 찾을 수 없음
#[utoipa::path(
    get,
    path = "/api/cex/orders/{order_id}",
    params(
        ("order_id" = u64, Path, description = "Order ID to retrieve")
    ),
    responses(
        (status = 200, description = "Order retrieved successfully", body = Order),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Order not found"),
        (status = 500, description = "Internal server error")
    ),
    tag = "CEX Orders",
    security(
        ("BearerAuth" = [])
    )
)]
pub async fn get_order(
    State(app_state): State<AppState>,
    AuthenticatedUser { user_id, .. }: AuthenticatedUser,
    Path(order_id): Path<u64>,
) -> Result<Json<Order>, (StatusCode, Json<serde_json::Value>)> {
    // Service 호출
    let order = app_state
        .cex_state
        .order_service
        .get_order(user_id, order_id)
        .await
        .map_err(|e| {
            (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "error": format!("Order not found: {}", e)
                })),
            )
        })?;

    Ok(Json(order))
}

/// 쿼리 파라미터 (내 주문 목록)
/// Query parameters for my orders
#[derive(Debug, Deserialize, ToSchema, IntoParams)]
pub struct MyOrdersQuery {
    /// 주문 상태 필터 (pending, partial, filled, cancelled)
    /// Order status filter
    #[serde(default)]
    pub status: Option<String>,
    
    /// 최대 조회 개수 (기본: 50)
    /// Limit (default: 50)
    #[serde(default)]
    pub limit: Option<i64>,
    
    /// 페이지네이션 오프셋 (기본: 0)
    /// Offset for pagination (default: 0)
    #[serde(default)]
    pub offset: Option<i64>,
}

/// 내 주문 목록 조회 핸들러
/// Get my orders handler
/// 
/// 현재 로그인한 사용자의 주문 목록을 조회합니다.
/// 
/// # Authentication
/// JWT 토큰 필요
/// 
/// # Query Parameters
/// - status: 주문 상태 필터 (optional)
/// - limit: 최대 조회 개수 (optional, default: 50)
/// - offset: 페이지네이션 오프셋 (optional, default: 0)
/// 
/// # Response
/// - 200: 주문 목록 조회 성공
/// - 401: 인증 실패
#[utoipa::path(
    get,
    path = "/api/cex/orders/my",
    params(
        MyOrdersQuery
    ),
    responses(
        (status = 200, description = "My orders retrieved successfully", body = Vec<Order>),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    tag = "CEX Orders",
    security(
        ("BearerAuth" = [])
    )
)]
pub async fn get_my_orders(
    State(app_state): State<AppState>,
    AuthenticatedUser { user_id, .. }: AuthenticatedUser,
    Query(query): Query<MyOrdersQuery>,
) -> Result<Json<Vec<Order>>, (StatusCode, Json<serde_json::Value>)> {
    // Service 호출
    let orders = app_state
        .cex_state
        .order_service
        .get_my_orders(
            user_id,
            query.status.as_deref(),
            query.limit,
            query.offset,
        )
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("Failed to fetch orders: {}", e)
                })),
            )
        })?;

    Ok(Json(orders))
}

/// 오더북 응답 모델
/// Orderbook response model
#[derive(Debug, Serialize, ToSchema)]
pub struct OrderbookResponse {
    /// 매수 호가 목록 (가격 내림차순)
    /// Buy orders (price descending)
    pub bids: Vec<Order>,
    
    /// 매도 호가 목록 (가격 오름차순)
    /// Sell orders (price ascending)
    pub asks: Vec<Order>,
}

/// 오더북 쿼리 파라미터
/// Orderbook query parameters
#[derive(Debug, Deserialize, ToSchema, IntoParams)]
pub struct OrderbookQuery {
    /// 기준 자산 (예: "SOL")
    /// Base asset (e.g., "SOL")
    pub base_mint: String,
    
    /// 기준 통화 (예: "USDT", 기본값)
    /// Quote currency (e.g., "USDT", default)
    #[serde(default = "default_quote_mint")]
    pub quote_mint: String,
    
    /// 조회할 가격 레벨 개수 (optional)
    /// Number of price levels to retrieve (optional)
    #[serde(default)]
    pub depth: Option<usize>,
}

fn default_quote_mint() -> String {
    "USDT".to_string()
}

/// 오더북 조회 핸들러
/// Get orderbook handler
/// 
/// 특정 거래쌍의 오더북(호가창)을 조회합니다.
/// 
/// # Query Parameters
/// - base_mint: 기준 자산 (required, 예: "SOL")
/// - quote_mint: 기준 통화 (optional, 기본: "USDT")
/// - depth: 조회할 가격 레벨 개수 (optional)
/// 
/// # Response
/// - 200: 오더북 조회 성공
/// - 400: 잘못된 요청
/// - 500: 서버 오류
#[utoipa::path(
    get,
    path = "/api/cex/orderbook",
    params(
        OrderbookQuery
    ),
    responses(
        (status = 200, description = "Orderbook retrieved successfully", body = OrderbookResponse),
        (status = 400, description = "Bad request"),
        (status = 500, description = "Internal server error")
    ),
    tag = "Orders"
)]
pub async fn get_orderbook(
    State(app_state): State<AppState>,
    Query(query): Query<OrderbookQuery>,
) -> Result<Json<OrderbookResponse>, (StatusCode, Json<serde_json::Value>)> {
    // Service 호출
    let (bids, asks) = app_state
        .cex_state
        .order_service
        .get_orderbook(&query.base_mint, &query.quote_mint, query.depth)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("Failed to fetch orderbook: {}", e)
                })),
            )
        })?;

    Ok(Json(OrderbookResponse { bids, asks }))
}

