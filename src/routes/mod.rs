use crate::domains::cex::routes::create_cex_router;
use crate::shared::handlers::health::health_check;
use crate::shared::services::AppState;
use axum::{routing::get, Router, middleware::{self, Next}, extract::Request, http::StatusCode};

/// 엔진의 HTTP 진입점: 헬스체크·거래 명령·내부 감사 조회를 등록한다.
///
/// 사용자 로그인과 토큰 발급은 Java 플랫폼의 책임이다. 엔진이 사용자 인증을
/// 처리한다는 의미로 해석되지 않도록 비활성 인증 라우트도 코드에서 제거했다.
/// 플랫폼 신원 검증은 HTTP 경계에서 끝난다. 체결 코어에 사용자 역할·인증 상태를 전달하지 않는다.
pub fn create_router(service_token: &str) -> Router<AppState> {
    assert!(service_token.len() >= 32 && service_token.len() <= 1024, "서비스 키는 32~1024바이트여야 합니다");
    let expected = crate::shared::service_identity::ServiceToken::parse(service_token).expect("유효한 플랫폼 명령 키가 필요합니다");
    let commands = create_cex_router().route_layer(middleware::from_fn(move |request: Request, next: Next| async move {
        let provided = request.headers().get("x-engine-service-token").and_then(|value| value.to_str().ok());
        if provided.is_none_or(|key| !expected.matches(key)) {
            return Err(StatusCode::UNAUTHORIZED);
        }
        Ok(next.run(request).await)
    }));
    Router::new()
        .route("/api/health", get(health_check))
        .route("/internal/audit/results", get(crate::domains::cex::engine::runtime::audit::page))
        .nest("/api/cex", commands)
}

// 같은 모듈의 별도 파일에서 실제 라우터를 대상으로 HTTP 계약을 검증한다.
// `cfg(test)` 모듈은 테스트 빌드에만 포함되며 서버 바이너리에 들어가지 않는다.
#[cfg(test)]
mod tests;
