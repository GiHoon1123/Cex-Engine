use crate::domains::cex::routes::create_cex_router;
use crate::shared::handlers::health::health_check;
use crate::shared::services::AppState;
use axum::{routing::get, Router};

/// 엔진의 HTTP 진입점: 헬스체크와 거래 명령만 등록한다.
///
/// 사용자 로그인과 토큰 발급은 Java 플랫폼의 책임이다. 엔진이 사용자 인증을
/// 처리한다는 의미로 해석되지 않도록 비활성 인증 라우트도 코드에서 제거했다.
/// 현재 거래 명령의 user_id는 호출자가 전달하므로, 이 라우터를 구성하는 것만으로
/// 호출자 신원이 보장되지는 않는다. 운영 시에는 별도로 서비스 접근을 제한해야 한다.
pub fn create_router() -> Router<AppState> {
    Router::new()
        .route("/api/health", get(health_check))
        .nest("/api/cex", create_cex_router())
}

// 같은 모듈의 별도 파일에서 실제 라우터를 대상으로 HTTP 계약을 검증한다.
// `cfg(test)` 모듈은 테스트 빌드에만 포함되며 서버 바이너리에 들어가지 않는다.
#[cfg(test)]
mod tests;
