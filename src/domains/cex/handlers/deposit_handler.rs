//! 플랫폼이 확정한 입금 원본을 기존 명령 핸들에 전달한다.
//! 체인·고객·출처 심사는 플랫폼 책임이다. 이 경계는 원본 없는 잔액 증감 API가 아니다.
use crate::domains::cex::engine::runtime::{DepositCredit, DepositCreditError};
use crate::shared::services::AppState;
use axum::{
    extract::{rejection::JsonRejection, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use uuid::Uuid;

/// 금액을 문자열로 먼저 받는다. 설치된 Decimal의 기본 문자열 역직렬화는 지수 표기와
/// 정밀도 축소를 허용하므로, 금융 경계에서는 아래 정확 변환 함수를 명시적으로 사용한다.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreditRequest {
    deposit_id: Uuid,
    user_id: u64,
    asset: String,
    amount: String,
}
impl CreditRequest {
    fn validated(self) -> Option<DepositCredit> {
        if self.deposit_id.is_nil() {
            return None;
        }
        let value = super::financial_input::validated(self.user_id, &self.asset, &self.amount)?;
        Some(DepositCredit {
            deposit_id: self.deposit_id,
            user_id: self.user_id,
            asset: self.asset,
            amount: value,
        })
    }
}

/// HTTP 대기는 비동기 요청 태스크에서만 수행한다. 코어는 기존 큐의 명령을 순차 계산하고
/// 기록 워커가 커밋한 뒤에만 핸들의 성공/확정 거절을 돌려준다. 큐 진입은 성공 응답이 아니다.
pub async fn credit_deposit(
    State(state): State<AppState>,
    request: Result<Json<CreditRequest>, JsonRejection>,
) -> Response {
    let request = match request {
        Ok(Json(request)) => request,
        Err(rejection) => {
            return reply(
                if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
                    StatusCode::PAYLOAD_TOO_LARGE
                } else {
                    StatusCode::BAD_REQUEST
                },
                "INVALID_DEPOSIT_CREDIT",
                None,
            )
        }
    };
    let Some(credit) = request.validated() else {
        return reply(StatusCode::BAD_REQUEST, "INVALID_DEPOSIT_CREDIT", None);
    };
    let id = credit.deposit_id;
    // 내부 SQL이나 저널 오류 문자열 대신 고정 코드만 공개한다.
    match state.engine.credit_deposit(credit).await {
        Ok(()) => reply(StatusCode::OK, "DURABLY_CREDITED", Some(id)),
        Err(DepositCreditError::NotAccepted) => {
            reply(StatusCode::SERVICE_UNAVAILABLE, "NOT_ACCEPTED", Some(id))
        }
        Err(DepositCreditError::Rejected { .. }) => {
            reply(StatusCode::CONFLICT, "DURABLY_REJECTED", Some(id))
        }
        Err(DepositCreditError::OutcomeUnknown) => {
            reply(StatusCode::GATEWAY_TIMEOUT, "OUTCOME_UNKNOWN", Some(id))
        }
    }
}
fn reply(status: StatusCode, code: &'static str, id: Option<Uuid>) -> Response {
    // 이번 HTTP 실패만으로 같은 입금의 과거 실행까지 부정하지 않는다.
    (
        status,
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({"code":code,"deposit_id":id})),
    )
        .into_response()
}
