//! 플랫폼이 검증한 출금 원본의 예약/해제/완료만 전달한다. 실제 체인 RPC나 서명키를 다루지 않는다.
use crate::domains::cex::engine::runtime::{
    WithdrawalAction, WithdrawalCommand, WithdrawalError, WithdrawalFunds,
};
use crate::shared::services::AppState;
use axum::{
    extract::{rejection::JsonRejection, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WithdrawalRequest {
    withdrawal_id: Uuid,
    user_id: u64,
    asset: String,
    amount: String,
    action: WithdrawalAction,
}
impl WithdrawalRequest {
    fn validated(self) -> Option<WithdrawalCommand> {
        if self.withdrawal_id.is_nil() {
            return None;
        }
        let amount = super::financial_input::validated(self.user_id, &self.asset, &self.amount)?;
        Some(WithdrawalCommand {
            action: self.action,
            funds: WithdrawalFunds {
                withdrawal_id: self.withdrawal_id,
                user_id: self.user_id,
                asset: self.asset,
                amount,
            },
        })
    }
}
/// 서비스 신원은 상위 라우터에서 확인한다. 요청 태스크만 응답을 기다리고 단일 코어는 다음 명령을 계산한다.
pub async fn change_withdrawal(
    State(state): State<AppState>,
    request: Result<Json<WithdrawalRequest>, JsonRejection>,
) -> Response {
    let request = match request {
        Ok(Json(value)) => value,
        Err(error) => {
            return reply(
                if error.status() == StatusCode::PAYLOAD_TOO_LARGE {
                    StatusCode::PAYLOAD_TOO_LARGE
                } else {
                    StatusCode::BAD_REQUEST
                },
                "INVALID_WITHDRAWAL_COMMAND",
                None,
                None,
            )
        }
    };
    let Some(command) = request.validated() else {
        return reply(
            StatusCode::BAD_REQUEST,
            "INVALID_WITHDRAWAL_COMMAND",
            None,
            None,
        );
    };
    let id = command.funds.withdrawal_id;
    let action = command.action;
    match state.engine.change_withdrawal(command).await {
        Ok(()) => reply(
            StatusCode::OK,
            match action {
                WithdrawalAction::Reserve => "DURABLY_RESERVED",
                WithdrawalAction::Release => "DURABLY_RELEASED",
                WithdrawalAction::Complete => "DURABLY_COMPLETED",
            },
            Some(id),
            Some(action),
        ),
        Err(WithdrawalError::NotAccepted) => reply(
            StatusCode::SERVICE_UNAVAILABLE,
            "NOT_ACCEPTED",
            Some(id),
            Some(action),
        ),
        Err(WithdrawalError::OutcomeUnknown) => reply(
            StatusCode::GATEWAY_TIMEOUT,
            "OUTCOME_UNKNOWN",
            Some(id),
            Some(action),
        ),
        Err(WithdrawalError::Rejected { .. }) => reply(
            StatusCode::CONFLICT,
            "DURABLY_REJECTED",
            Some(id),
            Some(action),
        ),
    }
}
fn reply(
    status: StatusCode,
    code: &'static str,
    id: Option<Uuid>,
    action: Option<WithdrawalAction>,
) -> Response {
    // 기록 장애·잔액 부족의 내부 문자열은 공개하지 않는다. 이번 거절과 다른 단계의 과거 실행은 구분한다.
    (
        status,
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({"code":code,"withdrawal_id":id,"action":action})),
    )
        .into_response()
}
