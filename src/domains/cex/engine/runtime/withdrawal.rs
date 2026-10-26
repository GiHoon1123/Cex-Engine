//! 출금별 불변 원본과 예약 생명주기를 단일 코어가 소유한다.
//! 체인 전송/서명/승인은 플랫폼 책임이며 코어는 금액 이동과 중복 실행만 검사한다.
use super::{engine::EngineState, output::OutputBatch};
use crate::domains::cex::engine::amount;
use anyhow::{ensure, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// 출금 UUID는 플랫폼이 확정한 경제적 원본이다. 재전송 때 사용자/자산/금액을 바꿀 수 없다.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WithdrawalFunds {
    pub withdrawal_id: Uuid,
    pub user_id: u64,
    pub asset: String,
    pub amount: Decimal,
}
/// 예약은 가용→잠금, 해제는 잠금→가용, 완료는 잠금의 외부 유출 확정이다.
/// 완료 명령은 실제 전송 증명의 대체물이 아니다. 그 권한은 플랫폼이 검증해야 한다.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WithdrawalAction {
    Reserve,
    Release,
    Complete,
}
impl WithdrawalAction {
    pub(super) fn event_type(self) -> &'static str {
        match self {
            Self::Reserve => "withdrawal_reserved",
            Self::Release => "withdrawal_released",
            Self::Complete => "withdrawal_completed",
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WithdrawalCommand {
    pub action: WithdrawalAction,
    pub funds: WithdrawalFunds,
}

/// 이번 명령의 전송 미접수·영속 거절·결과 불명을 구분한다. 큐 전달은 영속 확정이 아니다.
#[derive(Debug, thiserror::Error)]
pub enum WithdrawalError {
    #[error("이번 출금 명령 전송이 접수되지 않았습니다")]
    NotAccepted,
    #[error("출금 명령 결과를 확정할 수 없습니다. 같은 원본과 동작으로 재확인하세요")]
    OutcomeUnknown,
    #[error("자체 저널에 확정된 이번 출금 명령 거절: {reason}")]
    Rejected { reason: String },
}
#[derive(Debug, PartialEq, Eq)]
enum Status {
    Reserved,
    Released,
    Completed,
    Rejected(String),
}
pub(super) struct WithdrawalReceipt {
    original: WithdrawalFunds,
    status: Status,
}

pub(super) fn apply(
    state: &mut EngineState,
    command: &WithdrawalCommand,
) -> Result<Option<OutputBatch>> {
    let funds = &command.funds;
    ensure!(
        !funds.withdrawal_id.is_nil() && funds.user_id > 0 && funds.user_id <= i64::MAX as u64,
        "Invalid withdrawal identity"
    );
    ensure!(
        !funds.asset.is_empty()
            && funds.asset.len() <= 128
            && funds
                .asset
                .bytes()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()),
        "Invalid withdrawal asset"
    );
    ensure!(
        funds.amount > Decimal::ZERO,
        "Withdrawal amount must be positive"
    );
    amount::units(funds.amount)?;
    if let Some(receipt) = state.withdrawal_receipts.get(&funds.withdrawal_id) {
        ensure!(
            &receipt.original == funds,
            "Withdrawal id conflicts with original financial content"
        );
        if let Status::Rejected(reason) = &receipt.status {
            anyhow::bail!("{}", reason);
        }
        match (&receipt.status, command.action) {
            // 같은 단계의 성공만 멱등이다. 이미 해제/완료한 예약을 재전송으로 다시 잠그지 않는다.
            (Status::Reserved, WithdrawalAction::Reserve)
            | (Status::Released, WithdrawalAction::Release)
            | (Status::Completed, WithdrawalAction::Complete) => return Ok(None),
            (Status::Reserved, _) => {}
            _ => anyhow::bail!("Withdrawal reservation is already finalized"),
        }
    } else {
        // 주문 취소의 미접수 원본과 같다. 아직 존재하지 않는 예약을 임의로 생성/해제하지 않는다.
        // 이 거절은 이번 저널 위치의 결과이며 이후 실제 예약 생성 자체를 막는 묘비가 아니다.
        ensure!(
            command.action == WithdrawalAction::Reserve,
            "Withdrawal reservation does not exist"
        );
    }
    let cache = state.executor.balance_cache_mut();
    match command.action {
        WithdrawalAction::Reserve => {
            // 계산 실패도 최초 출금 원본에 결합한다. 입금 뒤 같은 UUID의 부족 거절을 성공으로 바꾸지 않는다.
            if let Err(error) = cache.lock_balance(funds.user_id, &funds.asset, funds.amount) {
                let reason = error.to_string();
                state.withdrawal_receipts.insert(
                    funds.withdrawal_id,
                    WithdrawalReceipt {
                        original: funds.clone(),
                        status: Status::Rejected(reason.clone()),
                    },
                );
                // 최초 금융 거절도 별도 결과다. HTTP의 다른 단계 충돌과 구분해 플랫폼이 안전하게 반영한다.
                return Ok(Some(OutputBatch::WithdrawalRejected {
                    funds: funds.clone(),
                    reason,
                }));
            }
        }
        WithdrawalAction::Release => {
            cache.unlock_balance(funds.user_id, &funds.asset, funds.amount)?
        }
        WithdrawalAction::Complete => {
            let old = cache
                .get_balance(funds.user_id, &funds.asset)
                .ok_or_else(|| anyhow::anyhow!("Missing withdrawal balance"))?;
            let next = amount::subtract(old.locked, funds.amount)?;
            // 검사 후에만 한 필드를 반영한다. 주문 몫과 다른 출금의 예약 금액을 함께 차감하지 않는다.
            cache.get_balance_mut(funds.user_id, &funds.asset).locked = next;
        }
    }
    let status = match command.action {
        WithdrawalAction::Reserve => Status::Reserved,
        WithdrawalAction::Release => Status::Released,
        WithdrawalAction::Complete => Status::Completed,
    };
    state.withdrawal_receipts.insert(
        funds.withdrawal_id,
        WithdrawalReceipt {
            original: funds.clone(),
            status,
        },
    );
    Ok(Some(OutputBatch::WithdrawalChanged(command.clone())))
}
