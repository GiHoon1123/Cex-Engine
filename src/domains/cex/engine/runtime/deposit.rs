//! 입금의 외부 식별자와 금융 내용을 코어의 독점 상태에 결합한다.
//! 체인 확인·고객 자격은 플랫폼 책임이며 코어는 확정 반입 명령의 중복과 금액만 검증한다.
use super::{engine::EngineState, output::OutputBatch};
use crate::domains::cex::engine::amount;
use anyhow::{ensure, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// 식별자는 플랫폼의 불변 입금 반입 원본이다. 고객이나 금액이 달라지면 같은 입금으로 처리하지 않는다.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DepositCredit {
    pub deposit_id: Uuid,
    pub user_id: u64,
    pub asset: String,
    pub amount: Decimal,
}
/// 오류 문구를 파싱하지 않고 이번 전송/영속 거절/결과 불명을 구분한다.
/// 이번 전송이 접수되지 않아도 과거 같은 입금의 실행 여부까지 부정하지는 않는다.
#[derive(Debug, thiserror::Error)]
pub enum DepositCreditError {
    #[error("이번 입금 명령 전송이 접수되지 않았습니다")]
    NotAccepted,
    #[error("입금 결과를 확정할 수 없습니다. 같은 식별자와 금융 내용으로 재확인하세요")]
    OutcomeUnknown,
    /// 이 변형은 저널 커밋 이후 받은 거절에만 사용한다. 이유는 설명용이며 분기에 사용하지 않는다.
    #[error("자체 저널에 확정된 입금 거절: {reason}")]
    Rejected { reason: String },
}

/// 정상 형식의 명령은 계산 실패도 보존한다. 실패 뒤의 중복을 성공으로 바꾸지 않는다.
pub(super) struct DepositReceipt {
    original: DepositCredit,
    rejection: Option<String>,
}

pub(super) fn apply(
    state: &mut EngineState,
    credit: &DepositCredit,
) -> Result<Option<OutputBatch>> {
    if let Some(receipt) = state.deposit_receipts.get(&credit.deposit_id) {
        ensure!(
            &receipt.original == credit,
            "Deposit id conflicts with original financial content"
        );
        if let Some(reason) = &receipt.rejection {
            anyhow::bail!("{}", reason);
        }
        // 중복 요청 자체는 저널에 남지만 새 금융 출력과 잔액 증가는 만들지 않는다.
        return Ok(None);
    }
    ensure!(
        !credit.deposit_id.is_nil() && credit.user_id > 0,
        "Invalid deposit identity"
    );
    ensure!(
        !credit.asset.is_empty()
            && credit.asset.len() <= 128
            && credit.asset.trim() == credit.asset,
        "Invalid deposit asset"
    );
    ensure!(
        credit.amount > Decimal::ZERO,
        "Deposit amount must be positive"
    );
    amount::units(credit.amount)?;
    let result = (|| -> Result<Option<OutputBatch>> {
        let cache = state.executor.balance_cache_mut();
        let old = cache
            .get_balance(credit.user_id, &credit.asset)
            .map(|b| b.available)
            .unwrap_or(Decimal::ZERO);
        let next = amount::add(old, credit.amount)?;
        // 범위 검증이 끝난 뒤에만 원본 상태를 바꾼다. 잠금 잔액은 건드리지 않는다.
        cache
            .get_balance_mut(credit.user_id, &credit.asset)
            .available = next;
        Ok(Some(OutputBatch::DepositCredited(credit.clone())))
    })();
    state.deposit_receipts.insert(
        credit.deposit_id,
        DepositReceipt {
            original: credit.clone(),
            rejection: result.as_ref().err().map(ToString::to_string),
        },
    );
    result
}
