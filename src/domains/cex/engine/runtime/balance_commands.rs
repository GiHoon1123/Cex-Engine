//! 외부 잔액 증감도 코어가 실제 처리한 위치에서 저널 명령으로 바꾼다.
//! 음수 보정 없이 정확히 계산하고 저널 커밋 뒤 응답한다. 이 명령 자체에는
//! 외부 거래 식별자가 없으므로 호출자의 맹목적인 자동 재시도는 허용하지 않는다.
use anyhow::Result;
use rust_decimal::Decimal;
use tokio::sync::oneshot;

#[derive(Debug)]
pub enum BalanceCommand {
    UpdateBalance {
        user_id: u64,
        mint: String,
        available_delta: Decimal,
        response: oneshot::Sender<Result<()>>,
    },
}
