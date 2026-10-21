//! 코어가 계산한 명령과 결과를 영속 워커로 이동한다. 코어는 저장 완료를 기다리지
//! 않으며 응답 채널도 함께 넘긴다. 저장 워커만 커밋 이후 응답을 공개한다.
use super::{engine::EngineState, output::OutputBatch, threads};
use crate::domains::cex::{
    engine::{
        amount,
        types::{OrderEntry, TradingPair},
    },
    services::order_receipt::ReceiptError,
};
use anyhow::{ensure, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

pub(super) mod store;

/// 별도 입력 큐에서 받은 명령도 코어가 실제 실행한 순서로 이 타입을 만든다.
/// 재생은 같은 계산 함수를 사용하고 저장된 결과와 비교한다.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) enum Command {
    Submit(OrderEntry),
    Cancel {
        order_id: u64,
        user_id: u64,
        pair: TradingPair,
    },
    Balance {
        user_id: u64,
        mint: String,
        delta: Decimal,
    },
    Lock {
        user_id: u64,
        mint: String,
        amount: Decimal,
    },
    Unlock {
        user_id: u64,
        mint: String,
        amount: Decimal,
    },
}
#[derive(Debug, Serialize, Deserialize)]
pub(super) enum Outcome {
    Applied(Option<OutputBatch>),
    Failed(String),
    DuplicateCancellation(OrderEntry),
}

/// 요청 응답은 직렬화 대상이 아니다. 채널 소유권을 저장 워커로 옮기면 코어가
/// 동기 디스크 대기 없이 다음 계산을 진행하면서 성공 공개는 커밋 뒤로 늦출 수 있다.
pub(super) enum Reply {
    Submit(oneshot::Sender<Result<()>>),
    Cancel(oneshot::Sender<Result<OrderEntry>>),
    Unit(oneshot::Sender<Result<()>>),
    Book(
        oneshot::Sender<Result<(Vec<OrderEntry>, Vec<OrderEntry>)>>,
        (Vec<OrderEntry>, Vec<OrderEntry>),
    ),
    Balance(
        oneshot::Sender<Result<(Decimal, Decimal)>>,
        (Decimal, Decimal),
    ),
}
pub(super) struct Pending {
    pub command: Option<Command>,
    pub outcome: Outcome,
    pub reply: Reply,
}
impl Pending {
    pub fn finish(self) {
        match self.reply {
            Reply::Submit(sender) => {
                // 접수 응답은 실행 성공과 구분한다. 거부 결과도 기록된 명령이다.
                let result = match self.outcome {
                    Outcome::Failed(reason) => Err(anyhow::anyhow!(reason)),
                    _ => Ok(()),
                };
                let _ = sender.send(result);
            }
            Reply::Cancel(sender) => {
                let result = match self.outcome {
                    Outcome::Applied(Some(OutputBatch::Cancelled(order)))
                    | Outcome::DuplicateCancellation(order) => Ok(order),
                    Outcome::Failed(reason) => Err(anyhow::anyhow!(reason)),
                    _ => Err(anyhow::anyhow!("Invalid cancellation outcome")),
                };
                let _ = sender.send(result);
            }
            Reply::Unit(sender) => {
                let _ = sender.send(match self.outcome {
                    Outcome::Failed(reason) => Err(anyhow::anyhow!(reason)),
                    _ => Ok(()),
                });
            }
            Reply::Book(sender, value) => {
                let _ = sender.send(Ok(value));
            }
            Reply::Balance(sender, value) => {
                let _ = sender.send(Ok(value));
            }
        }
    }
}

/// 실패한 명령도 기록해 재시도·재생에서 같은 실행 이력을 유지한다.
pub(super) fn apply(state: &mut EngineState, command: &Command) -> Outcome {
    if let Command::Cancel {
        order_id,
        user_id,
        pair,
    } = command
    {
        if let Some(order) = state.cancellations.get(order_id) {
            if order.user_id == *user_id
                && order.base_mint == pair.base_mint
                && order.quote_mint == pair.quote_mint
            {
                return Outcome::DuplicateCancellation(order.clone());
            }
            return Outcome::Failed("Unauthorized cancellation".into());
        }
    }
    match execute(state, command) {
        Ok(output) => Outcome::Applied(output),
        Err(error) => Outcome::Failed(error.to_string()),
    }
}
fn execute(state: &mut EngineState, command: &Command) -> Result<Option<OutputBatch>> {
    match command {
        Command::Submit(order) => {
            if let Some(original) = state.submissions.get(&order.id) {
                let mut repeated = order.clone();
                repeated.created_at = original.created_at;
                ensure!(&repeated == original, ReceiptError::Conflict);
                return Ok(None);
            }
            // 재전송 방지는 메모리에서 검사하고 최초 명령만 실행한다. 이 인덱스도
            // 로그 재생으로 복원되므로 재시작 뒤 같은 주문이 다시 매칭되지 않는다.
            state.submissions.insert(order.id, order.clone());
            let batch = match threads::process_submit_order(
                order.clone(),
                &mut state.orderbooks,
                &state.matcher,
                &mut state.executor,
            ) {
                Ok(applied) => OutputBatch::submitted(applied),
                Err(error) => OutputBatch::Rejected {
                    order: order.clone(),
                    reason: error.to_string(),
                },
            };
            Ok(Some(batch))
        }
        Command::Cancel {
            order_id,
            user_id,
            pair,
        } => {
            let order = threads::cancel_order(
                *order_id,
                *user_id,
                pair,
                &mut state.orderbooks,
                &mut state.executor,
            )?;
            state.cancellations.insert(*order_id, order.clone());
            Ok(Some(OutputBatch::Cancelled(order)))
        }
        Command::Balance {
            user_id,
            mint,
            delta,
        } => {
            ensure!(
                *user_id > 0 && !mint.is_empty() && mint.trim() == mint,
                "Invalid balance account"
            );
            let cache = state.executor.balance_cache_mut();
            let old = cache
                .get_balance(*user_id, mint)
                .map(|b| b.available)
                .unwrap_or(Decimal::ZERO);
            let next = if *delta >= Decimal::ZERO {
                amount::add(old, *delta)?
            } else {
                amount::subtract(old, delta.abs())?
            };
            // 검증 후에만 계정을 만든다. 음수 출금을 0으로 보정하지 않는다.
            cache.get_balance_mut(*user_id, mint).available = next;
            Ok(None)
        }
        Command::Lock {
            user_id,
            mint,
            amount,
        } => {
            state
                .executor
                .lock_balance_for_order(0, *user_id, mint, *amount)?;
            Ok(None)
        }
        Command::Unlock {
            user_id,
            mint,
            amount,
        } => {
            state
                .executor
                .unlock_balance_for_cancel(0, *user_id, mint, *amount)?;
            Ok(None)
        }
    }
}
