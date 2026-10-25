//! 요청용 명령 핸들. 주문장·잔액·스레드 JoinHandle을 보유하지 않는다.
use super::{balance_commands::BalanceCommand, commands::OrderCommand};
use crate::domains::cex::engine::{Engine, OrderEntry, TradingPair};
use anyhow::Result;
use async_trait::async_trait;
use crossbeam::channel::Sender;
use rust_decimal::Decimal;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::{
    sync::oneshot,
    time::{timeout, Duration},
};

/// clone은 송신 권한만 복제한다. 여러 HTTP·UDP 호출자가 응답을 기다려도
/// 서로의 요청 전체를 Mutex로 잠그지 않는다. 거래 상태는 워커 안에만 있다.
#[derive(Clone)]
pub struct EngineHandle {
    pub(super) order_tx: Sender<OrderCommand>,
    pub(super) balance_tx: Sender<BalanceCommand>,
    pub(super) closed: Arc<AtomicBool>,
}

impl EngineHandle {
    /// 같은 입금 원본만 재시도한다. 성공 응답은 기존 기록 워커의 영속 커밋 뒤에만 도착한다.
    pub async fn credit_deposit(
        &self,
        credit: super::DepositCredit,
    ) -> std::result::Result<(), super::DepositCreditError> {
        use super::DepositCreditError;
        let (sender, receiver) = oneshot::channel();
        self.send_balance(BalanceCommand::CreditDeposit {
            credit,
            response: sender,
        })
        .map_err(|_| DepositCreditError::NotAccepted)?;
        // 큐 진입 뒤에는 응답 유실/시간 초과를 거절로 바꾸지 않는다. 메모리 계산이나
        // 저널 커밋이 이미 진행됐을 수 있으므로 금융 원본을 유지한 재확인만 허용한다.
        timeout(Duration::from_secs(3), receiver)
            .await
            .map_err(|_| DepositCreditError::OutcomeUnknown)?
            .map_err(|_| DepositCreditError::OutcomeUnknown)?
            // 기록 워커의 finish는 커밋 뒤에만 호출된다. 계산 거절도 저장에 실패하면
            // 이 분기에 도달하지 않고 위의 응답 유실로 처리된다.
            .map_err(|error| DepositCreditError::Rejected {
                reason: error.to_string(),
            })
    }
    /// 닫힘은 정상 정지 또는 출력 실패로 더 이상 명령을 실행하지 않음을 뜻한다.
    /// 브로커 연결이나 프로세스 장애 복구 가능성 전체를 검증하는 상태는 아니다.
    pub fn is_accepting(&self) -> bool {
        !self.closed.load(Ordering::Acquire)
    }

    /// 모든 진입 명령은 큐가 가득 차면 즉시 오류다. timeout 전에 동기 send로
    /// Tokio 실행 스레드를 막지 않는다. 이미 접수한 주문의 복구는 별도 계약이다.
    fn send_order(&self, command: OrderCommand) -> Result<()> {
        anyhow::ensure!(
            !self.closed.load(Ordering::Acquire),
            "Engine command channel is closed"
        );
        self.order_tx
            .try_send(command)
            .map_err(|e| anyhow::anyhow!("Engine queue unavailable: {}", e))
    }
    fn send_balance(&self, command: BalanceCommand) -> Result<()> {
        anyhow::ensure!(
            !self.closed.load(Ordering::Acquire),
            "Engine command channel is closed"
        );
        self.balance_tx
            .try_send(command)
            .map_err(|e| anyhow::anyhow!("Engine balance queue unavailable: {}", e))
    }
}

#[async_trait]
impl Engine for EngineHandle {
    /// 코어는 계산 후 응답 채널을 저널 워커로 넘긴다. 이 요청 작업만 비동기로
    /// 영속 커밋을 기다리므로 매칭 스레드가 디스크 완료를 기다리지 않는다.
    async fn submit_order(&self, order: OrderEntry) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.send_order(OrderCommand::SubmitOrder {
            order,
            response: sender,
        })?;
        timeout(Duration::from_secs(10), receiver)
            .await
            .map_err(|_| {
                anyhow::anyhow!("Durable submission outcome unknown; retry the same order id")
            })?
            .map_err(|_| {
                anyhow::anyhow!("Journal response unavailable; retry the same order id")
            })??;
        Ok(())
    }

    /// 주문 취소
    async fn cancel_order(
        &self,
        order_id: u64,
        user_id: u64,
        trading_pair: &TradingPair,
    ) -> Result<OrderEntry> {
        let (tx, rx) = oneshot::channel();

        let cmd = OrderCommand::CancelOrder {
            order_id,
            user_id,
            trading_pair: trading_pair.clone(),
            response: tx,
        };

        self.send_order(cmd)
            .map_err(|e| anyhow::anyhow!("Failed to send cancel command: {}", e))?;

        timeout(Duration::from_secs(10), rx)
            .await
            .map_err(|_| anyhow::anyhow!("Cancel order timeout"))?
            .map_err(|e| anyhow::anyhow!("Failed to receive response: {}", e))?
    }

    /// 오더북 조회
    async fn get_orderbook(
        &self,
        trading_pair: &TradingPair,
        depth: Option<usize>,
    ) -> Result<(Vec<OrderEntry>, Vec<OrderEntry>)> {
        let (tx, rx) = oneshot::channel();

        let cmd = OrderCommand::GetOrderbook {
            trading_pair: trading_pair.clone(),
            depth,
            response: tx,
        };

        self.send_order(cmd)
            .map_err(|e| anyhow::anyhow!("Failed to send get_orderbook command: {}", e))?;

        timeout(Duration::from_secs(10), rx)
            .await
            .map_err(|_| anyhow::anyhow!("Get orderbook timeout"))?
            .map_err(|e| anyhow::anyhow!("Failed to receive response: {}", e))?
    }

    /// 최고 매수가 조회
    async fn get_best_bid(&self, trading_pair: &TradingPair) -> Result<Option<Decimal>> {
        let (buy_orders, _) = self.get_orderbook(trading_pair, Some(1)).await?;

        if buy_orders.is_empty() {
            return Ok(None);
        }

        // 첫 번째 주문의 가격 반환
        Ok(buy_orders[0].price)
    }

    /// 최저 매도가 조회
    async fn get_best_ask(&self, trading_pair: &TradingPair) -> Result<Option<Decimal>> {
        let (_, sell_orders) = self.get_orderbook(trading_pair, Some(1)).await?;

        if sell_orders.is_empty() {
            return Ok(None);
        }

        // 첫 번째 주문의 가격 반환
        Ok(sell_orders[0].price)
    }

    /// 잔고 잠금
    async fn lock_balance(&self, user_id: u64, mint: &str, amount: Decimal) -> Result<()> {
        let (tx, rx) = oneshot::channel();

        let cmd = OrderCommand::LockBalance {
            user_id,
            mint: mint.to_string(),
            amount,
            response: tx,
        };

        self.send_order(cmd)
            .map_err(|e| anyhow::anyhow!("Failed to send lock_balance command: {}", e))?;

        timeout(Duration::from_secs(10), rx)
            .await
            .map_err(|_| anyhow::anyhow!("Lock balance timeout"))?
            .map_err(|e| anyhow::anyhow!("Failed to receive response: {}", e))?
    }

    /// 잔고 잠금 해제
    async fn unlock_balance(&self, user_id: u64, mint: &str, amount: Decimal) -> Result<()> {
        let (tx, rx) = oneshot::channel();

        let cmd = OrderCommand::UnlockBalance {
            user_id,
            mint: mint.to_string(),
            amount,
            response: tx,
        };

        self.send_order(cmd)
            .map_err(|e| anyhow::anyhow!("Failed to send unlock_balance command: {}", e))?;

        timeout(Duration::from_secs(10), rx)
            .await
            .map_err(|_| anyhow::anyhow!("Unlock balance timeout"))?
            .map_err(|e| anyhow::anyhow!("Failed to receive response: {}", e))?
    }

    /// 잔고 조회
    async fn get_balance(&self, user_id: u64, mint: &str) -> Result<(Decimal, Decimal)> {
        let (tx, rx) = oneshot::channel();

        let cmd = OrderCommand::GetBalance {
            user_id,
            mint: mint.to_string(),
            response: tx,
        };

        self.send_order(cmd)
            .map_err(|e| anyhow::anyhow!("Failed to send get_balance command: {}", e))?;

        timeout(Duration::from_secs(5), rx)
            .await
            .map_err(|_| anyhow::anyhow!("Get balance timeout"))?
            .map_err(|e| anyhow::anyhow!("Failed to receive response: {}", e))?
    }

    /// 잔고 업데이트 (입금/출금)
    async fn update_balance(
        &self,
        user_id: u64,
        mint: &str,
        available_delta: Decimal,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();

        let cmd = BalanceCommand::UpdateBalance {
            user_id,
            mint: mint.to_string(),
            available_delta,
            response: tx,
        };

        self.send_balance(cmd)
            .map_err(|e| anyhow::anyhow!("Failed to send update_balance command: {}", e))?;

        // 응답은 저널 커밋 뒤에 온다. 시간 초과를 확정 실패로 오인하지 않는다.
        timeout(Duration::from_secs(3), rx)
            .await
            .map_err(|_| anyhow::anyhow!("Update balance timeout"))?
            .map_err(|e| anyhow::anyhow!("Failed to receive response: {}", e))?
    }
}

#[cfg(test)]
mod ownership_handle_tests {
    use super::*;
    use crossbeam::channel::{bounded, Receiver};

    fn handle(
        capacity: usize,
    ) -> (
        EngineHandle,
        Receiver<OrderCommand>,
        Receiver<BalanceCommand>,
    ) {
        let (order_tx, orders) = bounded(capacity);
        let (balance_tx, balances) = bounded(capacity);
        (
            EngineHandle {
                order_tx,
                balance_tx,
                closed: Arc::new(AtomicBool::new(false)),
            },
            orders,
            balances,
        )
    }

    #[tokio::test]
    async fn ownership_handle_queries_enter_queue_without_waiting_for_other_reply() -> Result<()> {
        let (handle, orders, _balances) = handle(2);
        let first = handle.clone();
        let second = handle.clone();
        let a = tokio::spawn(async move { first.get_balance(1, "SOL").await });
        let b = tokio::spawn(async move { second.get_balance(2, "SOL").await });
        // 첫 응답을 주지 않은 상태에서 두 명령이 모두 진입해야 한다.
        // 전역 Mutex가 요청의 await까지 감싸면 두 번째 명령은 들어오지 못한다.
        let entered = timeout(Duration::from_millis(50), async {
            while orders.len() != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        if entered.is_err() {
            a.abort();
            b.abort();
            anyhow::bail!("requests serialized while waiting for a reply");
        }
        let commands: Vec<_> = orders.try_iter().collect();
        for command in commands.into_iter().rev() {
            if let OrderCommand::GetBalance {
                user_id, response, ..
            } = command
            {
                response
                    .send(Ok((Decimal::from(user_id), Decimal::ZERO)))
                    .unwrap();
            } else {
                panic!("unexpected command");
            }
        }
        assert_eq!(a.await??.0, Decimal::ONE);
        assert_eq!(b.await??.0, Decimal::from(2));
        Ok(())
    }

    #[test]
    fn ownership_handle_all_inputs_reject_full_queues_without_blocking() {
        let (handle, orders, balances) = handle(1);
        let (reply, _) = oneshot::channel();
        handle
            .order_tx
            .try_send(OrderCommand::GetBalance {
                user_id: 1,
                mint: "SOL".into(),
                response: reply,
            })
            .unwrap();
        let (reply, _) = oneshot::channel();
        handle
            .balance_tx
            .try_send(BalanceCommand::UpdateBalance {
                user_id: 1,
                mint: "SOL".into(),
                available_delta: Decimal::ONE,
                response: reply,
            })
            .unwrap();
        let (done, result) = std::sync::mpsc::channel();
        // blocking send가 재도입돼도 테스트 러너를 영구 정지시키지 않는다.
        // 제한 시간 후 수신자를 닫아 대기를 해제하고 스레드를 회수한다.
        let worker = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap();
            let errors = runtime.block_on(async {
                let order = OrderEntry {
                    fee_rate: rust_decimal::Decimal::ZERO,
                    id: 1,
                    user_id: 1,
                    order_type: "buy".into(),
                    order_side: "limit".into(),
                    base_mint: "SOL".into(),
                    quote_mint: "USDT".into(),
                    price: Some(Decimal::ONE),
                    amount: Decimal::ONE,
                    quote_amount: None,
                    filled_amount: Decimal::ZERO,
                    remaining_amount: Decimal::ONE,
                    remaining_quote_amount: None,
                    created_at: chrono::Utc::now(),
                };
                let pair = TradingPair::new("SOL".into(), "USDT".into());
                [
                    handle.submit_order(order).await.is_err(),
                    handle.get_balance(1, "SOL").await.is_err(),
                    handle.get_orderbook(&pair, Some(1)).await.is_err(),
                    handle.cancel_order(1, 1, &pair).await.is_err(),
                    handle.lock_balance(1, "SOL", Decimal::ONE).await.is_err(),
                    handle.unlock_balance(1, "SOL", Decimal::ONE).await.is_err(),
                    handle.update_balance(1, "SOL", Decimal::ONE).await.is_err(),
                ]
            });
            let _ = done.send(errors);
        });
        let completed = result.recv_timeout(std::time::Duration::from_secs(1));
        drop(orders);
        drop(balances);
        worker.join().unwrap();
        assert!(completed
            .expect("full queue blocked a caller")
            .into_iter()
            .all(|error| error));
    }

    #[tokio::test]
    async fn ownership_handle_rejects_closed_and_disconnected_channels() {
        let (handle, orders, balances) = handle(1);
        drop(orders);
        drop(balances);
        assert!(handle.get_balance(1, "SOL").await.is_err());
        assert!(handle.update_balance(1, "SOL", Decimal::ONE).await.is_err());
        let (handle, _orders, _balances) = self::handle(1);
        handle.closed.store(true, Ordering::Release);
        assert!(handle
            .get_balance(1, "SOL")
            .await
            .unwrap_err()
            .to_string()
            .contains("closed"));
        assert!(handle.update_balance(1, "SOL", Decimal::ONE).await.is_err());
    }
    fn deposit() -> super::super::DepositCredit {
        super::super::DepositCredit {
            deposit_id: uuid::Uuid::new_v4(),
            user_id: 3,
            asset: "BTC".into(),
            amount: Decimal::ONE,
        }
    }

    #[tokio::test]
    async fn deposit_full_input_queue_is_not_accepted_without_consuming_existing_command() {
        let (handle, _orders, balances) = handle(1);
        handle
            .balance_tx
            .try_send(BalanceCommand::UpdateBalance {
                user_id: 9,
                mint: "BTC".into(),
                available_delta: Decimal::ONE,
                response: oneshot::channel().0,
            })
            .unwrap();
        let result = timeout(Duration::from_millis(100), handle.credit_deposit(deposit()))
            .await
            .expect("포화 큐에서 요청이 대기함");
        assert!(matches!(
            result,
            Err(super::super::DepositCreditError::NotAccepted)
        ));
        assert_eq!(balances.len(), 1);
        assert!(matches!(
            balances.try_recv().unwrap(),
            BalanceCommand::UpdateBalance { user_id: 9, .. }
        ));
    }

    #[tokio::test]
    async fn deposit_closed_or_disconnected_input_is_not_accepted() {
        let (handle, _orders, balances) = handle(1);
        drop(balances);
        assert!(matches!(
            handle.credit_deposit(deposit()).await,
            Err(super::super::DepositCreditError::NotAccepted)
        ));
        let (handle, _orders, balances) = self::handle(1);
        handle.closed.store(true, Ordering::Release);
        assert!(matches!(
            handle.credit_deposit(deposit()).await,
            Err(super::super::DepositCreditError::NotAccepted)
        ));
        assert!(balances.is_empty());
    }

    #[tokio::test]
    async fn deposit_lost_response_after_queue_admission_is_outcome_unknown() -> Result<()> {
        let (handle, _orders, balances) = handle(1);
        let task = tokio::spawn(async move { handle.credit_deposit(deposit()).await });
        timeout(Duration::from_secs(1), async {
            while balances.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        // 명령은 이미 소비됐다. 응답 채널 소유자를 잃어도 확정 거절로 간주하지 않는다.
        drop(balances.try_recv()?);
        assert!(matches!(
            task.await?,
            Err(super::super::DepositCreditError::OutcomeUnknown)
        ));
        Ok(())
    }

}
