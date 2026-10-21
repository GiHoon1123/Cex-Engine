//! 거래 상태의 독점 소유와 출력 워커의 생명주기를 연결한다.
use super::{
    balance_commands::BalanceCommand,
    commands::OrderCommand,
    handle::EngineHandle,
    output::{self, FailedOutput, OutputBatch},
};
#[cfg(any(test, feature = "bench_mode"))]
use crate::domains::cex::engine::types::{MatchResult, OrderEntry};
use crate::{
    domains::cex::engine::{
        executor::Executor, matcher::Matcher, orderbook::OrderBook, types::TradingPair,
    },
    shared::{database::Database, kafka::KafkaProducer},
};
use anyhow::{Context, Result};
use crossbeam::channel::{bounded, Receiver};
#[cfg(any(test, feature = "bench_mode"))]
use rust_decimal::Decimal;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EngineMode {
    Standard,
    Bench,
}

/// 실행 중에는 엔진 워커만 보유하고 정지 후 제어 객체로 되돌려준다.
/// 상태 공유 참조나 내부 락을 갖지 않는다.
pub(super) struct EngineState {
    pub orderbooks: HashMap<TradingPair, OrderBook>,
    pub matcher: Matcher,
    pub executor: Executor,
}
impl EngineState {
    fn new() -> Self {
        Self {
            orderbooks: HashMap::new(),
            matcher: Matcher::new(),
            executor: Executor::new(),
        }
    }
}

/// 서버가 소유하는 제어 객체다. 출력 실패 시 실패 배치와 남은 수신 큐를
/// 보관한다. 이 메모리 보존을 프로세스 장애에 대한 영속 복구로 설명하지 않는다.
pub struct HighPerformanceEngine {
    handle: EngineHandle,
    state: Option<EngineState>,
    order_rx: Receiver<OrderCommand>,
    balance_rx: Receiver<BalanceCommand>,
    engine_thread: Option<thread::JoinHandle<EngineState>>,
    output_thread: Option<thread::JoinHandle<Option<FailedOutput>>>,
    output_rx: Option<Receiver<OutputBatch>>,
    output_failure: Option<FailedOutput>,
    running: Arc<AtomicBool>,
    db: Option<Database>,
    mode: EngineMode,
}
impl HighPerformanceEngine {
    pub fn new(db: Database) -> Self {
        Self::new_with_mode(db, EngineMode::Standard)
    }
    pub fn new_bench() -> Self {
        Self::new_with_mode(None, EngineMode::Bench)
    }
    pub fn new_with_mode(db: impl Into<Option<Database>>, mode: EngineMode) -> Self {
        let (order_tx, order_rx) = bounded(100_000);
        let (balance_tx, balance_rx) = bounded(100_000);
        Self {
            handle: EngineHandle {
                order_tx,
                balance_tx,
                closed: Arc::new(AtomicBool::new(false)),
            },
            state: Some(EngineState::new()),
            order_rx,
            balance_rx,
            engine_thread: None,
            output_thread: None,
            output_rx: None,
            output_failure: None,
            running: Arc::new(AtomicBool::new(false)),
            db: db.into(),
            mode,
        }
    }
    pub fn handle(&self) -> EngineHandle {
        self.handle.clone()
    }

    pub async fn start(&mut self) -> Result<()> {
        anyhow::ensure!(
            !self.running.load(Ordering::Acquire),
            "Engine is already running"
        );
        anyhow::ensure!(
            !self.handle.closed.load(Ordering::Acquire) && self.state.is_some(),
            "Stopped engine cannot be restarted; create a new instance"
        );
        if self.mode == EngineMode::Standard {
            let db = self
                .db
                .as_ref()
                .context("Database unavailable in this mode")?;
            let balances = super::startup::load_balances(db.pool()).await?;
            let state = self.state.as_mut().context("Engine state already moved")?;
            *state.executor.balance_cache_mut() = balances;
            state.orderbooks.clear();
        }
        // 상태를 이동하기 전에 출력 워커 생성을 완료한다.
        let output = if self.mode == EngineMode::Standard {
            let servers = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
                .unwrap_or_else(|_| "localhost:9092".into());
            // 생성 실패를 무시한 채 금융 결과 없는 엔진을 시작하지 않는다.
            let producer = KafkaProducer::new(&servers)?;
            let (sender, receiver) = bounded(100_000);
            self.output_rx = Some(receiver.clone());
            self.output_thread = Some(output::spawn(
                receiver,
                self.handle.closed.clone(),
                producer,
            )?);
            Some(sender)
        } else {
            None
        };
        self.running.store(true, Ordering::Release);
        let state = self.state.take().context("Engine state already moved")?;
        let orders = self.order_rx.clone();
        let balances = self.balance_rx.clone();
        let running = self.running.clone();
        let closed = self.handle.closed.clone();
        self.engine_thread = Some(thread::spawn(move || {
            super::threads::engine_thread_loop(orders, balances, output, state, running, closed)
        }));
        Ok(())
    }

    pub async fn stop(&mut self) -> Result<()> {
        self.handle.closed.store(true, Ordering::Release);
        self.running.store(false, Ordering::Release);
        let mut core_error = None;
        if let Some(worker) = self.engine_thread.take() {
            match worker.join() {
                Ok(state) => self.state = Some(state),
                Err(_) => core_error = Some("Engine worker panicked"),
            }
        }
        // 엔진을 먼저 회수해 유일한 출력 송신자를 닫은 뒤 출력 큐를 비운다.
        // 한 워커의 실패 때문에 다른 워커의 종료 확인을 건너뛰지 않는다.
        if let Some(worker) = self.output_thread.take() {
            match worker.join() {
                Ok(failure) => self.output_failure = failure,
                Err(_) => core_error = Some("Output worker panicked"),
            }
        }
        if let Some(failure) = &self.output_failure {
            anyhow::bail!(
                "Output delivery failed at event {}: {}; {} output batches retained in memory",
                failure.next_event,
                failure.reason,
                self.pending_output_count()
            );
        }
        if let Some(error) = core_error {
            anyhow::bail!("{error}");
        }
        anyhow::ensure!(
            self.pending_output_count() == 0,
            "Output queue was not drained"
        );
        Ok(())
    }

    /// 큐와 회수한 실패 배치의 개수다. 워커가 처리 중인 배치는 포함하지 않는다.
    /// 이 개수는 디스크에 보존됐다는 의미가 아니다.
    pub fn pending_output_count(&self) -> usize {
        self.output_rx
            .as_ref()
            .map(|receiver| receiver.len())
            .unwrap_or(0)
            + usize::from(self.output_failure.is_some())
    }
    #[cfg(any(test, feature = "bench_mode"))]
    fn bench_state(&mut self) -> &mut EngineState {
        assert_eq!(
            self.mode,
            EngineMode::Bench,
            "bench state requires Bench mode"
        );
        assert!(
            !self.running.load(Ordering::Acquire),
            "running worker exclusively owns state"
        );
        self.state
            .as_mut()
            .expect("running worker exclusively owns state")
    }
    #[cfg(any(test, feature = "bench_mode"))]
    pub fn bench_clear_balances(&mut self) {
        self.bench_state().executor.balance_cache_mut().clear();
    }
    #[cfg(any(test, feature = "bench_mode"))]
    pub fn bench_clear_orderbooks(&mut self) {
        self.bench_state().orderbooks.clear();
    }
    #[cfg(any(test, feature = "bench_mode"))]
    pub fn bench_set_balance(
        &mut self,
        user: u64,
        asset: &str,
        available: Decimal,
        locked: Decimal,
    ) {
        self.bench_state()
            .executor
            .balance_cache_mut()
            .set_balance(user, asset, available, locked);
    }
    #[cfg(any(test, feature = "bench_mode"))]
    pub fn bench_submit_direct(&mut self, order: OrderEntry) -> Result<Vec<MatchResult>> {
        anyhow::ensure!(
            self.mode == EngineMode::Bench,
            "bench_submit_direct is only available in bench mode"
        );
        let state = self.bench_state();
        Ok(super::submission::apply(
            order,
            &mut state.orderbooks,
            &state.matcher,
            &mut state.executor,
        )?
        .matches)
    }
    #[cfg(test)]
    pub(crate) fn test_queued_orders(&self) -> usize {
        self.order_rx.len()
    }
}
#[cfg(test)]
mod output_tests;
#[cfg(test)]
mod ownership_tests;
#[cfg(test)]
mod reservation_tests;
#[cfg(test)]
mod settlement_pipeline_tests;
#[cfg(test)]
mod startup_tests;
