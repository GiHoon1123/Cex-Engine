//! 거래 상태의 독점 소유와 출력 워커의 생명주기를 연결한다.
use super::{
    balance_commands::BalanceCommand,
    commands::OrderCommand,
    handle::EngineHandle,
    journal::{
        self,
        store::{Ownership, Progress, Store},
        Pending,
    },
    output,
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
    pub withdrawal_receipts: HashMap<uuid::Uuid, super::withdrawal::WithdrawalReceipt>,
    pub deposit_receipts: HashMap<uuid::Uuid, super::deposit::DepositReceipt>,
    pub submissions: HashMap<u64, crate::domains::cex::engine::types::OrderEntry>,
    pub cancellations: HashMap<u64, crate::domains::cex::engine::types::OrderEntry>,
    pub orderbooks: HashMap<TradingPair, OrderBook>,
    pub matcher: Matcher,
    pub executor: Executor,
}
impl EngineState {
    pub(super) fn new() -> Self {
        Self {
            deposit_receipts: HashMap::new(),
            withdrawal_receipts: HashMap::new(),
            submissions: HashMap::new(),
            cancellations: HashMap::new(),
            orderbooks: HashMap::new(),
            matcher: Matcher::new(),
            executor: Executor::new(),
        }
    }
}

/// 서버가 소유하는 제어 객체다. 거래 상태는 코어로 이동하고 저널과 Kafka
/// 워커의 진행 위치·종료만 제어한다. 재시작 상태는 자체 저널에서 검증해 복원한다.
pub struct HighPerformanceEngine {
    handle: EngineHandle,
    state: Option<EngineState>,
    order_rx: Receiver<OrderCommand>,
    balance_rx: Receiver<BalanceCommand>,
    engine_thread: Option<thread::JoinHandle<EngineState>>,
    output_thread: Option<thread::JoinHandle<Result<()>>>,
    journal_thread: Option<thread::JoinHandle<Result<()>>>,
    journal_rx: Option<Receiver<Pending>>,
    progress: Option<Arc<Progress>>,
    ownership: Option<Ownership>,
    data_dir: std::path::PathBuf,
    #[cfg(test)]
    test_directory: Option<tempfile::TempDir>,
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
            #[cfg(test)]
            test_directory: None,
            journal_thread: None,
            journal_rx: None,
            progress: None,
            ownership: None,
            data_dir: std::env::var_os("ENGINE_DATA_DIR")
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| ".engine-data".into()),
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
        let output = if self.mode == EngineMode::Standard {
            let ownership = Ownership::acquire(&self.data_dir)?;
            anyhow::ensure!(
                ownership.fresh || self.data_dir.join("journal.sqlite3").is_file(),
                "Previously owned engine journal is missing; automatic DB bootstrap refused"
            );
            let mut store = Store::open(&self.data_dir)?;
            if !store.initialized()? {
                // 최초 시작점만 검증된 플랫폼 잔액에서 만든다. 기존 접수·활성
                // 주문이 있으면 임의 변환하지 않고 시작을 거부한다.
                let db = self
                    .db
                    .as_ref()
                    .context("Database unavailable in this mode")?;
                let balances = super::startup::load_balances(db.pool()).await?;
                let mut initial = EngineState::new();
                *initial.executor.balance_cache_mut() = balances;
                store.initialize(&initial)?;
            }
            // 이후 재시작은 플랫폼 잔액과 섞지 않는다. 검증 실패 때 DB를 읽는
            // 폴백도 없으며 전체 로그의 결과 일치 검사를 통과해야 시작한다.
            let (recovered, engine_id) = store.recover()?;
            self.state = Some(recovered);
            let progress = Arc::new(Progress::new(store.last_sequence()?, store.delivered()?));
            let servers = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
                .unwrap_or_else(|_| "localhost:9092".into());
            let producer = KafkaProducer::new(&servers)?;
            self.ownership = Some(ownership);
            let (worker, ready) = output::spawn(
                store.delivery_connection()?,
                engine_id,
                progress.clone(),
                self.handle.closed.clone(),
                producer,
            )?;
            self.output_thread = Some(worker);
            self.progress = Some(progress.clone());
            // 미전달 이력을 먼저 재발행한다. 플랫폼이 중복 이벤트를 안전하게
            // 처리할 수 있어야 하며 실패 시 신규 거래를 열지 않는다.
            ready
                .await
                .context("Recovery output worker stopped")?
                .map_err(|error| anyhow::anyhow!(error))?;
            let (sender, receiver) = bounded(100_000);
            self.journal_rx = Some(receiver.clone());
            let closed = self.handle.closed.clone();
            self.journal_thread = Some(thread::spawn(move || {
                journal::store::run(store, receiver, closed, progress)
            }));
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
        self.halt()
    }

    fn halt(&mut self) -> Result<()> {
        self.handle.closed.store(true, Ordering::Release);
        self.running.store(false, Ordering::Release);
        let mut core_error: Option<String> = None;
        if let Some(worker) = self.engine_thread.take() {
            match worker.join() {
                Ok(state) => self.state = Some(state),
                Err(_) => core_error = Some("Engine worker panicked".into()),
            }
        }
        // 코어 송신자 종료 → 저널 큐 커밋 완료 → Kafka 확정 범위 소진 순서다.
        // 오류가 나도 다른 워커를 회수하고 파일 소유권은 마지막에 해제한다.
        if let Some(worker) = self.journal_thread.take() {
            match worker.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => core_error = Some(format!("Journal worker failed: {error:#}")),
                Err(_) => core_error = Some("Journal worker panicked".into()),
            }
        } else if let Some(progress) = &self.progress {
            progress.journal_done.store(true, Ordering::Release);
        }
        if let Some(worker) = self.output_thread.take() {
            match worker.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => core_error = Some(format!("Output worker failed: {error:#}")),
                Err(_) => core_error = Some("Output worker panicked".into()),
            }
        }
        self.ownership.take();
        if let Some(error) = core_error {
            anyhow::bail!("{error}");
        }
        anyhow::ensure!(
            self.journal_rx
                .as_ref()
                .map_or(true, |queue| queue.is_empty()),
            "Journal queue was not drained"
        );
        anyhow::ensure!(
            self.pending_output_count() == 0,
            "Durable output remains pending"
        );
        Ok(())
    }

    /// 영속 명령 중 Kafka 전달 위치가 아직 전진하지 않은 개수다. 체결 이벤트
    /// 수와 다르며 외부 플랫폼의 정산 완료를 의미하지 않는다.
    pub fn pending_output_count(&self) -> usize {
        self.progress
            .as_ref()
            .map(|progress| {
                progress
                    .durable
                    .load(Ordering::Acquire)
                    .saturating_sub(progress.delivered.load(Ordering::Acquire))
                    as usize
            })
            .unwrap_or(0)
    }
    /// 제어 객체의 경로만 제공한다. 실행 상태·채널·코어 생명주기를 조회에 넘기지 않는다.
    pub fn journal_directory(&self) -> &std::path::Path {
        &self.data_dir
    }

    /// 배포와 격리 검증은 명시한 영속 디렉터리를 쓴다. 실행 중에는 바꾸지 않는다.
    pub fn with_data_dir(mut self, directory: impl Into<std::path::PathBuf>) -> Self {
        assert!(!self.running.load(Ordering::Acquire));
        self.data_dir = directory.into();
        self
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

#[cfg(test)]
mod journal_tests;

/// 명시적인 종료 호출을 빠뜨려도 워커가 분리된 채 계속 거래하지 않게 한다.
/// 서버의 정상 종료는 stop으로 오류를 보고하며 이 경로는 마지막 자원 회수다.
impl Drop for HighPerformanceEngine {
    fn drop(&mut self) {
        let _ = self.halt();
    }
}
