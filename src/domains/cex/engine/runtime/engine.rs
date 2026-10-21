//! 실행 생명주기 제어와 거래 상태의 독점 소유권을 연결한다.
//! 요청은 EngineHandle, 상태 변경은 단일 워커가 담당한다.
use super::{
    balance_commands::BalanceCommand, commands::OrderCommand, db_commands::DbCommand,
    handle::EngineHandle,
};
#[cfg(any(test, feature = "bench_mode"))]
use crate::domains::cex::engine::types::{MatchResult, OrderEntry};
use crate::{
    domains::cex::engine::{
        executor::Executor, matcher::Matcher, orderbook::OrderBook, types::TradingPair,
        wal::WalEntry,
    },
    shared::{database::Database, kafka::KafkaProducer},
};
use anyhow::{Context, Result};
use crossbeam::channel::{bounded, Receiver, Sender};
#[cfg(any(test, feature = "bench_mode"))]
use rust_decimal::Decimal;
use std::{
    collections::HashMap,
    sync::{atomic::AtomicBool, Arc},
    thread,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EngineMode {
    Standard,
    Bench,
}
impl EngineMode {
    fn load_from_db(self) -> bool {
        self == Self::Standard
    }
    fn use_wal(self) -> bool {
        self == Self::Standard
    }
    fn use_db_writer(self) -> bool {
        self == Self::Standard
    }
}

/// 공유 참조나 내부 락을 갖지 않는 거래 상태. 시작 전에는 제어 객체가 소유하고
/// 검증 후 워커로 이동한다. 벤치·테스트도 같은 상태를 &mut로 독점 대여한다.
struct EngineState {
    orderbooks: HashMap<TradingPair, OrderBook>,
    matcher: Matcher,
    executor: Executor,
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

/// 서버의 main이 소유하는 생명주기 제어 객체. Clone하지 않는다.
/// 시작 뒤 state는 None이므로 HTTP가 실행 중 상태를 직접 읽거나 수정할 수 없다.
pub struct HighPerformanceEngine {
    handle: EngineHandle,
    state: Option<EngineState>,
    order_rx: Receiver<OrderCommand>,
    balance_rx: Receiver<BalanceCommand>,
    wal_tx: Option<Sender<WalEntry>>,
    wal_rx: Receiver<WalEntry>,
    db_tx: Option<Sender<DbCommand>>,
    db_rx: Receiver<DbCommand>,
    engine_thread: Option<thread::JoinHandle<()>>,
    wal_thread: Option<thread::JoinHandle<()>>,
    db_writer_thread: Option<thread::JoinHandle<()>>,
    running: Arc<AtomicBool>,
    db: Option<Database>,
    wal_dir: std::path::PathBuf,
    mode: EngineMode,
    kafka_producer: Option<Arc<KafkaProducer>>,
}
impl HighPerformanceEngine {
    pub fn new(db: Database) -> Self {
        Self::new_with_mode(db, EngineMode::Standard)
    }
    pub fn new_bench() -> Self {
        Self::new_with_mode(None, EngineMode::Bench)
    }
    /// 기존 bounded 채널을 유지한다. 송신 핸들은 거래 상태를 공유하지 않는다.
    pub fn new_with_mode(db: impl Into<Option<Database>>, mode: EngineMode) -> Self {
        let (order_tx, order_rx) = bounded(100_000);
        let (balance_tx, balance_rx) = bounded(100_000);
        let (wal_tx, wal_rx) = bounded(100_000);
        let (db_tx, db_rx) = bounded(100_000);
        let kafka_producer = if mode == EngineMode::Standard {
            let servers = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
                .unwrap_or_else(|_| "localhost:9092".into());
            match KafkaProducer::new(&servers) {
                Ok(producer) => Some(Arc::new(producer)),
                Err(error) => {
                    eprintln!("Kafka initialization failed: {}", error);
                    None
                }
            }
        } else {
            None
        };
        Self {
            handle: EngineHandle {
                order_tx,
                balance_tx,
                closed: Arc::new(AtomicBool::new(false)),
            },
            state: Some(EngineState::new()),
            order_rx,
            balance_rx,
            wal_tx: if mode.use_wal() { Some(wal_tx) } else { None },
            wal_rx,
            db_tx: if mode.use_db_writer() {
                Some(db_tx)
            } else {
                None
            },
            db_rx,
            engine_thread: None,
            wal_thread: None,
            db_writer_thread: None,
            running: Arc::new(AtomicBool::new(false)),
            db: db.into(),
            wal_dir: std::env::var("WAL_DIR")
                .map(Into::into)
                .unwrap_or_else(|_| "./wal".into()),
            mode,
            kafka_producer,
        }
    }
    /// 요청 서비스에는 이 값만 전달한다. 엔진 시작/종료 권한은 넘기지 않는다.
    pub fn handle(&self) -> EngineHandle {
        self.handle.clone()
    }
    pub async fn start(&mut self) -> Result<()> {
        // 이미 동작하는 상태를 DB 값으로 덮거나 작업 스레드를 중복 생성하지 않는다.
        // &mut self는 같은 객체의 동시 호출을 막지만 순차적인 두 번째 시작은 막지 않는다.
        anyhow::ensure!(
            !self.running.load(std::sync::atomic::Ordering::Acquire),
            "Engine is already running"
        );
        // 정지 시 닫힘 표식을 게시하므로 같은 인스턴스를 재시작하지 않는다.
        // 새 엔진을 생성해 초기 검증을 다시 수행하도록 명시적으로 실패시킨다.
        anyhow::ensure!(
            !self
                .handle
                .closed
                .load(std::sync::atomic::Ordering::Acquire)
                && self.state.is_some(),
            "Stopped engine cannot be restarted; create a new instance"
        );

        eprintln!("[Engine Start] Starting engine initialization...");

        if self.mode.load_from_db() {
            eprintln!("[Engine Start] Loading data from database...");
            let db = self
                .db
                .as_ref()
                .context("Database unavailable in this mode")?;

            eprintln!("[Engine Start] Validating startup snapshot...");
            let balances = super::startup::load_balances(db.pool()).await?;
            // 전체 검증이 성공한 뒤, 아직 이동하지 않은 상태에만 반영한다.
            let state = self.state.as_mut().context("Engine state already moved")?;
            *state.executor.balance_cache_mut() = balances;
            state.orderbooks.clear();
        }

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 3. DB Writer 스레드 시작 (필요한 경우)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if self.mode.use_db_writer() {
            let db_rx = self.db_rx.clone();
            let db_pool = self
                .db
                .as_ref()
                .expect("DB must exist in Standard mode")
                .pool()
                .clone();
            let db_writer_thread = thread::spawn(move || {
                super::threads::db_writer_thread_loop(db_rx, db_pool);
            });
            self.db_writer_thread = Some(db_writer_thread);
        } else {
            self.db_writer_thread = None;
        }

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 4. WAL 스레드 시작 (필요한 경우)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        eprintln!("[Engine Start] Starting WAL thread...");
        if self.mode.use_wal() {
            let wal_rx = self.wal_rx.clone();
            let wal_dir = self.wal_dir.clone();
            let wal_thread = thread::spawn(move || {
                super::threads::wal_thread_loop(wal_rx, wal_dir);
            });
            self.wal_thread = Some(wal_thread);
            eprintln!("[Engine Start] WAL thread started");
        } else {
            self.wal_thread = None;
            eprintln!("[Engine Start] WAL thread disabled");
        }

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 5. 실행 플래그 설정 (스레드 시작 전에 설정)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        self.running
            .store(true, std::sync::atomic::Ordering::Relaxed);

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 6. 엔진 스레드 시작
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        eprintln!("[Engine Start] Starting engine thread...");
        let order_rx = self.order_rx.clone();
        let balance_rx = self.balance_rx.clone();
        let wal_tx = self.wal_tx.clone();
        let db_tx = self.db_tx.clone();
        // take()가 Some을 None으로 바꾼다. 이후 제어 객체에는 거래 상태가
        // 없으며 move 클로저가 세 컴포넌트를 엔진 스레드로 이동시킨다.
        let EngineState {
            orderbooks,
            matcher,
            executor,
        } = self.state.take().context("Engine state already moved")?;
        let running = Arc::clone(&self.running);

        let kafka_producer_for_thread = self.kafka_producer.clone();
        let engine_thread = thread::spawn(move || {
            super::threads::engine_thread_loop(
                order_rx,
                balance_rx,
                wal_tx,
                db_tx,
                kafka_producer_for_thread,
                orderbooks,
                matcher,
                executor,
                running,
            );
        });
        self.engine_thread = Some(engine_thread);
        eprintln!("[Engine Start] Engine thread started");
        eprintln!("[Engine Start] Engine initialization completed successfully");

        Ok(())
    }

    /// 엔진 정지 (내부 구현)
    ///
    /// # 처리 과정
    /// 1. 실행 플래그 해제
    /// 2. 채널 닫기 (스레드 루프 종료)
    /// 3. 엔진 스레드 종료 대기
    /// 4. WAL 스레드 종료 대기
    /// 5. 최종 WAL 동기화
    ///
    /// # Note
    /// 이 메서드는 `Engine` trait의 `stop()`에서 호출됩니다.
    /// `&mut self`를 사용하여 필드를 직접 수정합니다.
    pub async fn stop(&mut self) -> Result<()> {
        eprintln!("[Engine Stop] Starting shutdown...");

        // 1. 실행 플래그 해제
        self.running
            .store(false, std::sync::atomic::Ordering::Relaxed);
        eprintln!("[Engine Stop] Running flag set to false");

        // 복제된 명령 핸들에도 닫힘을 게시한다. 기존 핸들을 가진 호출자는
        // 이후 요청에서 오류를 받는다. 현재 종료 정책은 큐를 비우지 않으므로
        // 영속 처리 위치를 연결하는 후속 작업이 필요하다.
        self.handle
            .closed
            .store(true, std::sync::atomic::Ordering::Release);
        drop(self.wal_tx.take());
        eprintln!("[Engine Stop] wal_tx closed");
        drop(self.db_tx.take());
        eprintln!("[Engine Stop] db_tx closed");

        // 3. 스레드 종료 대기
        eprintln!("[Engine Stop] Waiting for engine thread...");
        if let Some(handle) = self.engine_thread.take() {
            handle
                .join()
                .map_err(|e| anyhow::anyhow!("Engine thread panicked: {:?}", e))?;
            eprintln!("[Engine Stop] Engine thread joined");
        }

        eprintln!("[Engine Stop] Waiting for WAL thread...");
        if let Some(handle) = self.wal_thread.take() {
            handle
                .join()
                .map_err(|e| anyhow::anyhow!("WAL thread panicked: {:?}", e))?;
            eprintln!("[Engine Stop] WAL thread joined");
        }

        eprintln!("[Engine Stop] Waiting for DB Writer thread...");
        if let Some(handle) = self.db_writer_thread.take() {
            handle
                .join()
                .map_err(|e| anyhow::anyhow!("DB Writer thread panicked: {:?}", e))?;
            eprintln!("[Engine Stop] DB Writer thread joined");
        }

        eprintln!("[Engine Stop] Shutdown complete");
        Ok(())
    }

    /// 직접 상태 접근은 워커를 시작하지 않은 벤치에서만 허용한다.
    #[cfg(any(test, feature = "bench_mode"))]
    fn bench_state(&mut self) -> &mut EngineState {
        assert_eq!(
            self.mode,
            EngineMode::Bench,
            "bench state requires Bench mode"
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
        user_id: u64,
        mint: &str,
        available: Decimal,
        locked: Decimal,
    ) {
        self.bench_state()
            .executor
            .balance_cache_mut()
            .set_balance(user_id, mint, available, locked);
    }
    #[cfg(any(test, feature = "bench_mode"))]
    pub fn bench_submit_direct(&mut self, order: OrderEntry) -> Result<Vec<MatchResult>> {
        anyhow::ensure!(
            self.mode == EngineMode::Bench,
            "bench_submit_direct is only available in bench mode"
        );
        let state = self
            .state
            .as_mut()
            .context("Running worker exclusively owns state")?;
        super::threads::process_submit_order(
            order,
            None,
            None,
            None,
            &mut state.orderbooks,
            &state.matcher,
            &mut state.executor,
        )
    }
    #[cfg(test)]
    pub(crate) fn test_queued_orders(&self) -> usize {
        self.order_rx.len()
    }
}
#[cfg(test)]
mod reservation_tests;
#[cfg(test)]
mod settlement_pipeline_tests;
#[cfg(test)]
mod startup_tests;

#[cfg(test)]
mod ownership_tests;
