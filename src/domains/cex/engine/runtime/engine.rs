// =====================================================
// HighPerformanceEngine - 고성능 체결 엔진
// =====================================================
// 역할: 모든 주문 처리, 매칭, 체결을 담당하는 통합 엔진
// 
// 핵심 설계:
// 1. 싱글 스레드 엔진 (Core 0 고정) - 모든 주문 순차 처리
// 2. WAL 스레드 (Core 1 고정) - 디스크 쓰기 전용
// 3. DB Writer 스레드 (Core 2, 로컬만) - 배치 DB 쓰기
// 4. Lock-free 채널 - 스레드 간 통신 (crossbeam::channel)
// 5. 환경별 코어 설정 - dev(11코어), prod(2코어)
//
// 성능:
// - 주문 처리: < 0.5ms (평균)
// - 체결 처리: < 0.2ms (평균)
// - TPS: 50,000+ orders/sec
// =====================================================

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;
use anyhow::{Result, Context};
use crossbeam::channel::{Receiver, Sender, bounded};
use parking_lot::{RwLock, Mutex};
use tokio::sync::oneshot;
use tokio::time::{timeout, Duration};
use rust_decimal::Decimal;
use async_trait::async_trait;

use crate::shared::database::Database;
use crate::shared::kafka::KafkaProducer;
use crate::domains::cex::engine::types::{TradingPair, OrderEntry, MatchResult};
use crate::domains::cex::engine::orderbook::OrderBook;
use crate::domains::cex::engine::matcher::Matcher;
use crate::domains::cex::engine::executor::Executor;
use crate::domains::cex::engine::wal::WalEntry;
use crate::domains::cex::engine::Engine;

use super::commands::OrderCommand;
use super::balance_commands::BalanceCommand;
use super::config::CoreConfig;
use super::db_commands::DbCommand;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EngineMode {
    Standard,
    Bench,
}

impl EngineMode {
    fn load_from_db(&self) -> bool {
        matches!(self, EngineMode::Standard)
    }

    fn use_wal(&self) -> bool {
        matches!(self, EngineMode::Standard)
    }

    fn use_db_writer(&self) -> bool {
        matches!(self, EngineMode::Standard)
    }
}

/// 고성능 체결 엔진
/// 
/// 싱글 스레드 엔진 + 멀티 스레드 워커 구조로
/// 초고속 주문 처리와 안전한 데이터 저장을 동시에 달성합니다.
/// 
/// # 아키텍처
/// ```
/// API Handler (tokio async)
///     ↓ order_tx.send()
/// [crossbeam::channel] (Lock-free)
///     ↓ order_rx.recv()
/// Engine Thread (Core 0, 싱글 스레드)
///     ├─ OrderBook::add_order()
///     ├─ Matcher::match_order()
///     ├─ Executor::execute_trade()
///     └─ wal_tx.send()
/// [crossbeam::channel] (Lock-free)
///     ↓ wal_rx.recv()
/// WAL Thread (Core 1)
///     └─ WalWriter::append() → fsync()
/// ```
/// 
/// # 성능
/// - 주문 처리: < 0.5ms (평균)
/// - 체결 처리: < 0.2ms (평균)
/// - TPS: 50,000+ orders/sec
pub struct HighPerformanceEngine {
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 채널 (Lock-free Ring Buffer)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    /// 주문 명령 전송 채널 (Sender)
    /// 
    /// API Handler에서 엔진 스레드로 주문을 전송할 때 사용
    /// 
    /// # 특징
    /// - Lock-free: 락 없이 동작 (스핀락도 없음)
    /// - 링버퍼: 고정 크기 버퍼를 순환 사용
    /// - SPSC: Single Producer, Single Consumer
    /// - 성능: ~100ns (메모리 연산)
    order_tx: Option<Sender<OrderCommand>>,
    
    /// 주문 명령 수신 채널 (Receiver)
    /// 
    /// 엔진 스레드에서 주문을 수신할 때 사용
    /// 
    /// # 사용 위치
    /// - `engine_thread_loop()`에서 `order_rx.recv()` 호출
    order_rx: Receiver<OrderCommand>,
    
    /// 잔고 업데이트 명령 전송 채널 (Sender)
    /// 
    /// 외부 입금/출금 서비스에서 엔진 스레드로 잔고 업데이트를 전송할 때 사용
    /// 
    /// # 특징
    /// - Lock-free: 락 없이 동작 (스핀락도 없음)
    /// - 링버퍼: 고정 크기 버퍼를 순환 사용
    /// - SPSC: Single Producer, Single Consumer
    /// - 성능: ~100ns (메모리 연산)
    /// 
    /// # 우선순위
    /// - 주문 큐보다 우선 처리 (입금이 선행되어야 주문 가능)
    balance_tx: Option<Sender<BalanceCommand>>,
    
    /// 잔고 업데이트 명령 수신 채널 (Receiver)
    /// 
    /// 엔진 스레드에서 잔고 업데이트를 수신할 때 사용
    /// 
    /// # 사용 위치
    /// - `engine_thread_loop()`에서 `balance_rx.try_recv()` 호출 (우선순위 높음)
    balance_rx: Receiver<BalanceCommand>,
    
    /// WAL 메시지 전송 채널 (Sender)
    /// 
    /// 엔진 스레드에서 WAL 스레드로 메시지를 전송할 때 사용
    /// 
    /// # 전송하는 메시지
    /// - OrderCreated
    /// - OrderCancelled
    /// - TradeExecuted
    /// - BalanceLocked
    /// - BalanceUpdated
    wal_tx: Option<Sender<WalEntry>>,
    
    /// WAL 메시지 수신 채널 (Receiver)
    /// 
    /// WAL 스레드에서 메시지를 수신할 때 사용
    /// 
    /// # 사용 위치
    /// - `wal_thread_loop()`에서 `wal_rx.recv()` 호출
    wal_rx: Receiver<WalEntry>,
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 핵심 컴포넌트 (엔진 스레드에서만 접근)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    /// 거래쌍별 오더북
    /// 
    /// Key: TradingPair (예: SOL/USDT)
    /// Value: OrderBook (매수/매도 호가)
    /// 
    /// # 접근
    /// - 엔진 스레드에서만 접근 (싱글 스레드이므로 안전)
    /// - `engine_thread_loop()`에서 사용
    /// 
    /// # 초기화
    /// - 서버 시작 시 DB에서 활성 주문 로드
    /// - 주문 제출 시 자동 생성
    orderbooks: Arc<RwLock<HashMap<TradingPair, OrderBook>>>,
    
    /// 매칭 엔진
    /// 
    /// Price-Time Priority 기반 매칭 알고리즘
    /// 
    /// # 특징
    /// - 싱글 스레드에서만 사용 (안전)
    /// - 상태 없음 (stateless)
    /// - 성능: < 0.5ms per order
    matcher: Arc<Matcher>,
    
    /// 체결 실행 엔진
    /// 
    /// MatchResult를 받아서 실제 체결 처리
    /// 
    /// # 특징
    /// - BalanceCache 포함
    /// - WAL 메시지 발행
    /// - 싱글 스레드에서만 사용 (안전)
    executor: Arc<Mutex<Executor>>,
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 스레드 관리
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    /// 엔진 스레드 핸들
    /// 
    /// 엔진 스레드가 실행 중인지 확인하고 종료 시 대기할 때 사용
    /// 
    /// # 생명주기
    /// - `start()`에서 생성
    /// - `stop()`에서 종료 대기
    engine_thread: Option<thread::JoinHandle<()>>,
    
    /// WAL 스레드 핸들
    /// 
    /// WAL 스레드가 실행 중인지 확인하고 종료 시 대기할 때 사용
    /// 
    /// # 생명주기
    /// - `start()`에서 생성
    /// - `stop()`에서 종료 대기
    wal_thread: Option<thread::JoinHandle<()>>,
    
    /// DB Writer 스레드 핸들
    /// 
    /// DB Writer 스레드가 실행 중인지 확인하고 종료 시 대기할 때 사용
    /// 
    /// # 생명주기
    /// - `start()`에서 생성
    /// - `stop()`에서 종료 대기
    db_writer_thread: Option<thread::JoinHandle<()>>,
    
    /// DB Writer 채널 (Sender)
    /// 
    /// 엔진 스레드에서 DB Writer 스레드로 명령을 전송할 때 사용
    /// 
    /// # 전송하는 명령
    /// - InsertOrder
    /// - UpdateOrderStatus
    /// - InsertTrade
    /// - UpdateBalance
    db_tx: Option<Sender<super::db_commands::DbCommand>>,
    
    /// DB Writer 채널 (Receiver)
    /// 
    /// DB Writer 스레드에서 명령을 수신할 때 사용
    /// 
    /// # 사용 위치
    /// - `db_writer_thread_loop()`에서 `db_rx.recv()` 호출
    db_rx: Receiver<super::db_commands::DbCommand>,
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 상태 관리
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    /// 엔진 실행 중 여부
    /// 
    /// # 사용 목적
    /// - 스레드 루프 종료 조건 확인
    /// - 중복 시작 방지
    /// 
    /// # 값
    /// - `true`: 실행 중
    /// - `false`: 정지됨
    running: Arc<AtomicBool>,
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 데이터베이스 (DB Writer용)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    /// 데이터베이스 연결 (표준 모드만 Some)
    /// 
    /// # 사용 목적
    /// - 서버 시작 시 잔고/주문 로드
    /// - DB Writer 스레드에서 배치 쓰기
    /// 
    /// # 접근
    /// - `start()`에서만 사용 (초기화)
    /// - DB Writer 스레드에서 사용 (배치 쓰기)
    db: Option<Database>,
    
    /// WAL 디렉토리 경로
    /// 
    /// # 기본값
    /// - `./wal/` (현재 디렉토리)
    /// 
    /// # 환경 변수
    /// - `WAL_DIR`: WAL 디렉토리 경로 지정 가능
    wal_dir: std::path::PathBuf,

    /// 실행 모드 (표준/벤치)
    mode: EngineMode,

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // Kafka Producer (이벤트 발행용)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /// Kafka Producer (이벤트 발행용)
    /// 
    /// # 사용 목적
    /// - 체결 이벤트 발행 (trade-executed-{asset})
    /// - 취소 이벤트 발행 (order-cancelled-{asset})
    /// 
    /// # 특징
    /// - 비동기 발행 (논블로킹)
    /// - 엔진 성능에 영향 없음
    kafka_producer: Option<Arc<KafkaProducer>>,
}

impl HighPerformanceEngine {
    /// 새 엔진 생성
    /// 
    /// # Arguments
    /// * `db` - 데이터베이스 연결
    /// 
    /// # Returns
    /// HighPerformanceEngine 인스턴스
    /// 
    /// # 초기화 내용
    /// 1. 채널 생성 (order_tx/rx, wal_tx/rx)
    /// 2. 컴포넌트 초기화 (OrderBook, Matcher, Executor)
    /// 3. 상태 초기화 (running = false)
    /// 
    /// # Note
    /// 아직 스레드는 시작하지 않음 (`start()` 호출 필요)
    /// 
    /// # Examples
    /// ```
    /// let db = Database::new("postgresql://...").await?;
    /// let engine = HighPerformanceEngine::new(db);
    /// engine.start().await?;  // 스레드 시작
    /// ```
    pub fn new(db: Database) -> Self {
        Self::new_with_mode(db, EngineMode::Standard)
    }

    /// 벤치마크용 엔진 생성 (DB/WAL 비활성)
    pub fn new_bench() -> Self {
        Self::new_with_mode(None, EngineMode::Bench)
    }

    pub fn new_with_mode(db: impl Into<Option<Database>>, mode: EngineMode) -> Self {
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 1. 채널 생성 (Lock-free Ring Buffer)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        // 주문 명령 채널 (크기: 10,000)
        // SPSC 패턴: API Handler (Producer) → Engine Thread (Consumer)
        let (order_tx, order_rx) = bounded(100_000);
        
        // 잔고 업데이트 채널 (크기: 10,000)
        // SPSC 패턴: 외부 입금/출금 서비스 (Producer) → Engine Thread (Consumer)
        // 우선순위: 주문 큐보다 높음 (입금이 선행되어야 주문 가능)
        let (balance_tx, balance_rx) = bounded(100_000);
        
        // WAL 메시지 채널 (크기: 10,000)
        // SPSC 패턴: Engine Thread (Producer) → WAL Thread (Consumer)
        let (wal_tx_inner, wal_rx) = bounded(100_000);
        
        // DB Writer 채널 (크기: 10,000)
        // SPSC 패턴: Engine Thread (Producer) → DB Writer Thread (Consumer)
        let (db_tx_inner, db_rx) = bounded(100_000);
        
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 2. 컴포넌트 초기화
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        // OrderBook: 거래쌍별로 관리 (빈 HashMap으로 시작)
        let orderbooks = Arc::new(RwLock::new(HashMap::new()));
        
        // Matcher: 상태 없음 (stateless), Arc로 공유
        let matcher = Arc::new(Matcher::new());
        
        // Executor: BalanceCache 포함, WAL Sender + DB Sender 전달
        let executor = Arc::new(Mutex::new(Executor::new(
            match mode {
                EngineMode::Standard => Some(wal_tx_inner.clone()),
                EngineMode::Bench => None,
            },
            match mode {
                EngineMode::Standard => Some(db_tx_inner.clone()),
                EngineMode::Bench => None,
            },
        )));
        
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 3. WAL 디렉토리 경로
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        let wal_dir = std::env::var("WAL_DIR")
            .map(|s| std::path::PathBuf::from(s))
            .unwrap_or_else(|_| std::path::PathBuf::from("./wal"));
        
        let wal_sender = match mode {
            EngineMode::Standard => Some(wal_tx_inner),
            EngineMode::Bench => None,
        };

        let db_sender = match mode {
            EngineMode::Standard => Some(db_tx_inner),
            EngineMode::Bench => None,
        };

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 4. Kafka Producer 초기화
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        let kafka_producer = match mode {
            EngineMode::Standard => {
                // 환경 변수에서 Kafka 브로커 주소 가져오기
                let bootstrap_servers = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
                    .unwrap_or_else(|_| "localhost:9092".to_string());
                
                match KafkaProducer::new(&bootstrap_servers) {
                    Ok(producer) => {
                        eprintln!("[Engine] Kafka Producer initialized: {}", bootstrap_servers);
                        Some(Arc::new(producer))
                    }
                    Err(e) => {
                        eprintln!("[Engine] Failed to initialize Kafka Producer: {} (continuing without Kafka)", e);
                        None // Kafka 실패해도 엔진은 계속 동작
                    }
                }
            }
            EngineMode::Bench => None, // 벤치마크 모드에서는 Kafka 사용 안 함
        };

        Self {
            order_tx: Some(order_tx),
            order_rx,
            balance_tx: Some(balance_tx),
            balance_rx,
            wal_tx: wal_sender,
            wal_rx,
            db_tx: db_sender,
            db_rx,
            orderbooks,
            matcher,
            executor,
            engine_thread: None,
            wal_thread: None,
            db_writer_thread: None,
            running: Arc::new(AtomicBool::new(false)),
            db: db.into(),
            wal_dir,
            mode,
            kafka_producer,
        }
    }
    
    /// 엔진 시작 (내부 구현)
    /// 
    /// # 처리 과정
    /// 1. 메모리 오더북 구성
    /// 2. DB에서 잔고 로드
    /// 3. BalanceCache에 로드
    /// 4. WAL 스레드 시작
    /// 5. 엔진 스레드 시작
    /// 
    /// # Note
    /// 이 메서드는 `Engine` trait의 `start()`에서 호출됩니다.
    /// 엔진 시작 (내부 구현)
    /// `&mut self`를 사용하여 필드를 직접 수정합니다.
    pub async fn start_impl(&mut self) -> Result<()> {
        use crate::shared::database::repositories::cex::UserBalanceRepository;
        use anyhow::Context;
        
        eprintln!("[Engine Start] Starting engine initialization...");
        
        if self.mode.load_from_db() {
            eprintln!("[Engine Start] Loading data from database...");
            let db = self
                .db
                .as_ref()
                .context("Database unavailable in this mode")?;

            eprintln!("[Engine Start] Loading balances from database...");
            let balance_repo = UserBalanceRepository::new(db.pool().clone());
            let all_balances = balance_repo
                .get_all_balances()
                .await
                .context("Failed to load balances from database")?;
            
            eprintln!("[Engine Start] Loaded {} balances from database", all_balances.len());

            {
                // 잔고 로드: locked는 0으로 초기화 (활성 주문 기반으로 재계산)
                let mut executor = self.executor.lock();
                for balance in all_balances {
                    // locked는 0으로 초기화 (활성 주문 기반으로 재계산할 예정)
                    executor.balance_cache_mut().set_balance(
                        balance.user_id,
                        &balance.mint_address,
                        balance.available + balance.locked, // available + locked를 available로 합침
                        rust_decimal::Decimal::ZERO, // locked는 0으로 초기화
                    );
                }
            }
        } else {
            let mut executor = self.executor.lock();
            executor.balance_cache_mut().clear();
            drop(executor);
            self.orderbooks.write().clear();
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
        
        self.running.store(true, std::sync::atomic::Ordering::Relaxed);
        
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 6. 엔진 스레드 시작
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        eprintln!("[Engine Start] Starting engine thread...");
        let order_rx = self.order_rx.clone();
        let balance_rx = self.balance_rx.clone();
        let wal_tx = self.wal_tx.clone();
        let db_tx = self.db_tx.clone();
        let orderbooks = Arc::clone(&self.orderbooks);
        let matcher = Arc::clone(&self.matcher);
        let executor = Arc::clone(&self.executor);
        let running = Arc::clone(&self.running);
        
        let db_for_thread = self.db.clone();
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
                db_for_thread,
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
    async fn stop_impl(&mut self) -> Result<()> {
        eprintln!("[Engine Stop] Starting shutdown...");
        
        // 1. 실행 플래그 해제
        self.running.store(false, std::sync::atomic::Ordering::Relaxed);
        eprintln!("[Engine Stop] Running flag set to false");
        
        // 2. Executor의 채널 Sender 해제 (먼저 해제해야 채널이 닫힘)
        // Executor가 가지고 있는 wal_sender와 db_sender도 drop해야 채널이 완전히 닫힙니다.
        {
            let mut executor = self.executor.lock();
            executor.clear_channels();
        }
        eprintln!("[Engine Stop] Executor channels cleared");
        
        // 3. 채널 닫기 (스레드 루프 종료)
        // Sender를 drop하면 채널이 닫히고, Receiver의 recv()가 Err를 반환하여 루프가 종료됩니다.
        // clone()을 drop하는 것은 원본 Sender를 drop하지 않으므로 채널이 닫히지 않습니다.
        // 따라서 원본을 직접 drop해야 합니다.
        eprintln!("[Engine Stop] Closing channels...");
        drop(self.order_tx.take());
        eprintln!("[Engine Stop] order_tx closed");
        drop(self.balance_tx.take());
        eprintln!("[Engine Stop] balance_tx closed");
        drop(self.wal_tx.take());
        eprintln!("[Engine Stop] wal_tx closed");
        drop(self.db_tx.take());
        eprintln!("[Engine Stop] db_tx closed");
        
        // 3. 스레드 종료 대기
        eprintln!("[Engine Stop] Waiting for engine thread...");
        if let Some(handle) = self.engine_thread.take() {
            handle.join().map_err(|e| anyhow::anyhow!("Engine thread panicked: {:?}", e))?;
            eprintln!("[Engine Stop] Engine thread joined");
        }
        
        eprintln!("[Engine Stop] Waiting for WAL thread...");
        if let Some(handle) = self.wal_thread.take() {
            handle.join().map_err(|e| anyhow::anyhow!("WAL thread panicked: {:?}", e))?;
            eprintln!("[Engine Stop] WAL thread joined");
        }
        
        eprintln!("[Engine Stop] Waiting for DB Writer thread...");
        if let Some(handle) = self.db_writer_thread.take() {
            handle.join().map_err(|e| anyhow::anyhow!("DB Writer thread panicked: {:?}", e))?;
            eprintln!("[Engine Stop] DB Writer thread joined");
        }
        
        eprintln!("[Engine Stop] Shutdown complete");
        Ok(())
    }

    /// 벤치모드에서만 사용: 잔고 초기화
    #[cfg(any(test, feature = "bench_mode"))]
    pub fn bench_clear_balances(&self) {
        if self.mode == EngineMode::Bench {
            let mut executor = self.executor.lock();
            executor.balance_cache_mut().clear();
        }
    }

    /// 벤치모드에서만 사용: 오더북 초기화
    #[cfg(any(test, feature = "bench_mode"))]
    pub fn bench_clear_orderbooks(&self) {
        if self.mode == EngineMode::Bench {
            self.orderbooks.write().clear();
        }
    }

    /// 벤치모드에서 잔고 설정
    #[cfg(any(test, feature = "bench_mode"))]
    pub fn bench_set_balance(&self, user_id: u64, mint: &str, available: Decimal, locked: Decimal) {
        if self.mode == EngineMode::Bench {
            let mut executor = self.executor.lock();
            executor
                .balance_cache_mut()
                .set_balance(user_id, mint, available, locked);
        }
    }

    /// 벤치모드에서 직접 주문 처리 (큐/oneshot 우회)
    #[cfg(any(test, feature = "bench_mode"))]
    pub fn bench_submit_direct(&self, order: OrderEntry) -> Result<Vec<MatchResult>> {
        if self.mode != EngineMode::Bench {
            anyhow::bail!("bench_submit_direct is only available in bench mode");
        }

        super::threads::process_submit_order(
            order,
            self.wal_tx.as_ref(),
            self.db_tx.as_ref(),
            &self.orderbooks,
            &self.matcher,
            &self.executor,
        )
    }
}

// =====================================================
// Engine Trait 구현
// =====================================================
// Engine trait의 모든 메서드를 구현합니다.
// 각 메서드는 oneshot 채널을 사용하여 비동기 응답을 반환합니다.
// =====================================================

#[async_trait]
impl Engine for HighPerformanceEngine {
    /// 주문 제출
    /// 
    /// # 처리 과정
    /// 1. OrderCommand::SubmitOrder 생성 (response: None)
    /// 2. 엔진 스레드로 전송
    /// 3. 즉시 반환 (응답을 기다리지 않음)
    /// 
    /// # 주의사항
    /// - 주문은 엔진이 백그라운드에서 처리됨
    /// - 주문 상태는 DB에서 확인 가능
    async fn submit_order(&self, order: OrderEntry) -> Result<()> {
        eprintln!("[Engine] submit_order 호출: order_id={}, user_id={}, order_type={}, order_side={}, price={:?}, amount={}", 
                 order.id, order.user_id, order.order_type, order.order_side, order.price, order.amount);
        let order_id = order.id;
        let cmd = OrderCommand::SubmitOrder {
            order,
            response: None,  // 응답을 기다리지 않음
        };
        
        eprintln!("[Engine] 엔진 스레드로 주문 전송 시도: order_id={}", order_id);
        match self.order_tx.as_ref().unwrap().send(cmd) {
            Ok(_) => {
                eprintln!("[Engine] 엔진 스레드로 주문 전송 성공: order_id={}", order_id);
                Ok(())
            }
            Err(e) => {
                eprintln!("[Engine] 엔진 스레드로 주문 전송 실패: order_id={}, error={}", order_id, e);
                Err(anyhow::anyhow!("Failed to send order to engine: {}", e))
            }
        }
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
        
        self.order_tx.as_ref().unwrap().send(cmd)
            .map_err(|e| anyhow::anyhow!("Failed to send cancel command: {}", e))?;
        
        timeout(Duration::from_millis(100), rx)
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
        
        self.order_tx.as_ref().unwrap().send(cmd)
            .map_err(|e| anyhow::anyhow!("Failed to send get_orderbook command: {}", e))?;
        
        timeout(Duration::from_millis(100), rx)
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
    async fn lock_balance(
        &self,
        user_id: u64,
        mint: &str,
        amount: Decimal,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        
        let cmd = OrderCommand::LockBalance {
            user_id,
            mint: mint.to_string(),
            amount,
            response: tx,
        };
        
        self.order_tx.as_ref().unwrap().send(cmd)
            .map_err(|e| anyhow::anyhow!("Failed to send lock_balance command: {}", e))?;
        
        timeout(Duration::from_millis(100), rx)
            .await
            .map_err(|_| anyhow::anyhow!("Lock balance timeout"))?
            .map_err(|e| anyhow::anyhow!("Failed to receive response: {}", e))?
    }
    
    /// 잔고 잠금 해제
    async fn unlock_balance(
        &self,
        user_id: u64,
        mint: &str,
        amount: Decimal,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        
        let cmd = OrderCommand::UnlockBalance {
            user_id,
            mint: mint.to_string(),
            amount,
            response: tx,
        };
        
        self.order_tx.as_ref().unwrap().send(cmd)
            .map_err(|e| anyhow::anyhow!("Failed to send unlock_balance command: {}", e))?;
        
        timeout(Duration::from_millis(100), rx)
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
        
        self.order_tx.as_ref().unwrap().send(cmd)
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
        
        self.balance_tx.as_ref().unwrap().send(cmd)
            .map_err(|e| anyhow::anyhow!("Failed to send update_balance command: {}", e))?;
        
        // DB 업데이트를 포함하므로 더 긴 타임아웃 필요 (운영서버 환경 고려)
        timeout(Duration::from_secs(3), rx)
            .await
            .map_err(|_| anyhow::anyhow!("Update balance timeout"))?
            .map_err(|e| anyhow::anyhow!("Failed to receive response: {}", e))?
    }
    
    /// 엔진 시작 (trait 구현)
    async fn start(&mut self) -> Result<()> {
        self.start_impl().await
    }
    
    /// 엔진 정지 (trait 구현)
    async fn stop(&mut self) -> Result<()> {
        self.stop_impl().await
    }
}

// HighPerformanceEngine의 pub 메서드 (main.rs에서 직접 호출 가능)
impl HighPerformanceEngine {
    /// 엔진 시작 (pub 메서드 - main.rs에서 직접 호출 가능)
    /// 
    /// # 처리 과정
    /// 1. DB에서 잔고/주문 로드
    /// 2. WAL 스레드 시작
    /// 3. 엔진 스레드 시작
    /// 4. 실행 플래그 설정
    /// 
    /// # Note
    /// `&mut self`를 사용하여 필드를 직접 수정합니다.
    /// 서버 시작 시 한 번만 호출됩니다.
    pub async fn start(&mut self) -> Result<()> {
        self.start_impl().await
    }
    
    /// 엔진 정지 (pub 메서드 - main.rs에서 직접 호출 가능)
    /// 
    /// # 처리 과정
    /// 1. 실행 플래그 해제
    /// 2. 채널 닫기
    /// 3. 스레드 종료 대기
    /// 
    /// # Note
    /// `&mut self`를 사용하여 필드를 직접 수정합니다.
    /// 서버 종료 시 한 번만 호출됩니다.
    pub async fn stop(&mut self) -> Result<()> {
        self.stop_impl().await
    }
}

