//! 현재 서버 실행 경로에 연결되지 않은 로그 원시 구현이다.
//! 활성 상태 복구나 출력 내구성을 제공한다고 해석하지 않는다.
// =====================================================
// WAL (Write-Ahead Logging) - 순차 쓰기 로그
// =====================================================
// 역할: 모든 엔진 이벤트를 디스크에 순차 기록하여 복구 가능하게 함
// 
// 핵심 설계:
// 1. Append-only: 순차 쓰기로 초고속 (Sequential I/O)
// 2. fsync(): 커널 버퍼 → 디스크 동기화 (데이터 무손실)
// 3. Recovery: 서버 재시작 시 WAL 재생으로 상태 복구
//
// 성능:
// - 순차 쓰기: ~500MB/s (HDD도 빠름)
// - 랜덤 쓰기: ~1MB/s (100배 느림)
// - fsync(): ~0.5ms (디스크 동기화 대기)
// =====================================================

use std::fs::{File, OpenOptions};
use std::io::{Write, BufWriter, BufRead, BufReader};
use std::path::{Path, PathBuf};
use anyhow::{Result, Context};
use serde::{Serialize, Deserialize};
use crate::domains::cex::engine::types::MatchResult;

/// WAL 엔트리 (로그에 기록되는 이벤트)
/// 
/// 모든 엔진 이벤트를 캡처하여 복구 시 재생 가능
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalEntry {
    /// 주문 생성
    OrderCreated {
        order_id: u64,
        user_id: u64,
        order_type: String,  // "buy" or "sell"
        base_mint: String,
        quote_mint: String,
        price: Option<String>,  // Decimal을 String으로 (Serialization)
        amount: String,
        timestamp: i64,  // Unix timestamp (milliseconds)
    },
    
    /// 잔고 잠금
    BalanceLocked {
        user_id: u64,
        mint: String,
        amount: String,
        timestamp: i64,
    },
    
    /// 체결 발생
    TradeExecuted {
        buy_order_id: u64,
        sell_order_id: u64,
        buyer_id: u64,
        seller_id: u64,
        price: String,
        amount: String,
        base_mint: String,
        quote_mint: String,
        timestamp: i64,
    },
    
    /// 잔고 업데이트
    BalanceUpdated {
        user_id: u64,
        mint: String,
        available: String,
        locked: String,
        timestamp: i64,
    },
    
    /// 주문 취소
    OrderCancelled {
        order_id: u64,
        user_id: u64,
        timestamp: i64,
    },
}

/// WAL Writer
/// 
/// BufWriter 사용 이유:
/// - 작은 쓰기를 버퍼에 모아서 한 번에 디스크에 쓰기
/// - syscall 횟수 감소 (성능 향상)
/// - File::write()를 매번 호출하면 느림
pub struct WalWriter {
    /// 버퍼링된 파일 writer
    writer: BufWriter<File>,
    /// WAL 파일 경로
    file_path: PathBuf,
    /// 마지막 fsync 이후 기록된 엔트리 수
    entries_since_sync: usize,
    /// fsync 주기 (N개 엔트리마다)
    sync_interval: usize,
}

impl WalWriter {
    /// 새 WAL Writer 생성
    /// 
    /// # Arguments
    /// * `wal_dir` - WAL 파일이 저장될 디렉토리
    /// * `sync_interval` - N개 엔트리마다 fsync 호출 (기본: 1 = 매번)
    /// 
    /// # Returns
    /// WalWriter 인스턴스
    /// 
    /// # 파일명 형식
    /// wal/20240101_120530.log (날짜_시간.log)
    pub fn new(wal_dir: &Path, sync_interval: usize) -> Result<Self> {
        // WAL 디렉토리 생성
        std::fs::create_dir_all(wal_dir)
            .context("Failed to create WAL directory")?;
        
        // 파일명 생성 (타임스탬프 기반)
        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
        let file_path = wal_dir.join(format!("wal_{}.log", timestamp));
        
        // 파일 열기 (append mode)
        // OpenOptions::new()
        //   .create(true)  - 없으면 생성
        //   .append(true)  - Append-only (Sequential Write)
        //   .open()        - 파일 열기
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .context("Failed to open WAL file")?;
        
        // BufWriter로 감싸기 (8KB 버퍼 기본)
        let writer = BufWriter::new(file);
        
        Ok(Self {
            writer,
            file_path,
            entries_since_sync: 0,
            sync_interval,
        })
    }
    
    /// WAL에 엔트리 추가
    /// 
    /// # Arguments
    /// * `entry` - 기록할 WAL 엔트리
    /// 
    /// # Process
    /// 1. 엔트리를 JSON으로 직렬화
    /// 2. BufWriter에 쓰기 (메모리 버퍼)
    /// 3. 개행 문자 추가 (라인 단위로 구분)
    /// 4. sync_interval마다 fsync() 호출
    /// 
    /// # Performance
    /// - 버퍼 쓰기: ~100ns (메모리)
    /// - fsync(): ~0.5ms (디스크, sync_interval마다)
    pub fn append(&mut self, entry: &WalEntry) -> Result<()> {
        // JSON 직렬화
        // serde_json::to_string()은 구조체를 JSON 문자열로 변환
        let json = serde_json::to_string(entry)
            .context("Failed to serialize WAL entry")?;
        
        // BufWriter에 쓰기 (아직 디스크 X, 메모리 버퍼 O)
        // writeln! 매크로: write!() + '\n' (개행 추가)
        writeln!(self.writer, "{}", json)
            .context("Failed to write to WAL buffer")?;
        
        self.entries_since_sync += 1;
        
        // sync_interval마다 fsync 호출
        if self.entries_since_sync >= self.sync_interval {
            self.sync()?;
        }
        
        Ok(())
    }
    
    /// 강제 동기화 (fsync)
    /// 
    /// # Process
    /// 1. BufWriter::flush() - 메모리 버퍼 → 커널 버퍼
    /// 2. File::sync_all() - 커널 버퍼 → 디스크
    /// 
    /// # 커널 버퍼 (Page Cache)
    /// - OS가 관리하는 RAM 영역
    /// - File::write()는 커널 버퍼까지만 쓰고 리턴 (빠름)
    /// - fsync()는 커널 버퍼를 디스크로 강제 플러시 (느림)
    /// 
    /// # Trade-off
    /// - fsync 매번: 안전, 하지만 TPS 낮음 (~2,000)
    /// - fsync 10개마다: 빠름, 하지만 10개는 위험 (~20,000)
    /// - fsync 100ms마다: 매우 빠름, 하지만 100ms 데이터 손실 가능 (~100,000)
    pub fn sync(&mut self) -> Result<()> {
        // BufWriter 버퍼를 커널로 플러시
        self.writer.flush()
            .context("Failed to flush WAL buffer")?;
        
        // 커널 버퍼를 디스크로 동기화 (fsync syscall)
        // get_ref()는 BufWriter 내부의 File 참조를 가져옴
        self.writer.get_ref().sync_all()
            .context("Failed to sync WAL to disk")?;
        
        self.entries_since_sync = 0;
        Ok(())
    }
    
    /// WAL 파일 경로
    pub fn file_path(&self) -> &Path {
        &self.file_path
    }
}

/// WAL Reader (복구용)
/// 
/// 서버 재시작 시 WAL 파일을 읽어서 상태 복구
pub struct WalReader {
    file_path: PathBuf,
}

impl WalReader {
    /// 새 WAL Reader 생성
    pub fn new(file_path: PathBuf) -> Self {
        Self { file_path }
    }
    
    /// WAL 파일에서 모든 엔트리 읽기
    /// 
    /// # Returns
    /// Vec<WalEntry> - 시간 순서대로 정렬된 엔트리들
    /// 
    /// # Process
    /// 1. 파일 열기
    /// 2. 라인별로 읽기
    /// 3. JSON 파싱
    /// 4. WalEntry로 역직렬화
    pub fn read_all(&self) -> Result<Vec<WalEntry>> {
        let file = File::open(&self.file_path)
            .context("Failed to open WAL file for reading")?;
        
        let reader = BufReader::new(file);
        let mut entries = Vec::new();
        
        // 라인별로 읽기
        // lines()는 Iterator<Item = Result<String>>을 반환
        for (line_num, line) in reader.lines().enumerate() {
            let line = line.context(format!("Failed to read line {}", line_num + 1))?;
            
            // JSON 파싱
            let entry: WalEntry = serde_json::from_str(&line)
                .context(format!("Failed to parse WAL entry at line {}", line_num + 1))?;
            
            entries.push(entry);
        }
        
        Ok(entries)
    }
}

