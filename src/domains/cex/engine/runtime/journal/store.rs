//! 엔진 전용 저널이다. 저장 원자성·잘린 쓰기 복구는 SQLite에 맡기고,
//! 명령 순서·결과 일치·이벤트 식별은 애플리케이션에서 검증한다.
use super::{Command, Outcome, Pending};
use crate::domains::cex::engine::{amount, runtime::engine::EngineState};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    os::fd::AsRawFd,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

#[derive(Serialize, Deserialize)]
pub(in super::super) struct Genesis {
    pub(in super::super) version: u32,
    pub(in super::super) engine_id: String,
    balances: Vec<(u64, String, Decimal, Decimal)>,
}
#[derive(Serialize, Deserialize)]
pub(in super::super) struct Record {
    pub sequence: u64,
    pub timestamp: DateTime<Utc>,
    pub command: Command,
    pub outcome: Outcome,
}
/// 프로세스 중복 기동 차단은 초기화·제어 객체에서만 수행한다. 코어 상태의
/// 공유 락이 아니며, 파일을 지우지 않아 동일 경로의 락 파일 교체 경쟁을 막는다.
pub(in super::super) struct Ownership {
    pub _file: File,
    pub fresh: bool,
}
impl Ownership {
    pub fn acquire(directory: &Path) -> Result<Self> {
        std::fs::create_dir_all(directory)?;
        let fresh = !directory.join("owner.lock").exists();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join("owner.lock"))?;
        let status = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        ensure!(
            status == 0,
            "Engine journal is already owned: {}",
            directory.display()
        );
        Ok(Self { _file: file, fresh })
    }
}
pub(in super::super) struct Store {
    pub connection: Connection,
    pub path: PathBuf,
}
fn digest(payload: &str) -> Vec<u8> {
    // 이미 설치된 솔라나 SDK의 SHA-256을 사용한다. 기록 변조 방지 서명이 아니라
    // 저장 중 손상·잘못 바뀐 본문을 재생 전에 검출하기 위한 검사다.
    solana_sdk::hash::hash(payload.as_bytes())
        .to_bytes()
        .to_vec()
}
pub(in super::super) fn decode<T: serde::de::DeserializeOwned>(
    payload: String,
    checksum: Vec<u8>,
) -> Result<T> {
    ensure!(digest(&payload) == checksum, "Journal checksum mismatch");
    serde_json::from_str(&payload).context("Invalid journal record")
}
impl Store {
    pub fn open(directory: &Path) -> Result<Self> {
        let path = directory.join("journal.sqlite3");
        let connection = Connection::open(&path)?;
        let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        ensure!(
            version == 0 || version == 4,
            "Unsupported journal schema version"
        );
        if version == 0 {
            let tables: i64 = connection.query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table'",
                [],
                |r| r.get(0),
            )?;
            ensure!(tables == 0, "Unknown existing journal schema");
        }
        Self::configure(&connection)?;
        connection.execute_batch(
            "BEGIN IMMEDIATE;
             CREATE TABLE IF NOT EXISTS genesis (id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL, checksum BLOB NOT NULL);
             CREATE TABLE IF NOT EXISTS records (sequence INTEGER PRIMARY KEY CHECK(sequence>0), payload TEXT NOT NULL, checksum BLOB NOT NULL);
             CREATE TABLE IF NOT EXISTS delivery (id INTEGER PRIMARY KEY CHECK(id=1), sequence INTEGER NOT NULL CHECK(sequence>=0));
             INSERT OR IGNORE INTO delivery VALUES(1,0);
             CREATE TRIGGER IF NOT EXISTS records_no_update BEFORE UPDATE ON records BEGIN SELECT RAISE(ABORT,'immutable journal'); END;
             CREATE TRIGGER IF NOT EXISTS records_no_delete BEFORE DELETE ON records BEGIN SELECT RAISE(ABORT,'immutable journal'); END;
             CREATE TRIGGER IF NOT EXISTS genesis_no_update BEFORE UPDATE ON genesis BEGIN SELECT RAISE(ABORT,'immutable genesis'); END;
             PRAGMA user_version=4;
             CREATE TRIGGER IF NOT EXISTS genesis_no_delete BEFORE DELETE ON genesis BEGIN SELECT RAISE(ABORT,'immutable genesis'); END;
             COMMIT;"
        )?;
        // 최초 파일 이름까지 디렉터리에 반영한다. 복구 때 파일을 찾을 수 있는
        // 조건이며 다른 디스크로의 복제·백업은 이번 구현 범위에 포함하지 않는다.
        File::open(directory)?.sync_all()?;
        let check: String = connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        ensure!(check == "ok", "Journal integrity check failed: {check}");
        Ok(Self { connection, path })
    }
    fn configure(connection: &Connection) -> Result<()> {
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        let mode: String = connection.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
        ensure!(mode == "wal", "Journal requires SQLite WAL mode");
        connection.execute_batch(
            "PRAGMA synchronous=FULL; PRAGMA fullfsync=ON; PRAGMA checkpoint_fullfsync=ON;",
        )?;
        let level: u32 = connection.query_row("PRAGMA synchronous", [], |row| row.get(0))?;
        ensure!(level == 2, "Journal requires synchronous FULL");
        Ok(())
    }
    pub fn delivery_connection(&self) -> Result<Self> {
        let connection =
            Connection::open_with_flags(&self.path, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        Self::configure(&connection)?;
        Ok(Self {
            connection,
            path: self.path.clone(),
        })
    }
    pub fn initialized(&self) -> Result<bool> {
        let count: i64 = self
            .connection
            .query_row("SELECT count(*) FROM genesis", [], |r| r.get(0))?;
        ensure!(count <= 1, "Invalid genesis count");
        if count == 0 {
            ensure!(
                self.last_sequence()? == 0 && self.delivered()? == 0,
                "Journal has records without genesis"
            );
        }
        Ok(count == 1)
    }
    pub fn initialize(&mut self, state: &EngineState) -> Result<()> {
        ensure!(!self.initialized()?, "Journal already initialized");
        let genesis = Genesis {
            version: 4,
            engine_id: uuid::Uuid::new_v4().to_string(),
            balances: state.executor.balance_cache().journal_seed(),
        };
        let payload = serde_json::to_string(&genesis)?;
        self.connection.execute(
            "INSERT INTO genesis VALUES(1,?1,?2)",
            params![payload, digest(&payload)],
        )?;
        Ok(())
    }
    pub fn recover(&self) -> Result<(EngineState, String)> {
        let (payload, checksum) = self.connection.query_row(
            "SELECT payload, checksum FROM genesis WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let genesis: Genesis = decode(payload, checksum)?;
        ensure!(
            genesis.version == 4 && uuid::Uuid::parse_str(&genesis.engine_id).is_ok(),
            "Unsupported journal genesis"
        );
        let mut state = EngineState::new();
        for (user, mint, available, locked) in genesis.balances {
            ensure!(
                user > 0 && !mint.is_empty() && mint.trim() == mint && locked == Decimal::ZERO,
                "Invalid genesis account"
            );
            amount::units(available)?;
            ensure!(
                state
                    .executor
                    .balance_cache()
                    .get_balance(user, &mint)
                    .is_none(),
                "Duplicate genesis account"
            );
            state
                .executor
                .balance_cache_mut()
                .set_balance(user, &mint, available, locked);
        }
        let mut next = 1;
        // 복구 속도보다 검증을 우선한다. 전체 이력을 한 번에 메모리에 적재하지 않는다.
        let mut statement = self
            .connection
            .prepare("SELECT sequence,payload,checksum FROM records ORDER BY sequence")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let sequence = u64::try_from(row.get::<_, i64>(0)?)?;
            ensure!(sequence == next, "Journal sequence gap");
            let record: Record = decode(row.get(1)?, row.get(2)?)?;
            ensure!(record.sequence == sequence, "Journal sequence mismatch");
            let replayed = super::apply(&mut state, &record.command);
            ensure!(
                serde_json::to_value(&replayed)? == serde_json::to_value(&record.outcome)?,
                "Journal replay differs at sequence {sequence}"
            );
            next += 1;
        }
        ensure!(
            self.delivered()? < next,
            "Delivery cursor is ahead of journal"
        );
        Ok((state, genesis.engine_id))
    }
    pub fn last_sequence(&self) -> Result<u64> {
        Ok(u64::try_from(self.connection.query_row(
            "SELECT coalesce(max(sequence),0) FROM records",
            [],
            |r| r.get::<_, i64>(0),
        )?)?)
    }
    pub fn delivered(&self) -> Result<u64> {
        Ok(u64::try_from(self.connection.query_row(
            "SELECT sequence FROM delivery WHERE id=1",
            [],
            |r| r.get::<_, i64>(0),
        )?)?)
    }
    pub fn read(&self, sequence: u64) -> Result<Record> {
        let (payload, checksum): (String, Vec<u8>) = self.connection.query_row(
            "SELECT payload, checksum FROM records WHERE sequence=?1",
            [i64::try_from(sequence)?],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let record: Record = decode(payload, checksum)?;
        ensure!(record.sequence == sequence, "Journal sequence mismatch");
        Ok(record)
    }
    pub fn mark_delivered(&self, sequence: u64) -> Result<()> {
        let changed = self.connection.execute(
            "UPDATE delivery SET sequence=?1 WHERE id=1 AND sequence=?2",
            params![i64::try_from(sequence)?, i64::try_from(sequence - 1)?],
        )?;
        ensure!(changed == 1, "Non-contiguous delivery cursor");
        Ok(())
    }
    pub fn append(&mut self, pending: &[Pending], sequence: &mut u64) -> Result<()> {
        // 본문은 빌려서 직렬화한다. 저장 워커에서도 체결 목록을 복제하거나
        // 직렬화 결과를 다시 역직렬화하는 중간 복사 경로를 만들지 않는다.
        #[derive(Serialize)]
        struct RecordRef<'a> {
            sequence: u64,
            timestamp: DateTime<Utc>,
            command: &'a Command,
            outcome: &'a Outcome,
        }
        let transaction = self.connection.transaction()?;
        let mut next = *sequence;
        for item in pending {
            if let Some(command) = &item.command {
                next = next.checked_add(1).context("Journal sequence exhausted")?;
                let record = RecordRef {
                    sequence: next,
                    timestamp: Utc::now(),
                    command,
                    outcome: &item.outcome,
                };
                let payload = serde_json::to_string(&record)?;
                transaction.execute(
                    "INSERT INTO records VALUES(?1,?2,?3)",
                    params![i64::try_from(next)?, payload, digest(&payload)],
                )?;
            }
        }
        // FULL 모드의 커밋이 반환되기 전에는 성공 응답도 진행 위치도 공개하지 않는다.
        transaction.commit()?;
        *sequence = next;
        Ok(())
    }
}

pub(in super::super) struct Progress {
    pub durable: AtomicU64,
    pub delivered: AtomicU64,
    pub journal_done: AtomicBool,
}
impl Progress {
    pub fn new(durable: u64, delivered: u64) -> Self {
        Self {
            durable: AtomicU64::new(durable),
            delivered: AtomicU64::new(delivered),
            journal_done: AtomicBool::new(false),
        }
    }
}
struct FinishJournal {
    closed: Arc<AtomicBool>,
    progress: Arc<Progress>,
    successful: bool,
}
impl Drop for FinishJournal {
    fn drop(&mut self) {
        if !self.successful {
            self.closed.store(true, Ordering::Release);
        }
        self.progress.journal_done.store(true, Ordering::Release);
    }
}
pub(in super::super) fn run(
    mut store: Store,
    receiver: crossbeam::channel::Receiver<Pending>,
    closed: Arc<AtomicBool>,
    progress: Arc<Progress>,
) -> Result<()> {
    let mut finish = FinishJournal {
        closed,
        progress,
        successful: false,
    };
    let mut sequence = store.last_sequence()?;
    while let Ok(first) = receiver.recv() {
        // 이미 도착한 최대 256건을 묶되 배치를 채우려고 기다리지 않는다.
        let mut group = Vec::with_capacity(256);
        group.push(first);
        group.extend(receiver.try_iter().take(255));
        store.append(&group, &mut sequence).map_err(|error| {
            eprintln!("journal_commit_failed reason={error:#}");
            error
        })?;
        finish.progress.durable.store(sequence, Ordering::Release);
        for item in group {
            item.finish();
        }
    }
    finish.successful = true;
    Ok(())
}
