//! 감사 조회는 별도 읽기 연결에서 커밋된 기록만 읽는다. 체결 큐·실행 상태·공유 락을 사용하지 않는다.
use super::{
    journal::{
        store::{decode, Genesis, Record},
        Outcome,
    },
    output::{self, Position},
};
use anyhow::{ensure, Result};
use axum::{
    extract::{Extension, Query},
    http::{HeaderMap, StatusCode},
    Json,
};
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct AuditReader {
    path: PathBuf,
    token: Option<String>,
    // 조회끼리의 동시 실행만 제한한다. 코어가 이 세마포어를 획득하는 경로는 없다.
    capacity: Arc<Semaphore>,
}
#[derive(Deserialize)]
pub struct PageQuery {
    #[serde(default)]
    pub after: u64,
    pub through: Option<u64>,
    pub engine_id: Option<String>,
    pub limit: Option<u16>,
}
#[derive(Serialize)]
pub struct AuditPage {
    pub engine_id: String,
    pub through: u64,
    pub next_after: u64,
    pub events: Vec<String>,
}
impl AuditReader {
    pub fn new(directory: &Path, token: Option<String>) -> Result<Self> {
        ensure!(
            token.as_ref().is_none_or(|value| value.len() >= 32),
            "감사 조회 키는 32바이트 이상이어야 합니다"
        );
        Ok(Self {
            path: directory.join("journal.sqlite3"),
            token,
            capacity: Arc::new(Semaphore::new(1)),
        })
    }
    fn read(&self, query: PageQuery) -> Result<AuditPage> {
        // 읽기 전용으로 열어 파일 생성·스키마 변경·저널 모드 변경을 모두 막는다.
        let connection = Connection::open_with_flags(&self.path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        connection.busy_timeout(std::time::Duration::from_millis(500))?;
        connection.execute_batch("PRAGMA query_only=ON; BEGIN;")?;
        let version: u32 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        ensure!(version == 4, "지원하지 않는 감사 저널 버전");
        let (payload, checksum) = connection.query_row(
            "SELECT payload,checksum FROM genesis WHERE id=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let genesis: Genesis = decode(payload, checksum)?;
        ensure!(
            genesis.version == 4 && uuid::Uuid::parse_str(&genesis.engine_id).is_ok(),
            "잘못된 최초 기록"
        );
        ensure!(
            query
                .engine_id
                .as_ref()
                .is_none_or(|id| id == &genesis.engine_id),
            "조회 중 엔진 식별자가 변경됨"
        );
        let last = u64::try_from(connection.query_row(
            "SELECT coalesce(max(sequence),0) FROM records",
            [],
            |row| row.get::<_, i64>(0),
        )?)?;
        let through = query.through.unwrap_or(last);
        ensure!(
            query.after <= through && through <= last,
            "확정 범위를 벗어난 조회"
        );
        let mut page = AuditPage {
            engine_id: genesis.engine_id,
            through,
            next_after: query.after,
            events: Vec::new(),
        };
        let mut bytes = 0;
        let mut rows=connection.prepare("SELECT sequence,payload,checksum FROM records WHERE sequence>?1 AND sequence<=?2 ORDER BY sequence LIMIT ?3")?;
        let mut cursor = rows.query(rusqlite::params![
            i64::try_from(query.after)?,
            i64::try_from(through)?,
            query.limit.unwrap_or(50)
        ])?;
        while let Some(row) = cursor.next()? {
            let sequence = u64::try_from(row.get::<_, i64>(0)?)?;
            ensure!(sequence == page.next_after + 1, "저널 순번 누락");
            let record: Record = decode(row.get(1)?, row.get(2)?)?;
            ensure!(record.sequence == sequence, "저널 순번 불일치");
            if let Outcome::Applied(Some(batch)) = &record.outcome {
                output::visit_events(
                    batch,
                    &Position {
                        engine_id: &page.engine_id,
                        sequence,
                        timestamp: record.timestamp,
                    },
                    |_, payload| {
                        bytes += payload.len();
                        if bytes > 4 * 1024 * 1024 {
                            return Err(output::DeliveryError {
                                next_event: 0,
                                reason: "감사 페이지 크기 초과".into(),
                            });
                        }
                        page.events.push(payload);
                        Ok(())
                    },
                )
                .map_err(|error| anyhow::anyhow!(error.reason))?;
            }
            // 한 요청이 무제한 응답을 만들지 않는다. 작은 페이지로 다시 요청할 수 있으며
            // 한 명령 자체가 한도를 넘으면 성공으로 잘라 보내지 않고 운영 조사를 요구한다.
            ensure!(bytes <= 4 * 1024 * 1024, "감사 페이지 크기 초과");
            page.next_after = sequence;
        }
        ensure!(
            page.next_after == through || page.next_after > query.after,
            "저널 꼬리 누락"
        );
        Ok(page)
    }
}

/// 조회 키는 고객 토큰과 별개다. 키가 없으면 비활성화하고 잘못된 키는 저장소 접근 전에 거부한다.
pub async fn page(
    Extension(reader): Extension<AuditReader>,
    headers: HeaderMap,
    Query(query): Query<PageQuery>,
) -> Result<Json<AuditPage>, StatusCode> {
    let token = reader
        .token
        .as_ref()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let provided = headers
        .get("x-engine-audit-token")
        .and_then(|value| value.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    if solana_sdk::hash::hash(provided.as_bytes()) != solana_sdk::hash::hash(token.as_bytes()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    if !(1..=100).contains(&query.limit.unwrap_or(50))
        || query.after > i64::MAX as u64
        || query.through.is_some_and(|n| n > i64::MAX as u64)
        || (query.after > 0 && (query.through.is_none() || query.engine_id.is_none()))
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    let permit = reader
        .capacity
        .clone()
        .try_acquire_owned()
        .map_err(|_| StatusCode::TOO_MANY_REQUESTS)?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        reader.read(query)
    })
    .await
    .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
    .map(Json)
    .map_err(|error| {
        eprintln!("audit_read_failed reason={error:#}");
        StatusCode::SERVICE_UNAVAILABLE
    })
}

#[cfg(test)]
mod tests {
    use super::super::{
        engine::EngineState,
        journal::{self, store::Store, Command, Pending, Reply},
    };
    use super::*;
    use crate::domains::cex::engine::types::OrderEntry;
    use rust_decimal::Decimal;
    use tower::ServiceExt;
    fn query(after: u64, through: Option<u64>, engine_id: Option<String>) -> PageQuery {
        PageQuery {
            after,
            through,
            engine_id,
            limit: Some(1),
        }
    }
    fn append(store: &mut Store, state: &mut EngineState, id: u64, sequence: &mut u64) {
        let order = OrderEntry {
            id,
            user_id: 1,
            order_type: "buy".into(),
            order_side: "limit".into(),
            base_mint: "BTC".into(),
            quote_mint: "USDT".into(),
            price: Some(Decimal::from(100)),
            amount: Decimal::ONE,
            quote_amount: None,
            remaining_amount: Decimal::ONE,
            remaining_quote_amount: None,
            filled_amount: Decimal::ZERO,
            fee_rate: Decimal::ZERO,
            created_at: chrono::Utc::now(),
        };
        let command = Command::Submit(order);
        let outcome = journal::apply(state, &command);
        store
            .append(
                &[Pending {
                    command: Some(command),
                    outcome,
                    reply: Reply::Submit(tokio::sync::oneshot::channel().0),
                }],
                sequence,
            )
            .unwrap();
    }
    #[test]
    fn audit_reads_commits_and_fixed_cut_without_core_or_platform_database() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let mut store = Store::open(directory.path())?;
        let mut state = EngineState::new();
        store.initialize(&state)?;
        let reader = AuditReader::new(directory.path(), Some("a".repeat(32)))?;
        let mut sequence = 0;
        append(&mut store, &mut state, 1, &mut sequence);
        let first = reader.read(query(0, None, None))?;
        assert_eq!(first.through, 1);
        assert_eq!(first.events.len(), 1);
        let value: serde_json::Value = serde_json::from_str(&first.events[0])?;
        assert_eq!(value["event_type"], "order_rejected");
        assert_eq!(value["event_id"], format!("{}:1:0", first.engine_id));
        // 미커밋 꼬리는 별도 연결에서 보이지 않아야 한다. 조회가 기록 워커를 기다리지도 않는다.
        store
            .connection
            .execute_batch("BEGIN IMMEDIATE; INSERT INTO records VALUES(2,'uncommitted',X'00')")?;
        assert_eq!(reader.read(query(0, None, None))?.through, 1);
        store.connection.execute_batch("ROLLBACK")?;
        append(&mut store, &mut state, 2, &mut sequence);
        assert_eq!(
            reader
                .read(query(0, Some(1), Some(first.engine_id.clone())))?
                .through,
            1
        );
        let second = reader.read(query(1, Some(2), Some(first.engine_id)))?;
        assert_eq!(second.next_after, 2);
        assert_eq!(second.events.len(), 1);
        Ok(())
    }
    #[test]
    fn audit_rejects_corruption_missing_file_and_wrong_engine_without_mutation() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let reader = AuditReader::new(directory.path(), None)?;
        assert!(reader.read(query(0, None, None)).is_err());
        assert!(!directory.path().join("journal.sqlite3").exists());
        let mut store = Store::open(directory.path())?;
        let mut state = EngineState::new();
        store.initialize(&state)?;
        append(&mut store, &mut state, 1, &mut 0);
        assert!(reader
            .read(query(0, None, Some(uuid::Uuid::new_v4().to_string())))
            .is_err());
        store
            .connection
            .execute_batch("DROP TRIGGER records_no_update; UPDATE records SET checksum=X'00'")?;
        assert!(reader.read(query(0, None, None)).is_err());
        assert_eq!(store.last_sequence()?, 1);
        Ok(())
    }
    #[tokio::test]
    async fn audit_http_authentication_bounds_and_capacity_fail_closed() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let reader = AuditReader::new(directory.path(), Some("a".repeat(32)))?;
        let router = axum::Router::new()
            .route("/internal/audit/results", axum::routing::get(page))
            .layer(Extension(reader.clone()));
        for (suffix, key, status) in [
            ("", None, 401),
            ("?limit=0", Some("a".repeat(32)), 400),
            ("?after=1", Some("a".repeat(32)), 400),
        ] {
            let mut request =
                axum::http::Request::builder().uri(format!("/internal/audit/results{suffix}"));
            if let Some(key) = key {
                request = request.header("x-engine-audit-token", key);
            }
            assert_eq!(
                router
                    .clone()
                    .oneshot(request.body(axum::body::Body::empty())?)
                    .await?
                    .status()
                    .as_u16(),
                status
            );
        }
        let _permit = reader.capacity.acquire().await?;
        let request = axum::http::Request::builder()
            .uri("/internal/audit/results")
            .header("x-engine-audit-token", "a".repeat(32))
            .body(axum::body::Body::empty())?;
        assert_eq!(
            router.oneshot(request).await?.status(),
            StatusCode::TOO_MANY_REQUESTS
        );
        Ok(())
    }
}
