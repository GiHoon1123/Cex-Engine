//! 이 실행 소유 임시 저널에서 실제 코어·기록·출력 워커를 검증한다.
//! 저장 지연·실패·재시작을 주입하되 주문·정산 계산은 제품 구현을 사용한다.
use super::super::journal::store::{self, Ownership, Progress, Store};
use super::super::journal::*;
use super::super::{
    engine::{EngineState, HighPerformanceEngine},
    output, threads,
};
use crate::domains::cex::engine::types::{OrderEntry, TradingPair};
use crate::domains::cex::engine::Engine;
use anyhow::Result;
use crossbeam::channel::bounded;
use rust_decimal::Decimal;
use sha2::Digest;
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
};

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("cex-journal-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn order(id: u64, user: u64, buy: bool, size: u64) -> OrderEntry {
    OrderEntry {
        fee_rate: rust_decimal::Decimal::ZERO,
        id,
        user_id: user,
        order_type: if buy { "buy" } else { "sell" }.into(),
        order_side: "limit".into(),
        base_mint: "SOL".into(),
        quote_mint: "USDT".into(),
        price: Some(Decimal::from(100)),
        amount: Decimal::from(size),
        quote_amount: None,
        filled_amount: Decimal::ZERO,
        remaining_amount: Decimal::from(size),
        remaining_quote_amount: None,
        created_at: chrono::Utc::now(),
    }
}
fn seeded() -> EngineState {
    let mut state = EngineState::new();
    state
        .executor
        .balance_cache_mut()
        .set_balance(1, "USDT", Decimal::from(1000), Decimal::ZERO);
    state
        .executor
        .balance_cache_mut()
        .set_balance(2, "SOL", Decimal::from(10), Decimal::ZERO);
    state
}
fn pending(state: &mut EngineState, command: Command) -> Pending {
    let outcome = apply(state, &command);
    Pending {
        command: Some(command),
        outcome,
        reply: Reply::Submit(tokio::sync::oneshot::channel().0),
    }
}
fn initialize(path: &std::path::Path) -> Store {
    let mut store = Store::open(path).unwrap();
    store.initialize(&seeded()).unwrap();
    store
}
fn attach(
    engine: &mut HighPerformanceEngine,
    store: Store,
    sink: impl FnMut(
            &output::OutputBatch,
            &output::Position<'_>,
        ) -> std::result::Result<(), output::DeliveryError>
        + Send
        + 'static,
) {
    let (state, id) = store.recover().unwrap();
    let progress = Arc::new(Progress::new(
        store.last_sequence().unwrap(),
        store.delivered().unwrap(),
    ));
    let (ready, _) = tokio::sync::oneshot::channel();
    let delivery = store.delivery_connection().unwrap();
    let p = progress.clone();
    let closed = engine.handle.closed.clone();
    engine.output_thread = Some(thread::spawn(move || {
        output::run_store(delivery, id, p, closed, ready, sink, |_| Ok(()))
    }));
    let (sender, receiver) = bounded(100);
    engine.journal_rx = Some(receiver.clone());
    let p = progress.clone();
    let closed = engine.handle.closed.clone();
    engine.journal_thread = Some(thread::spawn(move || {
        store::run(store, receiver, closed, p)
    }));
    engine.progress = Some(progress);
    engine.state = None;
    engine.running.store(true, Ordering::Release);
    let orders = engine.order_rx.clone();
    let balances = engine.balance_rx.clone();
    let running = engine.running.clone();
    let closed = engine.handle.closed.clone();
    engine.engine_thread = Some(thread::spawn(move || {
        threads::engine_thread_loop(orders, balances, Some(sender), state, running, closed)
    }));
}

#[tokio::test]
async fn journal_replays_partial_fill_cancel_balance_and_duplicate_commands() -> Result<()> {
    let directory = Directory::new();
    let store = initialize(&directory.0);
    let mut engine = HighPerformanceEngine::new_bench();
    attach(&mut engine, store, |_, _| Ok(()));
    let handle = engine.handle();
    let sell = order(1, 2, false, 3);
    let buy = order(2, 1, true, 1);
    handle.submit_order(sell.clone()).await?;
    handle.submit_order(buy.clone()).await?;
    handle.submit_order(buy.clone()).await?;
    handle.update_balance(1, "USDT", Decimal::from(5)).await?;
    handle.lock_balance(1, "USDT", Decimal::from(2)).await?;
    handle.unlock_balance(1, "USDT", Decimal::from(2)).await?;
    let pair = TradingPair::new("SOL".into(), "USDT".into());
    assert_eq!(
        handle.get_orderbook(&pair, None).await?.1[0].remaining_amount,
        Decimal::from(2)
    );
    handle.cancel_order(1, 2, &pair).await?;
    handle.cancel_order(1, 2, &pair).await?;
    let before = handle.get_balance(1, "USDT").await?;
    engine.stop().await?;
    let store = Store::open(&directory.0)?;
    let (recovered, _) = store.recover()?;
    assert_eq!(
        recovered
            .executor
            .balance_cache()
            .get_balance(1, "USDT")
            .unwrap()
            .available,
        before.0
    );
    assert_eq!(before, (Decimal::from(905), Decimal::ZERO));
    assert_eq!(recovered.orderbooks[&pair].sell_orders.total_orders(), 0);
    assert_eq!(
        recovered
            .executor
            .balance_cache()
            .get_balance(2, "SOL")
            .unwrap()
            .available,
        Decimal::from(9)
    );
    let mut restarted = HighPerformanceEngine::new_bench();
    attach(&mut restarted, store, |_, _| Ok(()));
    restarted.handle().submit_order(buy).await?;
    assert_eq!(restarted.handle().get_balance(1, "USDT").await?, before);
    restarted.stop().await?;
    Ok(())
}

#[tokio::test]
async fn journal_response_waits_for_commit_while_core_keeps_processing() -> Result<()> {
    let mut engine = HighPerformanceEngine::new_bench();
    let (sender, receiver) = bounded(4);
    engine.journal_rx = Some(receiver.clone());
    let state = seeded();
    let orders = engine.order_rx.clone();
    let balances = engine.balance_rx.clone();
    let running = engine.running.clone();
    let closed = engine.handle.closed.clone();
    running.store(true, Ordering::Release);
    engine.engine_thread = Some(thread::spawn(move || {
        threads::engine_thread_loop(orders, balances, Some(sender), state, running, closed)
    }));
    let first = engine.handle();
    let second = engine.handle();
    let a = tokio::spawn(async move { first.submit_order(order(1, 1, true, 1)).await });
    let b = tokio::spawn(async move { second.submit_order(order(2, 1, true, 1)).await });
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while receiver.len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(
        !a.is_finished() && !b.is_finished(),
        "uncommitted response escaped"
    );
    let directory = Directory::new();
    let mut store = initialize(&directory.0);
    let mut sequence = 0;
    let group: Vec<_> = receiver.try_iter().collect();
    store.append(&group, &mut sequence)?;
    for item in group {
        item.finish();
    }
    a.await??;
    b.await??;
    engine.stop().await?;
    assert_eq!(
        store
            .recover()?
            .0
            .orderbooks
            .values()
            .next()
            .unwrap()
            .buy_orders
            .total_orders(),
        2
    );
    Ok(())
}

#[tokio::test]
async fn journal_commit_failure_closes_input_and_never_replies_success() -> Result<()> {
    let directory = Directory::new();
    let store = initialize(&directory.0);
    store.connection.execute_batch("PRAGMA query_only=ON")?;
    let mut engine = HighPerformanceEngine::new_bench();
    attach(&mut engine, store, |_, _| Ok(()));
    assert!(engine
        .handle()
        .submit_order(order(1, 1, true, 1))
        .await
        .is_err());
    assert!(!engine.handle().is_accepting());
    assert!(engine
        .stop()
        .await
        .unwrap_err()
        .to_string()
        .contains("Journal worker failed"));
    assert_eq!(Store::open(&directory.0)?.last_sequence()?, 0);
    Ok(())
}

#[tokio::test]
async fn journal_kafka_failure_keeps_durable_result_for_same_event_republication() -> Result<()> {
    let directory = Directory::new();
    let store = initialize(&directory.0);
    let mut engine = HighPerformanceEngine::new_bench();
    attach(&mut engine, store, |batch, _| {
        if matches!(batch, output::OutputBatch::Submitted { trades, .. } if !trades.is_empty()) {
            Err(output::DeliveryError {
                next_event: 0,
                reason: "confirmation lost".into(),
            })
        } else {
            Ok(())
        }
    });
    engine.handle().submit_order(order(1, 2, false, 3)).await?;
    engine.handle().submit_order(order(2, 1, true, 1)).await?;
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while engine.handle().is_accepting() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(engine.stop().await.is_err());
    let store = Store::open(&directory.0)?;
    assert_eq!(store.delivered()?, 1);
    assert!(store.last_sequence()? >= 1);
    store.recover()?;
    Ok(())
}

#[test]
fn journal_corruption_gap_result_mismatch_and_cursor_ahead_block_recovery() -> Result<()> {
    for fault in ["checksum", "gap", "outcome", "cursor"] {
        let directory = Directory::new();
        let mut store = initialize(&directory.0);
        let mut state = seeded();
        let item = pending(&mut state, Command::Submit(order(1, 1, true, 1)));
        store.append(&[item], &mut 0)?;
        store
            .connection
            .execute_batch("DROP TRIGGER records_no_update; DROP TRIGGER records_no_delete;")?;
        match fault {
            "checksum" => {
                store
                    .connection
                    .execute("UPDATE records SET payload='{}'", [])?;
            }
            "gap" => {
                store
                    .connection
                    .execute("UPDATE records SET sequence=2", [])?;
            }
            "outcome" => {
                let mut record = store.read(1)?;
                record.outcome = Outcome::Applied(None);
                let payload = serde_json::to_string(&record)?;
                let checksum = sha2::Sha256::digest(payload.as_bytes()).to_vec();
                store.connection.execute(
                    "UPDATE records SET payload=?1,checksum=?2",
                    rusqlite::params![payload, checksum],
                )?;
            }
            "cursor" => {
                store
                    .connection
                    .execute("UPDATE delivery SET sequence=2", [])?;
            }
            _ => unreachable!(),
        }
        assert!(store.recover().is_err(), "accepted fault {fault}");
    }
    Ok(())
}

#[test]
fn journal_process_ownership_rejects_second_owner_and_releases_on_drop() -> Result<()> {
    let directory = Directory::new();
    let first = Ownership::acquire(&directory.0)?;
    assert!(Ownership::acquire(&directory.0).is_err());
    drop(first);
    Ownership::acquire(&directory.0)?;
    Ok(())
}

#[test]
fn journal_failed_balance_delta_preserves_original_and_replays_rejection() -> Result<()> {
    let directory = Directory::new();
    let mut store = initialize(&directory.0);
    let mut state = seeded();
    let item = pending(
        &mut state,
        Command::Balance {
            user_id: 1,
            mint: "USDT".into(),
            delta: Decimal::from(-1001),
        },
    );
    assert!(matches!(item.outcome, Outcome::Failed(_)));
    store.append(&[item], &mut 0)?;
    assert_eq!(
        store
            .recover()?
            .0
            .executor
            .balance_cache()
            .get_balance(1, "USDT")
            .unwrap()
            .available,
        Decimal::from(1000)
    );
    Ok(())
}

#[tokio::test]
async fn journal_full_queue_preserves_input_and_does_not_confirm_uncommitted_order() -> Result<()> {
    let mut engine = HighPerformanceEngine::new_bench();
    let (sender, receiver) = bounded(1);
    sender
        .try_send(Pending {
            command: None,
            outcome: Outcome::Applied(None),
            reply: Reply::Submit(tokio::sync::oneshot::channel().0),
        })
        .unwrap_or_else(|_| panic!("queue seed failed"));
    engine.journal_rx = Some(receiver.clone());
    let orders = engine.order_rx.clone();
    let balances = engine.balance_rx.clone();
    let running = engine.running.clone();
    let closed = engine.handle.closed.clone();
    running.store(true, Ordering::Release);
    engine.state = None;
    engine.engine_thread = Some(thread::spawn(move || {
        threads::engine_thread_loop(orders, balances, Some(sender), seeded(), running, closed)
    }));
    let handle = engine.handle();
    let submitted = tokio::spawn(async move { handle.submit_order(order(1, 1, true, 1)).await });
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while engine.order_rx.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert_eq!(engine.order_rx.len(), 1);
    assert!(!submitted.is_finished());
    assert!(engine.stop().await.is_err());
    assert!(engine.state.as_ref().unwrap().orderbooks.is_empty());
    submitted.abort();
    Ok(())
}

#[test]
fn journal_kafka_cannot_overtake_durable_watermark() -> Result<()> {
    let directory = Directory::new();
    let mut store = initialize(&directory.0);
    let mut state = seeded();
    let item = pending(&mut state, Command::Submit(order(1, 1, true, 1)));
    store.append(&[item], &mut 0)?;
    let (_, id) = store.recover()?;
    let progress = Arc::new(Progress::new(0, 0));
    let captured = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (ready, _) = tokio::sync::oneshot::channel();
    let delivery = store.delivery_connection()?;
    let p = progress.clone();
    let seen = captured.clone();
    let market_seen = captured.clone();
    let worker = thread::spawn(move || {
        output::run_store(
            delivery,
            id,
            p,
            Arc::new(AtomicBool::new(false)),
            ready,
            move |_, _| {
                seen.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
            move |_| {
                market_seen.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
    });
    thread::sleep(std::time::Duration::from_millis(25));
    assert_eq!(captured.load(Ordering::SeqCst), 0);
    progress.durable.store(1, Ordering::Release);
    progress.journal_done.store(true, Ordering::Release);
    worker.join().unwrap()?;
    assert_eq!(captured.load(Ordering::SeqCst), 2);
    assert_eq!(store.delivered()?, 1);
    Ok(())
}

#[test]
fn journal_lost_delivery_cursor_republishes_same_identity_timestamp_and_payload() -> Result<()> {
    let directory = Directory::new();
    let mut store = initialize(&directory.0);
    let mut state = seeded();
    let sell = pending(&mut state, Command::Submit(order(1, 2, false, 3)));
    let buy = pending(&mut state, Command::Submit(order(2, 1, true, 1)));
    store.append(&[sell, buy], &mut 0)?;
    let (_, id) = store.recover()?;
    let mut attempts = Vec::new();
    for read_only in [true, false] {
        let delivery = store.delivery_connection()?;
        if read_only {
            delivery.connection.execute_batch("PRAGMA query_only=ON")?;
        }
        let progress = Arc::new(Progress::new(2, store.delivered()?));
        progress.journal_done.store(true, Ordering::Release);
        let (ready, _) = tokio::sync::oneshot::channel();
        let result = output::run_store(
            delivery,
            id.clone(),
            progress,
            Arc::new(AtomicBool::new(false)),
            ready,
            |batch, position| {
                attempts.push((
                    format!("{}:{}", position.engine_id, position.sequence),
                    position.timestamp,
                    serde_json::to_string(batch).unwrap(),
                ));
                Ok(())
            },
            |_| Ok(()),
        );
        if read_only {
            assert!(result.is_err());
            assert_eq!(store.delivered()?, 0);
        } else {
            result?;
        }
    }
    assert_eq!(attempts[0], attempts[1]);
    assert_eq!(store.delivered()?, 2);
    Ok(())
}

#[test]
fn journal_multiple_makers_restore_fifo_and_remaining_order_exactly() -> Result<()> {
    let directory = Directory::new();
    let mut store = initialize(&directory.0);
    let mut state = seeded();
    let mut sequence = 0;
    for command in [
        Command::Balance {
            user_id: 3,
            mint: "SOL".into(),
            delta: Decimal::from(10),
        },
        Command::Submit(order(1, 2, false, 2)),
        Command::Submit(order(2, 3, false, 3)),
        Command::Submit(order(3, 1, true, 4)),
    ] {
        let item = pending(&mut state, command);
        store.append(&[item], &mut sequence)?;
    }
    let record = store.read(4)?;
    assert!(
        matches!(record.outcome,Outcome::Applied(Some(output::OutputBatch::Submitted { ref trades, .. })) if trades.len()==2)
    );
    let (state, _) = store.recover()?;
    let pair = TradingPair::new("SOL".into(), "USDT".into());
    let remaining = &state.orderbooks[&pair].sell_orders.orders[&Decimal::from(100)][0];
    assert_eq!(
        (remaining.id, remaining.remaining_amount),
        (2, Decimal::ONE)
    );
    assert_eq!(
        state
            .executor
            .balance_cache()
            .get_balance(1, "USDT")
            .unwrap()
            .available,
        Decimal::from(600)
    );
    Ok(())
}

#[test]
fn journal_conflicting_order_id_is_recorded_without_second_execution() -> Result<()> {
    let directory = Directory::new();
    let mut store = initialize(&directory.0);
    let mut state = seeded();
    let mut sequence = 0;
    for command in [
        Command::Submit(order(1, 1, true, 1)),
        Command::Submit(order(1, 1, true, 2)),
    ] {
        let item = pending(&mut state, command);
        store.append(&[item], &mut sequence)?;
    }
    assert!(matches!(store.read(2)?.outcome, Outcome::Failed(_)));
    assert_eq!(
        store
            .recover()?
            .0
            .executor
            .balance_cache()
            .get_balance(1, "USDT")
            .unwrap()
            .available,
        Decimal::from(900)
    );
    Ok(())
}

#[tokio::test]
async fn journal_standard_restart_needs_no_platform_database_and_missing_log_is_blocked(
) -> Result<()> {
    let directory = Directory::new();
    let mut store = initialize(&directory.0);
    let mut state = seeded();
    // 이 테스트는 외부 출력이 없는 예약 명령으로 디비 비의존 복구를 검증한다.
    // 미체결 주문도 공개 호가를 발생시키므로 실제 주문 복구와 출력 연결은
    // 별도 격리 카프카 프로세스 검증에서 함께 확인한다.
    let item = pending(
        &mut state,
        Command::Lock {
            user_id: 1,
            mint: "USDT".into(),
            amount: Decimal::from(100),
        },
    );
    store.append(&[item], &mut 0)?;
    drop(store);
    let db =
        crate::shared::database::Database::lazy("postgres://unused:unused@127.0.0.1:1/unused")?;
    let mut engine = HighPerformanceEngine::new(db.clone()).with_data_dir(&directory.0);
    engine.start().await?;
    assert_eq!(
        engine.handle().get_balance(1, "USDT").await?,
        (Decimal::from(900), Decimal::from(100))
    );
    engine.stop().await?;
    std::fs::remove_file(directory.0.join("journal.sqlite3"))?;
    let mut next = HighPerformanceEngine::new(db).with_data_dir(&directory.0);
    assert!(next
        .start()
        .await
        .unwrap_err()
        .to_string()
        .contains("journal is missing"));
    Ok(())
}

#[test]
fn journal_market_budget_and_ioc_remainder_replay_exactly() -> Result<()> {
    for buy in [true, false] {
        let directory = Directory::new();
        let mut store = initialize(&directory.0);
        let mut state = seeded();
        let maker = if buy {
            order(1, 2, false, 3)
        } else {
            order(1, 1, true, 2)
        };
        let mut market = if buy {
            order(2, 1, true, 0)
        } else {
            order(2, 2, false, 3)
        };
        market.order_side = "market".into();
        market.price = None;
        if buy {
            market.quote_amount = Some(Decimal::from(150));
            market.remaining_quote_amount = market.quote_amount;
        }
        let first = pending(&mut state, Command::Submit(maker));
        let second = pending(&mut state, Command::Submit(market));
        store.append(&[first, second], &mut 0)?;
        let (restored, _) = store.recover()?;
        let balance = restored.executor.balance_cache();
        if buy {
            assert_eq!(
                balance.get_balance(1, "USDT").unwrap().available,
                Decimal::from(850)
            );
            assert_eq!(
                balance.get_balance(1, "SOL").unwrap().available,
                Decimal::new(15, 1)
            );
        } else {
            assert_eq!(
                balance.get_balance(2, "SOL").unwrap().available,
                Decimal::from(8)
            );
            assert_eq!(balance.get_balance(2, "SOL").unwrap().locked, Decimal::ZERO);
        }
    }
    Ok(())
}

#[test]
fn journal_process_exit_during_uncommitted_write_preserves_last_commit() -> Result<()> {
    let directory = Directory::new();
    let mut store = initialize(&directory.0);
    let mut state = seeded();
    let item = pending(&mut state, Command::Submit(order(1, 1, true, 1)));
    store.append(&[item], &mut 0)?;
    let path = store.path.clone();
    drop(store);
    // 별도 프로세스가 열린 쓰기 트랜잭션을 남기고 즉시 종료한다. 엔진과 같은
    // SQLite 파일에서 미커밋 꼬리를 버리고 앞선 확정 기록을 보존해야 한다.
    let status=std::process::Command::new("python3").arg("-c").arg(
        "import sqlite3,sys,os; c=sqlite3.connect(sys.argv[1]); c.execute('BEGIN IMMEDIATE'); c.execute(\"INSERT INTO records VALUES(2,'broken',X'00')\"); os._exit(9)"
    ).arg(path).status()?;
    assert_eq!(status.code(), Some(9));
    let store = Store::open(&directory.0)?;
    assert_eq!(store.last_sequence()?, 1);
    assert_eq!(
        store
            .recover()?
            .0
            .executor
            .balance_cache()
            .get_balance(1, "USDT")
            .unwrap()
            .locked,
        Decimal::from(100)
    );
    Ok(())
}

#[test]
fn journal_unsupported_schema_blocks_without_rewriting_existing_file() -> Result<()> {
    let directory = Directory::new();
    let store = initialize(&directory.0);
    store.connection.execute_batch("PRAGMA user_version=99")?;
    drop(store);
    assert!(Store::open(&directory.0).is_err());
    let connection = rusqlite::Connection::open(directory.0.join("journal.sqlite3"))?;
    assert_eq!(
        connection.query_row("PRAGMA user_version", [], |row| row.get::<_, u32>(0))?,
        99
    );
    Ok(())
}

#[test]
fn journal_market_failure_keeps_cursor_and_replays_identical_snapshot() -> Result<()> {
    let directory = Directory::new();
    let mut store = initialize(&directory.0);
    let mut state = seeded();
    let item = pending(&mut state, Command::Submit(order(1, 2, false, 3)));
    store.append(&[item], &mut 0)?;
    let (_, id) = store.recover()?;
    let mut snapshots = Vec::new();
    for fail in [true, false] {
        let progress = Arc::new(Progress::new(1, store.delivered()?));
        progress.journal_done.store(true, Ordering::Release);
        let (ready, _) = tokio::sync::oneshot::channel();
        let result = output::run_store(
            store.delivery_connection()?,
            id.clone(),
            progress,
            Arc::new(AtomicBool::new(false)),
            ready,
            |_, _| Ok(()),
            |event| {
                snapshots.push(serde_json::to_string(event)?);
                if fail {
                    anyhow::bail!("의도한 시세 전달 실패");
                }
                Ok(())
            },
        );
        if fail {
            assert!(result.is_err());
            assert_eq!(store.delivered()?, 0);
        } else {
            result?;
        }
    }
    assert_eq!(snapshots.len(), 2);
    assert_eq!(snapshots[0], snapshots[1]);
    assert_eq!(store.delivered()?, 1);
    Ok(())
}

#[tokio::test]
async fn journal_fee_results_replay_and_duplicate_without_second_charge() -> Result<()> {
    let directory = Directory::new();
    let store = initialize(&directory.0);
    let mut engine = HighPerformanceEngine::new_bench();
    attach(&mut engine, store, |_, _| Ok(()));
    let mut sell = order(1, 2, false, 2);
    sell.fee_rate = "0.002".parse()?;
    let mut buy = order(2, 1, true, 1);
    buy.fee_rate = "0.001".parse()?;
    engine.handle().submit_order(sell).await?;
    engine.handle().submit_order(buy.clone()).await?;
    let expected = engine.handle().get_balance(1, "SOL").await?;
    assert_eq!(expected.0, "0.999".parse()?);
    engine.stop().await?;
    let store = Store::open(&directory.0)?;
    let mut restarted = HighPerformanceEngine::new_bench();
    attach(&mut restarted, store, |_, _| Ok(()));
    restarted.handle().submit_order(buy).await?;
    assert_eq!(restarted.handle().get_balance(1, "SOL").await?, expected);
    assert_eq!(
        restarted.handle().get_balance(2, "USDT").await?.0,
        "99.8".parse()?
    );
    restarted.stop().await?;
    Ok(())
}

#[test]
fn journal_old_fee_contract_is_rejected_without_rewriting_history() -> Result<()> {
    let directory = Directory::new();
    let store = initialize(&directory.0);
    store.connection.execute_batch("PRAGMA user_version=1")?;
    drop(store);
    assert!(Store::open(&directory.0).is_err());
    let connection = rusqlite::Connection::open(directory.0.join("journal.sqlite3"))?;
    assert_eq!(
        connection.query_row("PRAGMA user_version", [], |row| row.get::<_, u32>(0))?,
        1
    );
    Ok(())
}

#[test]
fn journal_fee_amount_mismatch_blocks_recovery_even_with_valid_checksum() -> Result<()> {
    let directory = Directory::new();
    let mut store = initialize(&directory.0);
    let mut state = seeded();
    let mut sequence = 0;
    let mut buy = order(2, 1, true, 1);
    buy.fee_rate = "0.001".parse()?;
    for command in [Command::Submit(order(1, 2, false, 1)), Command::Submit(buy)] {
        let item = pending(&mut state, command);
        store.append(&[item], &mut sequence)?;
    }
    let mut record = serde_json::to_value(store.read(2)?)?;
    let fee = &mut record["outcome"]["Applied"]["Submitted"]["trades"][0]["buyer_fee"];
    assert_eq!(fee.as_str(), Some("0.001"));
    *fee = serde_json::json!("0.002");
    let payload = serde_json::to_string(&record)?;
    let checksum = sha2::Sha256::digest(payload.as_bytes()).to_vec();
    // 검사 합까지 정상인 잘못된 결과를 주입해, 저장 손상 검사와 계산 결과 검사가 독립인지 확인한다.
    store
        .connection
        .execute_batch("DROP TRIGGER records_no_update")?;
    store.connection.execute(
        "UPDATE records SET payload=?1,checksum=?2 WHERE sequence=2",
        rusqlite::params![payload, checksum],
    )?;
    assert!(store.recover().is_err());
    Ok(())
}

#[tokio::test]
async fn journal_ioc_zero_fill_and_partial_completion_replay_without_second_refund() -> Result<()> {
    let directory = Directory::new();
    let store = initialize(&directory.0);
    let mut engine = HighPerformanceEngine::new_bench();
    attach(&mut engine, store, |_, _| Ok(()));
    let mut empty = order(1, 1, true, 1);
    empty.order_side = "market".into();
    empty.price = None;
    empty.amount = Decimal::ZERO;
    empty.remaining_amount = Decimal::ZERO;
    empty.quote_amount = Some(Decimal::from(200));
    empty.remaining_quote_amount = empty.quote_amount;
    engine.handle().submit_order(empty.clone()).await?;
    engine.handle().submit_order(order(2, 2, false, 1)).await?;
    let mut partial = empty.clone();
    partial.id = 3;
    engine.handle().submit_order(partial.clone()).await?;
    engine.stop().await?;
    let store = Store::open(&directory.0)?;
    let first = serde_json::to_value(store.read(1)?)?;
    assert_eq!(
        first["outcome"]["Applied"]["Submitted"]["completion"]["refund_amount"],
        "200"
    );
    let record = serde_json::to_value(store.read(3)?)?;
    assert_eq!(
        record["outcome"]["Applied"]["Submitted"]["completion"]["status"],
        "expired"
    );
    let mut restarted = HighPerformanceEngine::new_bench();
    attach(&mut restarted, store, |_, _| Ok(()));
    restarted.handle().submit_order(empty).await?;
    restarted.handle().submit_order(partial).await?;
    assert_eq!(
        restarted.handle().get_balance(1, "USDT").await?,
        (Decimal::from(900), Decimal::ZERO)
    );
    restarted.stop().await?;
    Ok(())
}

#[test]
fn journal_ioc_completion_failure_keeps_cursor_and_identical_retry() -> Result<()> {
    let directory = Directory::new();
    let mut store = initialize(&directory.0);
    let mut state = seeded();
    let mut market = order(1, 1, true, 1);
    market.order_side = "market".into();
    market.price = None;
    market.amount = Decimal::ZERO;
    market.remaining_amount = Decimal::ZERO;
    market.quote_amount = Some(Decimal::from(200));
    market.remaining_quote_amount = market.quote_amount;
    let item = pending(&mut state, Command::Submit(market));
    store.append(&[item], &mut 0)?;
    let (_, id) = store.recover()?;
    let mut attempts = Vec::new();
    for fail in [true, false] {
        let progress = Arc::new(Progress::new(1, store.delivered()?));
        progress.journal_done.store(true, Ordering::Release);
        let (ready, _) = tokio::sync::oneshot::channel();
        let result = output::run_store(
            store.delivery_connection()?,
            id.clone(),
            progress,
            Arc::new(AtomicBool::new(false)),
            ready,
            |batch, position| {
                attempts.push((position.sequence, serde_json::to_string(batch).unwrap()));
                if fail {
                    Err(output::DeliveryError {
                        next_event: 0,
                        reason: "시장가 종료 전달 실패 주입".into(),
                    })
                } else {
                    Ok(())
                }
            },
            |_| Ok(()),
        );
        assert_eq!(result.is_err(), fail);
        assert_eq!(store.delivered()?, if fail { 0 } else { 1 });
    }
    assert_eq!(attempts[0], attempts[1]);
    Ok(())
}

#[test]
fn journal_ioc_contract_rejects_previous_fee_version_without_mutation() -> Result<()> {
    let directory = Directory::new();
    let store = initialize(&directory.0);
    store.connection.execute_batch("PRAGMA user_version=2")?;
    drop(store);
    assert!(Store::open(&directory.0).is_err());
    let connection = rusqlite::Connection::open(directory.0.join("journal.sqlite3"))?;
    assert_eq!(
        connection.query_row("PRAGMA user_version", [], |row| row.get::<_, u32>(0))?,
        2
    );
    Ok(())
}

#[tokio::test]
async fn journal_rejection_preserves_original_and_restart_duplicate_has_no_second_effect(
) -> Result<()> {
    let directory = Directory::new();
    let store = initialize(&directory.0);
    let mut engine = HighPerformanceEngine::new_bench();
    attach(&mut engine, store, |_, _| Ok(()));
    // 최초 잔고보다 큰 예약은 전체 명령 거절이다. 성공 응답은 거래 성공이 아닌 저널 확정을 뜻한다.
    let rejected = order(1, 1, true, 11);
    engine.handle().submit_order(rejected.clone()).await?;
    assert_eq!(
        engine.handle().get_balance(1, "USDT").await?,
        (Decimal::from(1000), Decimal::ZERO)
    );
    engine.stop().await?;
    let store = Store::open(&directory.0)?;
    let record = store.read(1)?;
    match record.outcome {
        Outcome::Applied(Some(output::OutputBatch::Rejected { order, .. })) => {
            assert_eq!(
                serde_json::to_value(order)?,
                serde_json::to_value(&rejected)?
            );
        }
        _ => panic!("거절 원본이 저널에 없습니다"),
    }
    let mut restarted = HighPerformanceEngine::new_bench();
    attach(&mut restarted, store, |_, _| Ok(()));
    restarted.handle().submit_order(rejected).await?;
    assert_eq!(
        restarted.handle().get_balance(1, "USDT").await?,
        (Decimal::from(1000), Decimal::ZERO)
    );
    restarted.stop().await?;
    let store = Store::open(&directory.0)?;
    assert!(matches!(store.read(2)?.outcome, Outcome::Applied(None)));
    Ok(())
}

#[test]
fn journal_rejection_output_failure_keeps_cursor_and_identical_original() -> Result<()> {
    let directory = Directory::new();
    let mut store = initialize(&directory.0);
    let item = pending(&mut seeded(), Command::Submit(order(1, 1, true, 11)));
    store.append(&[item], &mut 0)?;
    let (_, id) = store.recover()?;
    let mut attempts = Vec::new();
    for fail in [true, false] {
        let progress = Arc::new(Progress::new(1, store.delivered()?));
        progress.journal_done.store(true, Ordering::Release);
        let (ready, _) = tokio::sync::oneshot::channel();
        let result = output::run_store(
            store.delivery_connection()?,
            id.clone(),
            progress,
            Arc::new(AtomicBool::new(false)),
            ready,
            |batch, position| {
                assert!(matches!(batch, output::OutputBatch::Rejected { .. }));
                attempts.push((
                    position.sequence,
                    position.timestamp,
                    serde_json::to_string(batch).unwrap(),
                ));
                if fail {
                    Err(output::DeliveryError {
                        next_event: 0,
                        reason: "거절 전달 실패 주입".into(),
                    })
                } else {
                    Ok(())
                }
            },
            |_| Ok(()),
        );
        assert_eq!(result.is_err(), fail);
        assert_eq!(store.delivered()?, if fail { 0 } else { 1 });
    }
    assert_eq!(attempts[0], attempts[1]);
    Ok(())
}

#[test]
fn journal_rejection_contract_blocks_version_three_without_rewriting() -> Result<()> {
    let directory = Directory::new();
    let store = initialize(&directory.0);
    store.connection.execute_batch("PRAGMA user_version=3")?;
    drop(store);
    assert!(Store::open(&directory.0).is_err());
    let connection = rusqlite::Connection::open(directory.0.join("journal.sqlite3"))?;
    assert_eq!(
        connection.query_row("PRAGMA user_version", [], |row| row.get::<_, u32>(0))?,
        3
    );
    Ok(())
}

fn deposit_credit(id: uuid::Uuid, user: u64, value: Decimal) -> super::super::DepositCredit {
    super::super::DepositCredit {
        deposit_id: id,
        user_id: user,
        asset: "BTC".into(),
        amount: value,
    }
}

#[test]
fn deposit_once_preserves_locked_balance_and_duplicate_has_no_financial_output() {
    let mut state = seeded();
    state
        .executor
        .balance_cache_mut()
        .set_balance(3, "BTC", Decimal::ONE, Decimal::from(2));
    let credit = deposit_credit(uuid::Uuid::new_v4(), 3, Decimal::new(125, 8));
    assert!(matches!(
        apply(&mut state, &Command::CreditDeposit(credit.clone())),
        Outcome::Applied(Some(output::OutputBatch::DepositCredited(_)))
    ));
    assert!(matches!(
        apply(&mut state, &Command::CreditDeposit(credit)),
        Outcome::Applied(None)
    ));
    let balance = state
        .executor
        .balance_cache()
        .get_balance(3, "BTC")
        .unwrap();
    assert_eq!(balance.available, Decimal::ONE + Decimal::new(125, 8));
    assert_eq!(balance.locked, Decimal::from(2));
}

#[test]
fn deposit_identifier_cannot_change_user_asset_or_amount() {
    let mut state = seeded();
    let credit = deposit_credit(uuid::Uuid::new_v4(), 3, Decimal::ONE);
    assert!(matches!(
        apply(&mut state, &Command::CreditDeposit(credit.clone())),
        Outcome::Applied(Some(_))
    ));
    let mut other_user = credit.clone();
    other_user.user_id = 4;
    let mut other_asset = credit.clone();
    other_asset.asset = "USDT".into();
    let mut other_amount = credit.clone();
    other_amount.amount = Decimal::from(2);
    for changed in [other_user, other_asset, other_amount] {
        assert!(matches!(
            apply(&mut state, &Command::CreditDeposit(changed)),
            Outcome::Failed(_)
        ));
    }
    assert_eq!(
        state
            .executor
            .balance_cache()
            .get_balance(3, "BTC")
            .unwrap()
            .available,
        Decimal::ONE
    );
    assert!(state
        .executor
        .balance_cache()
        .get_balance(4, "BTC")
        .is_none());
    assert!(state
        .executor
        .balance_cache()
        .get_balance(3, "USDT")
        .is_none());
}

#[test]
fn invalid_deposit_never_creates_account_or_consumes_identifier() {
    let mut state = seeded();
    let original = deposit_credit(uuid::Uuid::new_v4(), 3, Decimal::ONE);
    let mut invalid = vec![];
    for value in [Decimal::ZERO, -Decimal::ONE, Decimal::new(1, 10)] {
        let mut one = original.clone();
        one.amount = value;
        invalid.push(one);
    }
    let mut nil = original.clone();
    nil.deposit_id = uuid::Uuid::nil();
    invalid.push(nil);
    let mut zero = original.clone();
    zero.user_id = 0;
    invalid.push(zero);
    for asset in [
        "".to_string(),
        " BTC".into(),
        "BTC ".into(),
        "x".repeat(129),
    ] {
        let mut one = original.clone();
        one.asset = asset;
        invalid.push(one);
    }
    for one in invalid {
        assert!(matches!(
            apply(&mut state, &Command::CreditDeposit(one)),
            Outcome::Failed(_)
        ));
    }
    assert!(state
        .executor
        .balance_cache()
        .get_balance(3, "BTC")
        .is_none());
    assert!(state.deposit_receipts.is_empty());
    assert!(matches!(
        apply(&mut state, &Command::CreditDeposit(original)),
        Outcome::Applied(Some(_))
    ));
}

#[test]
fn valid_deposit_overflow_keeps_original_and_repeated_failure() {
    let mut state = seeded();
    let maximum = Decimal::from_str_exact("999999999999999999999").unwrap();
    state
        .executor
        .balance_cache_mut()
        .set_balance(3, "BTC", maximum, Decimal::from(2));
    let credit = deposit_credit(uuid::Uuid::new_v4(), 3, maximum);
    let failed = apply(&mut state, &Command::CreditDeposit(credit.clone()));
    assert!(matches!(failed, Outcome::Failed(_)));
    assert_eq!(
        state
            .executor
            .balance_cache()
            .get_balance(3, "BTC")
            .unwrap()
            .available,
        maximum
    );
    // 원래 계산 실패 이후 잔액이 달라져도 같은 입금을 성공으로 바꿔 답하지 않는다.
    state
        .executor
        .balance_cache_mut()
        .set_balance(3, "BTC", Decimal::ZERO, Decimal::from(2));
    let repeated = apply(&mut state, &Command::CreditDeposit(credit.clone()));
    assert_eq!(
        serde_json::to_string(&failed).unwrap(),
        serde_json::to_string(&repeated).unwrap()
    );
    let mut changed = credit;
    changed.amount = Decimal::ONE;
    assert!(matches!(
        apply(&mut state, &Command::CreditDeposit(changed)),
        Outcome::Failed(_)
    ));
    assert_eq!(
        state
            .executor
            .balance_cache()
            .get_balance(3, "BTC")
            .unwrap()
            .available,
        Decimal::ZERO
    );
}

#[tokio::test]
async fn deposit_real_worker_concurrent_retries_and_restart_credit_once() -> Result<()> {
    let directory = Directory::new();
    let store = initialize(&directory.0);
    let mut engine = HighPerformanceEngine::new_bench();
    let (sent, received) = bounded(10);
    attach(&mut engine, store, move |batch, position| {
        output::visit_events(batch, position, |_, payload| {
            sent.send(payload).unwrap();
            Ok(())
        })
    });
    let credit = deposit_credit(uuid::Uuid::new_v4(), 3, Decimal::new(12345678, 8));
    let handle = engine.handle();
    let (one, two) = tokio::join!(
        handle.credit_deposit(credit.clone()),
        handle.credit_deposit(credit.clone())
    );
    one?;
    two?;
    assert_eq!(
        handle.get_balance(3, "BTC").await?,
        (credit.amount, Decimal::ZERO)
    );
    engine.stop().await?;
    let events: Vec<_> = received.try_iter().collect();
    assert_eq!(events.len(), 1);
    let event: serde_json::Value = serde_json::from_str(&events[0])?;
    assert_eq!(event["event_type"], "deposit_credited");
    assert_eq!(event["deposit_id"], credit.deposit_id.to_string());
    assert_eq!(event["amount"], "0.12345678");
    let store = Store::open(&directory.0)?;
    let mut restarted = HighPerformanceEngine::new_bench();
    let (sent, received) = bounded(10);
    attach(&mut restarted, store, move |batch, position| {
        output::visit_events(batch, position, |_, payload| {
            sent.send(payload).unwrap();
            Ok(())
        })
    });
    restarted.handle().credit_deposit(credit.clone()).await?;
    assert_eq!(
        restarted.handle().get_balance(3, "BTC").await?,
        (credit.amount, Decimal::ZERO)
    );
    let mut conflict = credit;
    conflict.amount = Decimal::ONE;
    assert!(restarted.handle().credit_deposit(conflict).await.is_err());
    restarted.stop().await?;
    assert_eq!(received.try_iter().count(), 0);
    Ok(())
}

#[tokio::test]
async fn deposit_output_failure_republishes_same_event_after_recovery() -> Result<()> {
    let directory = Directory::new();
    let store = initialize(&directory.0);
    let mut engine = HighPerformanceEngine::new_bench();
    let (sent, received) = bounded(10);
    attach(&mut engine, store, move |batch, position| {
        output::visit_events(batch, position, |index, payload| {
            sent.send(payload).unwrap();
            Err(output::DeliveryError {
                next_event: index,
                reason: "시험용 브로커 응답 유실".into(),
            })
        })
    });
    let credit = deposit_credit(uuid::Uuid::new_v4(), 3, Decimal::ONE);
    engine.handle().credit_deposit(credit.clone()).await?;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while engine.handle().is_accepting() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(engine.stop().await.is_err());
    let original = received.try_recv()?;
    let store = Store::open(&directory.0)?;
    assert_eq!(store.delivered()?, 0);
    let mut restarted = HighPerformanceEngine::new_bench();
    let (sent, received) = bounded(10);
    attach(&mut restarted, store, move |batch, position| {
        output::visit_events(batch, position, |_, payload| {
            sent.send(payload).unwrap();
            Ok(())
        })
    });
    restarted.handle().credit_deposit(credit).await?;
    restarted.stop().await?;
    assert_eq!(received.try_recv()?, original);
    assert!(received.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn deposit_journal_failure_never_acknowledges_or_publishes() -> Result<()> {
    let directory = Directory::new();
    let store = initialize(&directory.0);
    store.connection.execute_batch("CREATE TRIGGER reject_deposit_write BEFORE INSERT ON records BEGIN SELECT RAISE(ABORT,'시험용 기록 실패'); END;")?;
    let mut engine = HighPerformanceEngine::new_bench();
    let (sent, received) = bounded(10);
    attach(&mut engine, store, move |batch, position| {
        output::visit_events(batch, position, |_, payload| {
            sent.send(payload).unwrap();
            Ok(())
        })
    });
    assert!(engine
        .handle()
        .credit_deposit(deposit_credit(uuid::Uuid::new_v4(), 3, Decimal::ONE))
        .await
        .is_err());
    assert!(engine.stop().await.is_err());
    assert!(received.try_recv().is_err());
    let store = Store::open(&directory.0)?;
    assert_eq!(store.last_sequence()?, 0);
    let (recovered, _) = store.recover()?;
    assert!(recovered
        .executor
        .balance_cache()
        .get_balance(3, "BTC")
        .is_none());
    Ok(())
}

#[test]
fn deposit_failed_receipt_replays_as_failure_and_keeps_original_payload() -> Result<()> {
    let directory = Directory::new();
    let mut store = Store::open(&directory.0)?;
    let mut state = seeded();
    let maximum = Decimal::from_str_exact("999999999999999999999")?;
    state
        .executor
        .balance_cache_mut()
        .set_balance(3, "BTC", maximum, Decimal::ZERO);
    store.initialize(&state)?;
    let credit = deposit_credit(uuid::Uuid::new_v4(), 3, maximum);
    let item = pending(&mut state, Command::CreditDeposit(credit.clone()));
    assert!(matches!(item.outcome, Outcome::Failed(_)));
    store.append(&[item], &mut 0)?;
    let (mut recovered, _) = store.recover()?;
    assert!(matches!(
        apply(&mut recovered, &Command::CreditDeposit(credit.clone())),
        Outcome::Failed(_)
    ));
    let mut changed = credit;
    changed.amount = Decimal::ONE;
    match apply(&mut recovered, &Command::CreditDeposit(changed)) {
        Outcome::Failed(reason) => assert!(reason.contains("conflicts")),
        _ => panic!("실패 원본 변경을 허용함"),
    }
    assert_eq!(
        recovered
            .executor
            .balance_cache()
            .get_balance(3, "BTC")
            .unwrap()
            .available,
        maximum
    );
    Ok(())
}
