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
        output::run_store(delivery, id, p, closed, ready, sink)
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
                let checksum = solana_sdk::hash::hash(payload.as_bytes())
                    .to_bytes()
                    .to_vec();
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
        )
    });
    thread::sleep(std::time::Duration::from_millis(25));
    assert_eq!(captured.load(Ordering::SeqCst), 0);
    progress.durable.store(1, Ordering::Release);
    progress.journal_done.store(true, Ordering::Release);
    worker.join().unwrap()?;
    assert_eq!(captured.load(Ordering::SeqCst), 1);
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
    let item = pending(&mut state, Command::Submit(order(1, 1, true, 1)));
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
