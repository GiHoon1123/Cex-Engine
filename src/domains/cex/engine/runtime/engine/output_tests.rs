//! 실제 엔진 루프와 출력 워커의 포화·실패·종료 경계를 검증한다.
//! 디비와 브로커를 쓰지 않는 출력 대역만 주입하며 거래 로직은 제품 경로다.
use super::super::{
    output::{self, DeliveryError, OutputBatch},
    threads,
};
use super::*;
use crate::domains::cex::engine::Engine;
use std::sync::Mutex;

fn order(id: u64) -> OrderEntry {
    OrderEntry {
        id,
        user_id: 1,
        order_type: "buy".into(),
        order_side: "limit".into(),
        base_mint: "SOL".into(),
        quote_mint: "USDT".into(),
        price: Some(Decimal::from(100)),
        amount: Decimal::ONE,
        quote_amount: None,
        filled_amount: Decimal::ZERO,
        remaining_amount: Decimal::ONE,
        remaining_quote_amount: None,
        created_at: chrono::Utc::now(),
    }
}
fn batch(id: u64) -> OutputBatch {
    OutputBatch::Submitted {
        order_id: id,
        trades: Vec::new(),
    }
}
fn seed() -> HighPerformanceEngine {
    let mut engine = HighPerformanceEngine::new_bench();
    engine.bench_set_balance(1, "USDT", Decimal::from(1000), Decimal::ZERO);
    engine
}
fn start_core(
    engine: &mut HighPerformanceEngine,
    sender: crossbeam::channel::Sender<OutputBatch>,
    receiver: Receiver<OutputBatch>,
) {
    engine.output_rx = Some(receiver);
    let state = engine.state.take().unwrap();
    let orders = engine.order_rx.clone();
    let balances = engine.balance_rx.clone();
    let running = engine.running.clone();
    let closed = engine.handle.closed.clone();
    running.store(true, Ordering::Release);
    engine.engine_thread = Some(thread::spawn(move || {
        threads::engine_thread_loop(orders, balances, Some(sender), state, running, closed)
    }));
}
fn start_with_sink(
    engine: &mut HighPerformanceEngine,
    sink: impl FnMut(&OutputBatch) -> std::result::Result<(), DeliveryError> + Send + 'static,
) {
    let (sender, receiver) = bounded(8);
    let closed = engine.handle.closed.clone();
    let consumer = receiver.clone();
    engine.output_thread = Some(thread::spawn(move || output::run(consumer, closed, sink)));
    start_core(engine, sender, receiver);
}

#[tokio::test]
async fn output_full_queue_preserves_command_and_trading_state() -> Result<()> {
    let mut engine = seed();
    let (sender, receiver) = bounded(1);
    sender.try_send(batch(0)).unwrap();
    assert!(!output::has_capacity(Some(&sender)));
    start_core(&mut engine, sender, receiver);
    engine.handle().submit_order(order(1)).await?;
    // 가득 찬 출력 큐에서 입력을 소비하지 않는지 확인한다. 시험 실패 때도
    // 먼저 워커를 정지시켜 바쁜 반복이 테스트 뒤에 남지 않게 한다.
    let consumed = tokio::time::timeout(std::time::Duration::from_millis(25), async {
        while engine.order_rx.len() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let stopped = engine.stop().await;
    assert!(consumed.is_err(), "full output queue consumed an input");
    assert!(
        stopped.is_err(),
        "undelivered output was reported as drained"
    );
    assert_eq!(engine.pending_output_count(), 1);
    assert_eq!(engine.order_rx.len(), 1);
    let state = engine.state.as_ref().unwrap();
    assert!(state.orderbooks.is_empty());
    let balance = state
        .executor
        .balance_cache()
        .get_balance(1, "USDT")
        .unwrap();
    assert_eq!(
        (balance.available, balance.locked),
        (Decimal::from(1000), Decimal::ZERO)
    );
    Ok(())
}

#[tokio::test]
async fn output_capacity_release_resumes_without_losing_a_batch() -> Result<()> {
    let mut engine = seed();
    let (sender, receiver) = bounded(1);
    sender.try_send(batch(0)).unwrap();
    let observer = receiver.clone();
    start_core(&mut engine, sender, receiver);
    engine.handle().submit_order(order(1)).await?;
    let first = observer.recv_timeout(std::time::Duration::from_secs(1))?;
    let second = observer.recv_timeout(std::time::Duration::from_secs(1))?;
    engine.stop().await?;
    assert!(matches!(first, OutputBatch::Submitted { order_id: 0, .. }));
    assert!(matches!(second, OutputBatch::Submitted { order_id: 1, .. }));
    assert_eq!(engine.pending_output_count(), 0);
    let balance = engine
        .state
        .as_ref()
        .unwrap()
        .executor
        .balance_cache()
        .get_balance(1, "USDT")
        .unwrap();
    assert_eq!(
        (balance.available, balance.locked),
        (Decimal::from(900), Decimal::from(100))
    );
    Ok(())
}

#[tokio::test]
async fn output_worker_drains_in_fifo_order_on_one_thread() -> Result<()> {
    let mut engine = seed();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let captured = seen.clone();
    start_with_sink(&mut engine, move |batch| {
        if let OutputBatch::Submitted { order_id, .. } = batch {
            captured
                .lock()
                .unwrap()
                .push((*order_id, thread::current().id()));
        }
        Ok(())
    });
    let handle = engine.handle();
    handle.submit_order(order(1)).await?;
    handle.submit_order(order(2)).await?;
    let checked = handle
        .get_orderbook(&TradingPair::new("SOL".into(), "USDT".into()), None)
        .await;
    engine.stop().await?;
    assert_eq!(checked?.0.len(), 2);
    let seen = seen.lock().unwrap();
    assert_eq!(
        seen.iter().map(|item| item.0).collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(seen[0].1, seen[1].1);
    assert_ne!(seen[0].1, thread::current().id());
    assert!(!handle.is_accepting());
    assert_eq!(engine.pending_output_count(), 0);
    Ok(())
}

#[tokio::test]
async fn output_delivery_failure_closes_input_and_retains_every_committed_batch() -> Result<()> {
    let mut engine = seed();
    start_with_sink(&mut engine, |_| {
        Err(DeliveryError {
            next_event: 0,
            reason: "injected delivery failure".into(),
        })
    });
    let handle = engine.handle();
    handle.submit_order(order(1)).await?;
    // 실패가 먼저 관측되면 두 번째 명령은 입구에서 거부될 수 있다.
    let _ = handle.submit_order(order(2)).await;
    let failed = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while handle.is_accepting() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let stopped = engine.stop().await;
    assert!(failed.is_ok());
    assert!(stopped
        .unwrap_err()
        .to_string()
        .contains("injected delivery failure"));
    assert!(handle.get_balance(1, "USDT").await.is_err());
    let state = engine.state.as_ref().unwrap();
    let count: usize = state
        .orderbooks
        .values()
        .map(|book| book.buy_orders.total_orders())
        .sum();
    assert_eq!(
        engine.pending_output_count(),
        count,
        "committed output was discarded"
    );
    assert!(matches!(
        engine.output_failure.as_ref().unwrap().batch,
        OutputBatch::Submitted { order_id: 1, .. }
    ));
    Ok(())
}

#[test]
fn output_failure_preserves_next_event_and_does_not_consume_later_batches() -> Result<()> {
    let mut engine = seed();
    engine.bench_set_balance(2, "SOL", Decimal::from(2), Decimal::ZERO);
    for id in [2, 3] {
        let mut maker = order(id);
        maker.user_id = 2;
        maker.order_type = "sell".into();
        engine.bench_submit_direct(maker)?;
    }
    let mut incoming = order(1);
    incoming.amount = Decimal::from(2);
    incoming.remaining_amount = incoming.amount;
    let state = engine.state.as_mut().unwrap();
    let applied = threads::process_submit_order(
        incoming,
        &mut state.orderbooks,
        &state.matcher,
        &mut state.executor,
    )?;
    let prepared = OutputBatch::submitted(applied, &state.executor);
    assert!(matches!(&prepared, OutputBatch::Submitted { trades, .. } if trades.len() == 2));
    let (sender, receiver) = bounded(2);
    sender.try_send(prepared).unwrap();
    sender.try_send(batch(2)).unwrap();
    let closed = Arc::new(AtomicBool::new(false));
    let failure = output::run(receiver.clone(), closed.clone(), |_| {
        Err(DeliveryError {
            next_event: 1,
            reason: "delivery confirmation lost".into(),
        })
    })
    .unwrap();
    assert!(closed.load(Ordering::Acquire));
    assert_eq!(failure.next_event, 1);
    assert!(matches!(
        failure.batch,
        OutputBatch::Submitted { order_id: 1, .. }
    ));
    assert!(matches!(
        receiver.try_recv().unwrap(),
        OutputBatch::Submitted { order_id: 2, .. }
    ));
    Ok(())
}

#[test]
fn output_cancel_failure_preserves_order_and_success_updates_counter() -> Result<()> {
    let mut engine = seed();
    engine.bench_submit_direct(order(1))?;
    let state = engine.state.as_mut().unwrap();
    let pair = TradingPair::new("SOL".into(), "USDT".into());
    state
        .executor
        .balance_cache_mut()
        .set_balance(1, "USDT", Decimal::from(900), Decimal::ZERO);
    let failed = threads::cancel_order(1, 1, &pair, &mut state.orderbooks, &mut state.executor);
    assert!(failed.is_err());
    assert_eq!(state.orderbooks[&pair].buy_orders.total_orders(), 1);
    state.executor.balance_cache_mut().set_balance(
        1,
        "USDT",
        Decimal::from(900),
        Decimal::from(100),
    );
    assert!(
        threads::cancel_order(1, 2, &pair, &mut state.orderbooks, &mut state.executor).is_err()
    );
    threads::cancel_order(1, 1, &pair, &mut state.orderbooks, &mut state.executor)?;
    assert_eq!(state.orderbooks[&pair].buy_orders.total_orders(), 0);
    let balance = state
        .executor
        .balance_cache()
        .get_balance(1, "USDT")
        .unwrap();
    assert_eq!(
        (balance.available, balance.locked),
        (Decimal::from(1000), Decimal::ZERO)
    );
    Ok(())
}

#[test]
fn output_cancel_invalid_book_entry_is_rejected_before_unlock() -> Result<()> {
    let mut engine = seed();
    engine.bench_submit_direct(order(1))?;
    let state = engine.state.as_mut().unwrap();
    let pair = TradingPair::new("SOL".into(), "USDT".into());
    state
        .orderbooks
        .get_mut(&pair)
        .unwrap()
        .buy_orders
        .orders
        .get_mut(&Decimal::from(100))
        .unwrap()
        .front_mut()
        .unwrap()
        .price = Some(Decimal::from(101));
    assert!(
        threads::cancel_order(1, 1, &pair, &mut state.orderbooks, &mut state.executor).is_err()
    );
    assert_eq!(state.orderbooks[&pair].buy_orders.total_orders(), 1);
    let balance = state
        .executor
        .balance_cache()
        .get_balance(1, "USDT")
        .unwrap();
    assert_eq!(
        (balance.available, balance.locked),
        (Decimal::from(900), Decimal::from(100))
    );
    Ok(())
}
