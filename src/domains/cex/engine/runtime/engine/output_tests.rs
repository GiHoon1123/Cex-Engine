//! 취소 계산의 실패 보존을 검증한다. 출력·기록 워커 검증은 journal::tests에서
//! 실제 영속 저널과 실제 코어 루프를 연결해 수행한다.
use super::super::{output::OutputBatch, threads};
use super::*;
fn order(id: u64) -> OrderEntry {
    OrderEntry {
        fee_rate: rust_decimal::Decimal::ZERO,
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
fn seed() -> HighPerformanceEngine {
    let mut engine = HighPerformanceEngine::new_bench();
    engine.bench_set_balance(1, "USDT", Decimal::from(1000), Decimal::ZERO);
    engine
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
