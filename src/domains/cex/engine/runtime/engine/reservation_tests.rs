//! 플랫폼 예약과 실행용 캐시 예약을 분리하는 회귀 검증이다.
use super::*;

#[test]
fn platform_reservation_is_not_written_to_database_twice() -> Result<()> {
    let mut engine = HighPerformanceEngine::new_bench();
    engine.bench_set_balance(101, "USDT", Decimal::from(1000), Decimal::ZERO);

    let order = OrderEntry {
        id: 1,
        user_id: 101,
        order_type: "buy".into(),
        order_side: "limit".into(),
        base_mint: "SOL".into(),
        quote_mint: "USDT".into(),
        price: Some(Decimal::from(100)),
        amount: Decimal::from(2),
        quote_amount: None,
        filled_amount: Decimal::ZERO,
        remaining_amount: Decimal::from(2),
        remaining_quote_amount: None,
        created_at: chrono::Utc::now(),
    };
    let state = engine.state.as_mut().unwrap();
    super::super::threads::process_submit_order(
        order,
        &mut state.orderbooks,
        &state.matcher,
        &mut state.executor,
    )?;
    let executor = &engine.state.as_ref().unwrap().executor;
    let balance = executor.balance_cache().get_balance(101, "USDT").unwrap();
    assert_eq!(balance.available, Decimal::from(800));
    assert_eq!(balance.locked, Decimal::from(200));

    Ok(())
}
