//! Matcher에서 Executor까지 실제 명령 처리 경계를 검증한다. DB·Kafka는 실행하지 않는다.
use super::*;

fn order(id: u64, user: u64, order_type: &str, market: bool) -> OrderEntry {
    let market_buy = market && order_type == "buy";
    OrderEntry {
        id,
        user_id: user,
        order_type: order_type.into(),
        order_side: if market { "market" } else { "limit" }.into(),
        base_mint: "SOL".into(),
        quote_mint: "USDT".into(),
        price: if market {
            None
        } else {
            Some(Decimal::from(100))
        },
        amount: if market_buy {
            Decimal::ZERO
        } else {
            Decimal::from(2)
        },
        quote_amount: if market_buy {
            Some(Decimal::from(200))
        } else {
            None
        },
        filled_amount: Decimal::ZERO,
        remaining_amount: if market_buy {
            Decimal::ZERO
        } else {
            Decimal::from(2)
        },
        remaining_quote_amount: if market_buy {
            Some(Decimal::from(200))
        } else {
            None
        },
        created_at: chrono::Utc::now(),
    }
}

#[test]
fn real_matching_pipeline_settles_both_assets_for_limit_and_market() -> Result<()> {
    for market in [false, true] {
        let mut engine = HighPerformanceEngine::new_bench();
        engine.bench_set_balance(101, "USDT", Decimal::from(1000), Decimal::ZERO);
        engine.bench_set_balance(102, "SOL", Decimal::from(10), Decimal::ZERO);
        assert!(engine
            .bench_submit_direct(order(2, 102, "sell", false))?
            .is_empty());
        let matches = engine.bench_submit_direct(order(1, 101, "buy", market))?;
        assert_eq!(matches.len(), 1);
        let executor = &engine.state.as_ref().unwrap().executor;
        let cache = executor.balance_cache();
        assert_eq!(
            cache.get_balance(101, "USDT").unwrap().available,
            Decimal::from(800)
        );
        assert_eq!(
            cache.get_balance(101, "USDT").unwrap().locked,
            Decimal::ZERO
        );
        assert_eq!(
            cache.get_balance(102, "USDT").unwrap().available,
            Decimal::from(200)
        );
        assert_eq!(cache.get_balance(102, "SOL").unwrap().locked, Decimal::ZERO);
        assert_eq!(
            cache.get_balance(101, "SOL").unwrap().available,
            Decimal::from(2)
        );
    }
    Ok(())
}

#[test]
fn settlement_failure_aborts_success_followups_for_limit_and_market() -> Result<()> {
    for market in [false, true] {
        let mut engine = HighPerformanceEngine::new_bench();
        engine.bench_set_balance(101, "USDT", Decimal::from(1000), Decimal::ZERO);
        engine.bench_set_balance(102, "SOL", Decimal::from(10), Decimal::ZERO);
        engine.bench_submit_direct(order(2, 102, "sell", false))?;
        // 주문장에는 매도 2개가 있지만 예약이 1개뿐인 손상 상태를 주입한다.
        engine.bench_set_balance(102, "SOL", Decimal::from(8), Decimal::ONE);

        let state = engine.state.as_mut().unwrap();
        let result = super::super::threads::process_submit_order(
            order(1, 101, "buy", market),
            &mut state.orderbooks,
            &state.matcher,
            &mut state.executor,
        );
        assert!(result.is_err(), "정산 실패를 처리 성공으로 반환했다");
        let executor = &engine.state.as_ref().unwrap().executor;
        let cache = executor.balance_cache();
        // 후보 전체가 실패했으므로 실행 캐시의 신규 예약도 반영하지 않는다.
        // 별도로 커밋된 플랫폼 DB 예약을 해제하는 동작은 하지 않는다.
        assert_eq!(
            cache.get_balance(101, "USDT").unwrap().locked,
            Decimal::ZERO
        );
        assert!(cache.get_balance(102, "USDT").is_none());
        assert!(cache.get_balance(101, "SOL").is_none());
    }
    Ok(())
}

fn book_snapshot(engine: &HighPerformanceEngine) -> serde_json::Value {
    let books = &engine.state.as_ref().unwrap().orderbooks;
    let pair = TradingPair::new("SOL".into(), "USDT".into());
    match books.get(&pair) {
        None => serde_json::Value::Null,
        Some(book) => serde_json::json!({
            "buys":book.buy_orders.orders,"sells":book.sell_orders.orders,
            "buy_count":book.buy_orders.total_orders(),"sell_count":book.sell_orders.total_orders()}),
    }
}

#[test]
fn submission_second_fill_failure_preserves_entire_book_and_cache() -> Result<()> {
    // 매수·매도 × 지정가·시장가 네 경로 모두 두 번째 체결만 실패시킨다.
    for buy in [false, true] {
        for market in [false, true] {
            let mut engine = HighPerformanceEngine::new_bench();
            let taker_mint = if buy { "USDT" } else { "SOL" };
            let maker_mint = if buy { "SOL" } else { "USDT" };
            engine.bench_set_balance(101, taker_mint, Decimal::from(1000), Decimal::ZERO);
            for (id, user) in [(2, 102), (3, 103)] {
                engine.bench_set_balance(user, maker_mint, Decimal::from(1000), Decimal::ZERO);
                engine.bench_submit_direct(order(
                    id,
                    user,
                    if buy { "sell" } else { "buy" },
                    false,
                ))?;
            }
            // 첫 상대는 정산 가능하고 두 번째 상대만 예약을 부족하게 만든다.
            engine.bench_set_balance(103, maker_mint, Decimal::from(800), Decimal::ONE);
            let before_book = book_snapshot(&engine);
            let before_cache = engine
                .state
                .as_mut()
                .unwrap()
                .executor
                .balance_cache()
                .test_balances_snapshot();
            let mut incoming = order(1, 101, if buy { "buy" } else { "sell" }, market);
            if buy && market {
                incoming.quote_amount = Some(Decimal::from(400));
                incoming.remaining_quote_amount = incoming.quote_amount;
            } else {
                incoming.amount = Decimal::from(4);
                incoming.remaining_amount = incoming.amount;
            }

            let state = engine.state.as_mut().unwrap();
            let result = super::super::threads::process_submit_order(
                incoming,
                &mut state.orderbooks,
                &state.matcher,
                &mut state.executor,
            );
            assert!(result.is_err());
            assert_eq!(
                book_snapshot(&engine),
                before_book,
                "실패가 주문·FIFO·카운터를 바꿨다"
            );
            assert_eq!(
                engine
                    .state
                    .as_mut()
                    .unwrap()
                    .executor
                    .balance_cache()
                    .test_balances_snapshot(),
                before_cache,
                "앞 체결이나 신규 주문 예약이 일부 반영됐다"
            );
        }
    }
    Ok(())
}

#[test]
fn submission_reservation_failure_creates_neither_market_nor_zero_balance() {
    let mut engine = HighPerformanceEngine::new_bench();
    let before = engine
        .state
        .as_mut()
        .unwrap()
        .executor
        .balance_cache()
        .test_balances_snapshot();
    assert!(engine
        .bench_submit_direct(order(1, 101, "buy", false))
        .is_err());
    assert_eq!(book_snapshot(&engine), serde_json::Value::Null);
    assert_eq!(
        engine
            .state
            .as_mut()
            .unwrap()
            .executor
            .balance_cache()
            .test_balances_snapshot(),
        before
    );
}

#[test]
fn submission_price_time_priority_and_partial_maker_counters_are_preserved() -> Result<()> {
    let mut engine = HighPerformanceEngine::new_bench();
    engine.bench_set_balance(101, "USDT", Decimal::from(1000), Decimal::ZERO);
    for (id, user, price) in [(2, 102, 100), (3, 103, 100), (4, 104, 101)] {
        engine.bench_set_balance(user, "SOL", Decimal::from(10), Decimal::ZERO);
        let mut maker = order(id, user, "sell", false);
        maker.price = Some(Decimal::from(price));
        engine.bench_submit_direct(maker)?;
    }
    let mut incoming = order(1, 101, "buy", false);
    incoming.price = Some(Decimal::from(110));
    incoming.amount = Decimal::from(5);
    incoming.remaining_amount = incoming.amount;
    let fills = engine.bench_submit_direct(incoming)?;
    assert_eq!(
        fills
            .iter()
            .map(|trade| trade.sell_order_id)
            .collect::<Vec<_>>(),
        vec![2, 3, 4]
    );
    assert_eq!(
        fills.iter().map(|trade| trade.amount).collect::<Vec<_>>(),
        vec![Decimal::from(2), Decimal::from(2), Decimal::ONE]
    );
    let books = &engine.state.as_ref().unwrap().orderbooks;
    let book = books
        .get(&TradingPair::new("SOL".into(), "USDT".into()))
        .unwrap();
    assert_eq!(book.sell_orders.total_orders(), 1);
    let maker = &book.sell_orders.orders[&Decimal::from(101)][0];
    assert_eq!(
        (maker.id, maker.remaining_amount, maker.filled_amount),
        (4, Decimal::ONE, Decimal::ONE)
    );
    let executor = &engine.state.as_ref().unwrap().executor;
    let cache = executor.balance_cache();
    assert_eq!(
        cache.get_balance(101, "USDT").unwrap().available,
        Decimal::from(499)
    );
    assert_eq!(
        cache.get_balance(101, "USDT").unwrap().locked,
        Decimal::ZERO
    );
    Ok(())
}

#[test]
fn submission_partial_limit_buy_refunds_improvement_and_keeps_only_remaining_reservation(
) -> Result<()> {
    let mut engine = HighPerformanceEngine::new_bench();
    engine.bench_set_balance(101, "USDT", Decimal::from(1000), Decimal::ZERO);
    engine.bench_set_balance(102, "SOL", Decimal::from(10), Decimal::ZERO);
    let mut maker = order(2, 102, "sell", false);
    maker.price = Some(Decimal::from(90));
    engine.bench_submit_direct(maker)?;
    let mut incoming = order(1, 101, "buy", false);
    incoming.amount = Decimal::from(4);
    incoming.remaining_amount = incoming.amount;
    engine.bench_submit_direct(incoming)?;
    {
        let executor = &engine.state.as_ref().unwrap().executor;
        let balance = executor.balance_cache().get_balance(101, "USDT").unwrap();
        assert_eq!(
            (balance.available, balance.locked),
            (Decimal::from(620), Decimal::from(200))
        );
    }
    // 잔여 주문은 다음 명령에서 정상 maker로 다시 체결될 수 있어야 한다.
    engine.bench_set_balance(103, "SOL", Decimal::from(10), Decimal::ZERO);
    let mut later = order(3, 103, "sell", false);
    later.amount = Decimal::ONE;
    later.remaining_amount = Decimal::ONE;
    assert_eq!(engine.bench_submit_direct(later)?.len(), 1);
    let executor = &engine.state.as_ref().unwrap().executor;
    let cache = executor.balance_cache();
    assert_eq!(
        cache.get_balance(101, "USDT").unwrap().locked,
        Decimal::from(100)
    );
    assert_eq!(
        cache.get_balance(101, "SOL").unwrap().available,
        Decimal::from(3)
    );
    Ok(())
}

#[test]
fn submission_market_budget_uses_exact_quantity_and_returns_dust() -> Result<()> {
    for (price, expected_quantity) in [
        (Decimal::from(3), Decimal::new(333333333, 9)),
        (Decimal::new(3, 1), Decimal::new(3333333330, 9)),
    ] {
        let mut engine = HighPerformanceEngine::new_bench();
        engine.bench_set_balance(101, "USDT", Decimal::from(10), Decimal::ZERO);
        engine.bench_set_balance(102, "SOL", Decimal::from(20), Decimal::ZERO);
        let mut maker = order(2, 102, "sell", false);
        maker.price = Some(price);
        maker.amount = Decimal::from(10);
        maker.remaining_amount = maker.amount;
        engine.bench_submit_direct(maker)?;
        let mut incoming = order(1, 101, "buy", true);
        incoming.quote_amount = Some(Decimal::ONE);
        incoming.remaining_quote_amount = incoming.quote_amount;
        let fills = engine.bench_submit_direct(incoming)?;
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].amount, expected_quantity);
        let executor = &engine.state.as_ref().unwrap().executor;
        let cache = executor.balance_cache();
        assert_eq!(
            cache.get_balance(101, "USDT").unwrap().available,
            Decimal::new(9000000001, 9)
        );
        assert_eq!(
            cache.get_balance(101, "USDT").unwrap().locked,
            Decimal::ZERO
        );
        assert_eq!(
            cache.get_balance(102, "USDT").unwrap().available,
            Decimal::new(999999999, 9)
        );
    }
    Ok(())
}

#[test]
fn submission_no_liquidity_market_orders_return_entire_reservation() -> Result<()> {
    for buy in [true, false] {
        let mut engine = HighPerformanceEngine::new_bench();
        let asset = if buy { "USDT" } else { "SOL" };
        engine.bench_set_balance(101, asset, Decimal::from(1000), Decimal::ZERO);
        let before = engine
            .state
            .as_mut()
            .unwrap()
            .executor
            .balance_cache()
            .test_balances_snapshot();
        assert!(engine
            .bench_submit_direct(order(1, 101, if buy { "buy" } else { "sell" }, true))?
            .is_empty());
        assert_eq!(
            engine
                .state
                .as_mut()
                .unwrap()
                .executor
                .balance_cache()
                .test_balances_snapshot(),
            before
        );
    }
    Ok(())
}

#[test]
fn submission_self_orders_do_not_rotate_or_disappear() -> Result<()> {
    let mut engine = HighPerformanceEngine::new_bench();
    engine.bench_set_balance(101, "USDT", Decimal::from(1000), Decimal::ZERO);
    for (id, user) in [(2, 101), (3, 102), (4, 103)] {
        engine.bench_set_balance(user, "SOL", Decimal::from(10), Decimal::ZERO);
        engine.bench_submit_direct(order(id, user, "sell", false))?;
    }
    let mut incoming = order(1, 101, "buy", false);
    incoming.amount = Decimal::ONE;
    incoming.remaining_amount = Decimal::ONE;
    let fills = engine.bench_submit_direct(incoming)?;
    assert_eq!(fills[0].sell_order_id, 3);
    let books = &engine.state.as_ref().unwrap().orderbooks;
    let book = books.values().next().unwrap();
    assert_eq!(
        book.sell_orders.orders[&Decimal::from(100)]
            .iter()
            .map(|order| order.id)
            .collect::<Vec<_>>(),
        vec![2, 3, 4]
    );
    assert_eq!(book.sell_orders.total_orders(), 3);
    Ok(())
}

#[test]
fn submission_only_self_liquidity_finishes_without_mutation_or_infinite_loop() -> Result<()> {
    for buy in [false, true] {
        let mut engine = HighPerformanceEngine::new_bench();
        engine.bench_set_balance(101, "USDT", Decimal::from(1000), Decimal::ZERO);
        engine.bench_set_balance(101, "SOL", Decimal::from(10), Decimal::ZERO);
        engine.bench_submit_direct(order(2, 101, if buy { "sell" } else { "buy" }, false))?;
        let before_book = book_snapshot(&engine);
        let before_cache = engine
            .state
            .as_mut()
            .unwrap()
            .executor
            .balance_cache()
            .test_balances_snapshot();
        assert!(engine
            .bench_submit_direct(order(1, 101, if buy { "buy" } else { "sell" }, true))?
            .is_empty());
        assert_eq!(book_snapshot(&engine), before_book);
        assert_eq!(
            engine
                .state
                .as_mut()
                .unwrap()
                .executor
                .balance_cache()
                .test_balances_snapshot(),
            before_cache
        );
    }
    Ok(())
}

#[test]
fn submission_refund_precision_failure_preserves_book_and_entire_cache() -> Result<()> {
    let mut engine = HighPerformanceEngine::new_bench();
    engine.bench_set_balance(
        101,
        "USDT",
        Decimal::from_str_exact("100000000000000000000")?,
        Decimal::ZERO,
    );
    engine.bench_set_balance(102, "SOL", Decimal::from(10), Decimal::ZERO);
    let mut maker = order(2, 102, "sell", false);
    maker.price = Some(Decimal::from_str_exact("99.999999999")?);
    engine.bench_submit_direct(maker)?;
    let before_book = book_snapshot(&engine);
    let before_cache = engine
        .state
        .as_mut()
        .unwrap()
        .executor
        .balance_cache()
        .test_balances_snapshot();

    let state = engine.state.as_mut().unwrap();
    assert!(super::super::threads::process_submit_order(
        order(1, 101, "buy", false),
        &mut state.orderbooks,
        &state.matcher,
        &mut state.executor
    )
    .is_err());
    assert_eq!(book_snapshot(&engine), before_book);
    assert_eq!(
        engine
            .state
            .as_mut()
            .unwrap()
            .executor
            .balance_cache()
            .test_balances_snapshot(),
        before_cache
    );
    Ok(())
}

#[test]
fn submission_benchmark_uses_same_core_and_failure_boundary() -> Result<()> {
    let mut engine = HighPerformanceEngine::new_bench();
    let mut bench = super::super::bench_engine::BenchEngine::new();
    for (user, asset) in [(101, "USDT"), (102, "SOL")] {
        engine.bench_set_balance(user, asset, Decimal::from(1000), Decimal::ZERO);
        bench.set_balance(user, asset, Decimal::from(1000), Decimal::ZERO);
    }
    let maker = order(2, 102, "sell", false);
    engine.bench_submit_direct(maker.clone())?;
    bench.submit_direct(maker)?;
    let incoming = order(1, 101, "buy", false);
    assert_eq!(
        serde_json::to_value(engine.bench_submit_direct(incoming.clone())?)?,
        serde_json::to_value(bench.submit_direct(incoming)?)?
    );
    assert!(bench.submit_direct(order(3, 999, "buy", false)).is_err());
    Ok(())
}

#[test]
fn submission_invalid_candidate_returns_error_without_panic_or_state_change() {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    for invalid in 0..4 {
        let mut engine = HighPerformanceEngine::new_bench();
        engine.bench_set_balance(101, "USDT", Decimal::from(1000), Decimal::ZERO);
        let before = engine
            .state
            .as_mut()
            .unwrap()
            .executor
            .balance_cache()
            .test_balances_snapshot();
        let mut incoming = order(1, 101, "buy", false);
        match invalid {
            0 => incoming.price = None,
            1 => incoming.amount = Decimal::MAX,
            2 => incoming.order_type = "unknown".into(),
            _ => incoming.remaining_amount = Decimal::ZERO,
        }
        let result = catch_unwind(AssertUnwindSafe(|| engine.bench_submit_direct(incoming)));
        assert!(result.is_ok());
        assert!(result.unwrap().is_err());
        assert_eq!(book_snapshot(&engine), serde_json::Value::Null);
        assert_eq!(
            engine
                .state
                .as_mut()
                .unwrap()
                .executor
                .balance_cache()
                .test_balances_snapshot(),
            before
        );
    }
}

#[test]
fn submission_candidate_planning_never_mutates_original_orderbook() -> Result<()> {
    let mut engine = HighPerformanceEngine::new_bench();
    engine.bench_set_balance(102, "SOL", Decimal::from(10), Decimal::ZERO);
    engine.bench_submit_direct(order(2, 102, "sell", false))?;
    let before = book_snapshot(&engine);
    {
        let books = &engine.state.as_ref().unwrap().orderbooks;
        let book = books.values().next().unwrap();
        let incoming = order(1, 101, "buy", false);
        let plan = engine
            .state
            .as_ref()
            .unwrap()
            .matcher
            .plan_order(&incoming, book)?;
        assert_eq!(plan.matches.len(), 1);
        assert_eq!(incoming.remaining_amount, Decimal::from(2));
    }
    assert_eq!(book_snapshot(&engine), before);
    Ok(())
}
