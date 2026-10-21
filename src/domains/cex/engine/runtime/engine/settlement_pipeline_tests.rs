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
        let engine = HighPerformanceEngine::new_bench();
        engine.bench_set_balance(101, "USDT", Decimal::from(1000), Decimal::ZERO);
        engine.bench_set_balance(102, "SOL", Decimal::from(10), Decimal::ZERO);
        assert!(engine
            .bench_submit_direct(order(2, 102, "sell", false))?
            .is_empty());
        let matches = engine.bench_submit_direct(order(1, 101, "buy", market))?;
        assert_eq!(matches.len(), 1);
        let executor = engine.executor.lock();
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
        let engine = HighPerformanceEngine::new_bench();
        engine.bench_set_balance(101, "USDT", Decimal::from(1000), Decimal::ZERO);
        engine.bench_set_balance(102, "SOL", Decimal::from(10), Decimal::ZERO);
        engine.bench_submit_direct(order(2, 102, "sell", false))?;
        // 주문장에는 매도 2개가 있지만 예약이 1개뿐인 손상 상태를 주입한다.
        engine.bench_set_balance(102, "SOL", Decimal::from(8), Decimal::ONE);
        let (sender, receiver) = crossbeam::channel::unbounded();
        let result = super::super::threads::process_submit_order(
            order(1, 101, "buy", market),
            None,
            Some(&sender),
            None,
            &engine.orderbooks,
            &engine.matcher,
            &engine.executor,
        );
        assert!(result.is_err(), "정산 실패를 처리 성공으로 반환했다");
        assert!(
            receiver.try_recv().is_err(),
            "실패 후 주문 상태·예약 해제 명령을 발행했다"
        );
        let executor = engine.executor.lock();
        let cache = executor.balance_cache();
        // 주문 접수 단계의 200 예약은 보존하되 체결 이체는 일어나지 않는다.
        assert_eq!(
            cache.get_balance(101, "USDT").unwrap().locked,
            Decimal::from(200)
        );
        assert!(cache.get_balance(102, "USDT").is_none());
        assert!(cache.get_balance(101, "SOL").is_none());
    }
    Ok(())
}
