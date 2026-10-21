//! 실제 단일 워커를 실행해 검증하는 회귀 테스트다.
//! 디비와 카프카를 사용하지 않는 동일한 명령 루프에서 소유권 이동과 잔액 결과를 확인한다.
use super::*;
use crate::domains::cex::engine::{runtime::bench_engine::BenchEngine, Engine};

fn order(id: u64, user_id: u64, buy: bool, amount: i64) -> OrderEntry {
    OrderEntry {
        id,
        user_id,
        order_type: if buy { "buy" } else { "sell" }.into(),
        order_side: "limit".into(),
        base_mint: "SOL".into(),
        quote_mint: "USDT".into(),
        price: Some(Decimal::from(100)),
        amount: Decimal::from(amount),
        quote_amount: None,
        filled_amount: Decimal::ZERO,
        remaining_amount: Decimal::from(amount),
        remaining_quote_amount: None,
        created_at: chrono::Utc::now(),
    }
}

#[tokio::test]
async fn ownership_worker_moves_state_and_matches_direct_core_results() -> Result<()> {
    let mut engine = HighPerformanceEngine::new_bench();
    let mut direct = BenchEngine::new();
    for (user, mint, amount) in [(1, "SOL", 3), (2, "SOL", 2), (3, "USDT", 1000)] {
        engine.bench_set_balance(user, mint, Decimal::from(amount), Decimal::ZERO);
        direct.set_balance(user, mint, Decimal::from(amount), Decimal::ZERO);
    }
    engine.start().await?;
    assert!(
        engine.state.is_none(),
        "controller retained mutable trading state"
    );
    let handle = engine.handle();
    // 같은 큐에 조회를 이어서 넣으면 앞선 주문 명령의 처리 결과를 관찰한다.
    // 큐 진입 성공과 체결 완료를 구분하고, 조회 결과로 상태를 비교한다.
    let checked = async {
        for command in [
            order(1, 1, false, 3),
            order(2, 2, false, 2),
            order(3, 3, true, 4),
        ] {
            direct.submit_direct(command.clone())?;
            handle.submit_order(command).await?;
        }
        let pair = TradingPair::new("SOL".into(), "USDT".into());
        let (bids, asks) = handle.get_orderbook(&pair, None).await?;
        assert!(bids.is_empty());
        assert_eq!(asks.len(), 1);
        assert_eq!((asks[0].id, asks[0].remaining_amount), (2, Decimal::ONE));
        for user in 1..=3 {
            for asset in ["SOL", "USDT"] {
                assert_eq!(
                    handle.get_balance(user, asset).await?,
                    direct.test_balance(user, asset)
                );
            }
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.stop().await?;
    checked?;
    assert!(
        handle.get_balance(3, "SOL").await.is_err(),
        "stopped controller left handle accepting commands"
    );
    Ok(())
}

#[tokio::test]
async fn ownership_worker_failure_preserves_orderbook_and_balance() -> Result<()> {
    let mut engine = HighPerformanceEngine::new_bench();
    engine.bench_set_balance(1, "USDT", Decimal::from(50), Decimal::ZERO);
    engine.start().await?;
    let handle = engine.handle();
    let checked = async {
        // 100이 필요한 주문을 잔액 50으로 제출한다. 전송 성공은 처리 성공이 아니다.
        handle.submit_order(order(1, 1, true, 1)).await?;
        let pair = TradingPair::new("SOL".into(), "USDT".into());
        let (bids, asks) = handle.get_orderbook(&pair, None).await?;
        assert!(bids.is_empty() && asks.is_empty());
        assert_eq!(
            handle.get_balance(1, "USDT").await?,
            (Decimal::from(50), Decimal::ZERO)
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.stop().await?;
    checked
}
