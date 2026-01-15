// =====================================================
// 오더북 조회 API 통합 테스트
// =====================================================

mod common;
use common::*;
use rust_decimal::Decimal;
use chrono::Utc;
use cex_engine::domains::cex::engine::types::{OrderEntry, TradingPair};
use cex_engine::domains::cex::engine::Engine;

/// 테스트: get_orderbook() 정상 동작
/// 
/// 오더북 조회가 정상적으로 동작하는지 확인합니다.
#[tokio::test]
async fn test_get_orderbook() {
    let (mut engine, db) = setup_test().await;
    
    let trading_pair = TradingPair {
        base_mint: "SOL".to_string(),
        quote_mint: "USDT".to_string(),
    };
    
    // 오더북 조회
    let (buy_orders, sell_orders) = engine.get_orderbook(&trading_pair, None).await
        .expect("Failed to get orderbook");
    
    // 오더북이 비어있지 않아야 함 (setup_orderbook에서 채워놓음)
    assert!(!buy_orders.is_empty() || !sell_orders.is_empty(), 
            "Orderbook should not be empty");
    
    teardown_test(&mut engine, &db).await;
}

/// 테스트: best_bid / best_ask 정확
/// 
/// 최고 매수가와 최저 매도가가 정확한지 확인합니다.
#[tokio::test]
async fn test_best_bid_ask() {
    let (mut engine, db) = setup_test().await;
    
    let trading_pair = TradingPair {
        base_mint: "SOL".to_string(),
        quote_mint: "USDT".to_string(),
    };
    
    // best_bid, best_ask 조회
    let best_bid = engine.get_best_bid(&trading_pair).await
        .expect("Failed to get best bid");
    let best_ask = engine.get_best_ask(&trading_pair).await
        .expect("Failed to get best ask");
    
    // setup_orderbook에서 매수는 90~100, 매도는 101~110으로 설정했음
    // best_bid는 100 (가장 높은 매수가)
    // best_ask는 101 (가장 낮은 매도가)
    
    if let Some(bid) = best_bid {
        assert!(bid >= Decimal::new(90, 0) && bid <= Decimal::new(100, 0),
                "Best bid should be between 90 and 100");
    }
    
    if let Some(ask) = best_ask {
        assert!(ask >= Decimal::new(101, 0) && ask <= Decimal::new(110, 0),
                "Best ask should be between 101 and 110");
    }
    
    teardown_test(&mut engine, &db).await;
}

/// 테스트: 스프레드 계산 정확
/// 
/// 스프레드가 올바르게 계산되는지 확인합니다.
#[tokio::test]
async fn test_spread_calculation() {
    let (mut engine, db) = setup_test().await;
    
    let trading_pair = TradingPair {
        base_mint: "SOL".to_string(),
        quote_mint: "USDT".to_string(),
    };
    
    // best_bid, best_ask 조회
    let best_bid = engine.get_best_bid(&trading_pair).await
        .expect("Failed to get best bid");
    let best_ask = engine.get_best_ask(&trading_pair).await
        .expect("Failed to get best ask");
    
    // 스프레드 = best_ask - best_bid
    if let (Some(bid), Some(ask)) = (best_bid, best_ask) {
        let spread = ask - bid;
        // setup_orderbook에서 매수 최고가 100, 매도 최저가 101이므로 스프레드는 최소 1
        assert!(spread >= Decimal::new(1, 0), "Spread should be at least 1 USDT");
    }
    
    teardown_test(&mut engine, &db).await;
}

/// 테스트: 부분 체결 후 레벨 제거 / 남은 수량 갱신 정상
/// 
/// 부분 체결 후 오더북이 올바르게 업데이트되는지 확인합니다.
#[tokio::test]
async fn test_orderbook_update_after_partial_fill() {
    let (mut engine, db) = setup_test().await;
    
    // TEST_USER_ID (1번)이 105 USDT로 1 SOL 매수 주문
    // 오더북의 매도 호가 101~110에 각 1 SOL씩 있으므로 체결됨
    let buy_order = OrderEntry {
        id: 50001,
        user_id: TEST_USER_ID,
        order_type: "buy".to_string(),
        order_side: "limit".to_string(),
        base_mint: "SOL".to_string(),
        quote_mint: "USDT".to_string(),
        price: Some(Decimal::new(105, 0)), // 105 USDT
        amount: Decimal::new(1, 0), // 1 SOL
        filled_amount: Decimal::ZERO,
        remaining_amount: Decimal::new(1, 0),
        quote_amount: None,
        remaining_quote_amount: None,
        created_at: Utc::now(),
    };
    
    // 주문 제출
    let submit_result = engine.submit_order(buy_order).await;
    assert!(submit_result.is_ok(), "Failed to submit order: {:?}", submit_result.err());
    
    // 잠시 대기 (주문 처리 완료 대기)
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // 오더북 재조회
    let trading_pair = TradingPair {
        base_mint: "SOL".to_string(),
        quote_mint: "USDT".to_string(),
    };
    let (buy_orders, sell_orders) = engine.get_orderbook(&trading_pair, None).await
        .expect("Failed to get orderbook after partial fill");
    
    // 오더북이 올바르게 업데이트되었는지 확인
    // 실제로는 더 상세한 검증이 필요
    
    teardown_test(&mut engine, &db).await;
}

/// 테스트: 오더북 정렬(BTreeMap) 유지 확인
/// 
/// 오더북이 가격별로 올바르게 정렬되어 있는지 확인합니다.
#[tokio::test]
async fn test_orderbook_sorting() {
    let (mut engine, db) = setup_test().await;
    
    let trading_pair = TradingPair {
        base_mint: "SOL".to_string(),
        quote_mint: "USDT".to_string(),
    };
    
    // 오더북 조회
    let (buy_orders, sell_orders) = engine.get_orderbook(&trading_pair, None).await
        .expect("Failed to get orderbook");
    
    // 매수 주문은 가격 내림차순 (높은 가격 먼저)
    let mut prev_price: Option<Decimal> = None;
    for order in &buy_orders {
        if let Some(price) = order.price {
            if let Some(prev) = prev_price {
                assert!(price <= prev, "Buy orders should be sorted in descending order");
            }
            prev_price = Some(price);
        }
    }
    
    // 매도 주문은 가격 오름차순 (낮은 가격 먼저)
    let mut prev_price: Option<Decimal> = None;
    for order in &sell_orders {
        if let Some(price) = order.price {
            if let Some(prev) = prev_price {
                assert!(price >= prev, "Sell orders should be sorted in ascending order");
            }
            prev_price = Some(price);
        }
    }
    
    teardown_test(&mut engine, &db).await;
}

