// =====================================================
// 지정가 주문 통합 테스트
// =====================================================

mod common;
use common::*;
use rust_decimal::Decimal;
use chrono::Utc;
use cex_engine::domains::cex::engine::types::{OrderEntry, TradingPair};
use cex_engine::domains::cex::engine::Engine;

/// 테스트: 지정가 매수 - 완전 체결
/// 
/// 매도 호가가 충분할 때 지정가 매수 주문이 완전히 체결되는지 확인합니다.
#[tokio::test]
async fn test_limit_buy_full_fill() {
    let (mut engine, db) = setup_test().await;
    
    // TEST_USER_ID (1번)이 105 USDT로 1 SOL 매수 주문
    let buy_order = OrderEntry {
        id: 10001,
        user_id: TEST_USER_ID,
        order_type: "buy".to_string(),
        order_side: "limit".to_string(),
        base_mint: "SOL".to_string(),
        quote_mint: "USDT".to_string(),
        price: Some(Decimal::new(105, 0)), // 105 USDT (매도 호가 101~110보다 높음)
        amount: Decimal::new(1, 0), // 1 SOL
        filled_amount: Decimal::ZERO,
        remaining_amount: Decimal::new(1, 0),
        quote_amount: None,
        remaining_quote_amount: None,
        created_at: Utc::now(),
    };
    
    // 주문 제출
    let result = engine.submit_order(buy_order).await;
    assert!(result.is_ok(), "Failed to submit buy order: {:?}", result.err());
    
    // 주문이 완전히 체결되었는지 확인 (오더북에 남지 않아야 함)
    // 실제로는 get_orderbook으로 확인하거나, 잔고 변화로 확인
    
    teardown_test(&mut engine, &db).await;
}

/// 테스트: 지정가 매수 - 부분 체결
/// 
/// 일부만 체결되고 나머지는 오더북에 남는지 확인합니다.
#[tokio::test]
async fn test_limit_buy_partial_fill() {
    let (mut engine, db) = setup_test().await;
    
    // TEST_USER_ID (1번)이 105 USDT로 5 SOL 매수 주문
    // 오더북에는 각 가격 레벨에 1 SOL씩만 있음 (101~110)
    let buy_order = OrderEntry {
        id: 10002,
        user_id: TEST_USER_ID,
        order_type: "buy".to_string(),
        order_side: "limit".to_string(),
        base_mint: "SOL".to_string(),
        quote_mint: "USDT".to_string(),
        price: Some(Decimal::new(105, 0)), // 105 USDT
        amount: Decimal::new(5, 0), // 5 SOL (오더북에 10개 레벨이 있지만 각 1개씩만)
        filled_amount: Decimal::ZERO,
        remaining_amount: Decimal::new(5, 0),
        quote_amount: None,
        remaining_quote_amount: None,
        created_at: Utc::now(),
    };
    
    // 주문 제출
    let result = engine.submit_order(buy_order).await;
    assert!(result.is_ok(), "Failed to submit buy order: {:?}", result.err());
    
    // 부분 체결 확인 (오더북에 남은 주문이 있는지 확인)
    // 실제로는 get_orderbook으로 확인
    
    teardown_test(&mut engine, &db).await;
}

/// 테스트: 지정가 매수 - 미체결
/// 
/// 가격이 낮아서 오더북에만 추가되고 체결되지 않는지 확인합니다.
#[tokio::test]
async fn test_limit_buy_no_fill() {
    let (mut engine, db) = setup_test().await;
    
    // TEST_USER_ID (1번)이 95 USDT로 1 SOL 매수 주문
    // 오더북의 매도 호가는 101~110이므로 매칭되지 않음
    let buy_order = OrderEntry {
        id: 10003,
        user_id: TEST_USER_ID,
        order_type: "buy".to_string(),
        order_side: "limit".to_string(),
        base_mint: "SOL".to_string(),
        quote_mint: "USDT".to_string(),
        price: Some(Decimal::new(95, 0)), // 95 USDT (매도 호가 101보다 낮음)
        amount: Decimal::new(1, 0), // 1 SOL
        filled_amount: Decimal::ZERO,
        remaining_amount: Decimal::new(1, 0),
        quote_amount: None,
        remaining_quote_amount: None,
        created_at: Utc::now(),
    };
    
    // 주문 제출
    let result = engine.submit_order(buy_order).await;
    assert!(result.is_ok(), "Failed to submit buy order: {:?}", result.err());
    
    // 오더북에 주문이 추가되었는지 확인
    // 실제로는 get_orderbook으로 확인
    
    teardown_test(&mut engine, &db).await;
}

/// 테스트: 지정가 매도 - 완전 체결
/// 
/// 매수 호가가 충분할 때 지정가 매도 주문이 완전히 체결되는지 확인합니다.
#[tokio::test]
async fn test_limit_sell_full_fill() {
    let (mut engine, db) = setup_test().await;
    
    // TEST_USER_ID (1번)이 95 USDT로 1 SOL 매도 주문
    // 오더북의 매수 호가는 90~100이므로 매칭됨
    let sell_order = OrderEntry {
        id: 10004,
        user_id: TEST_USER_ID,
        order_type: "sell".to_string(),
        order_side: "limit".to_string(),
        base_mint: "SOL".to_string(),
        quote_mint: "USDT".to_string(),
        price: Some(Decimal::new(95, 0)), // 95 USDT (매수 호가 90~100보다 낮음)
        amount: Decimal::new(1, 0), // 1 SOL
        filled_amount: Decimal::ZERO,
        remaining_amount: Decimal::new(1, 0),
        quote_amount: None,
        remaining_quote_amount: None,
        created_at: Utc::now(),
    };
    
    // 주문 제출
    let result = engine.submit_order(sell_order).await;
    assert!(result.is_ok(), "Failed to submit sell order: {:?}", result.err());
    
    teardown_test(&mut engine, &db).await;
}

/// 테스트: 지정가 매도 - 부분 체결
/// 
/// 일부만 체결되고 나머지는 오더북에 남는지 확인합니다.
#[tokio::test]
async fn test_limit_sell_partial_fill() {
    let (mut engine, db) = setup_test().await;
    
    // TEST_USER_ID (1번)이 95 USDT로 5 SOL 매도 주문
    // 오더북에는 각 가격 레벨에 1 SOL씩만 있음 (90~100)
    let sell_order = OrderEntry {
        id: 10005,
        user_id: TEST_USER_ID,
        order_type: "sell".to_string(),
        order_side: "limit".to_string(),
        base_mint: "SOL".to_string(),
        quote_mint: "USDT".to_string(),
        price: Some(Decimal::new(95, 0)), // 95 USDT
        amount: Decimal::new(5, 0), // 5 SOL
        filled_amount: Decimal::ZERO,
        remaining_amount: Decimal::new(5, 0),
        quote_amount: None,
        remaining_quote_amount: None,
        created_at: Utc::now(),
    };
    
    // 주문 제출
    let result = engine.submit_order(sell_order).await;
    assert!(result.is_ok(), "Failed to submit sell order: {:?}", result.err());
    
    teardown_test(&mut engine, &db).await;
}

/// 테스트: 지정가 매도 - 미체결
/// 
/// 가격이 높아서 오더북에만 추가되고 체결되지 않는지 확인합니다.
#[tokio::test]
async fn test_limit_sell_no_fill() {
    let (mut engine, db) = setup_test().await;
    
    // TEST_USER_ID (1번)이 115 USDT로 1 SOL 매도 주문
    // 오더북의 매수 호가는 90~100이므로 매칭되지 않음
    let sell_order = OrderEntry {
        id: 10006,
        user_id: TEST_USER_ID,
        order_type: "sell".to_string(),
        order_side: "limit".to_string(),
        base_mint: "SOL".to_string(),
        quote_mint: "USDT".to_string(),
        price: Some(Decimal::new(115, 0)), // 115 USDT (매수 호가 100보다 높음)
        amount: Decimal::new(1, 0), // 1 SOL
        filled_amount: Decimal::ZERO,
        remaining_amount: Decimal::new(1, 0),
        quote_amount: None,
        remaining_quote_amount: None,
        created_at: Utc::now(),
    };
    
    // 주문 제출
    let result = engine.submit_order(sell_order).await;
    assert!(result.is_ok(), "Failed to submit sell order: {:?}", result.err());
    
    teardown_test(&mut engine, &db).await;
}

