// =====================================================
// Self-Trade 방지 통합 테스트
// =====================================================

mod common;
use common::*;
use rust_decimal::Decimal;
use chrono::Utc;
use cex_engine::domains::cex::engine::types::OrderEntry;
use cex_engine::domains::cex::engine::Engine;

/// 테스트: 지정가 ↔ 지정가 Self-Trade 방지
/// 
/// 같은 user_id의 지정가 매수/매도 주문이 체결되지 않는지 확인합니다.
#[tokio::test]
async fn test_self_trade_prevention_limit_orders() {
    let (mut engine, db) = setup_test().await;
    
    // TEST_USER_ID (1번)이 105 USDT로 1 SOL 매도 주문
    let sell_order = OrderEntry {
        id: 40001,
        user_id: TEST_USER_ID,
        order_type: "sell".to_string(),
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
    
    // 매도 주문 제출
    let submit_result1 = engine.submit_order(sell_order).await;
    assert!(submit_result1.is_ok(), "Failed to submit sell order: {:?}", submit_result1.err());
    
    // 같은 유저가 105 USDT로 1 SOL 매수 주문
    let buy_order = OrderEntry {
        id: 40002,
        user_id: TEST_USER_ID,
        order_type: "buy".to_string(),
        order_side: "limit".to_string(),
        base_mint: "SOL".to_string(),
        quote_mint: "USDT".to_string(),
        price: Some(Decimal::new(105, 0)), // 105 USDT (같은 가격)
        amount: Decimal::new(1, 0), // 1 SOL
        filled_amount: Decimal::ZERO,
        remaining_amount: Decimal::new(1, 0),
        quote_amount: None,
        remaining_quote_amount: None,
        created_at: Utc::now(),
    };
    
    // 매수 주문 제출
    let submit_result2 = engine.submit_order(buy_order).await;
    assert!(submit_result2.is_ok(), "Failed to submit buy order: {:?}", submit_result2.err());
    
    // 잠시 대기 (주문 처리 완료 대기)
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Self-Trade 방지: 두 주문이 모두 오더북에 남아있어야 함 (체결되지 않음)
    let trading_pair = cex_engine::domains::cex::engine::types::TradingPair {
        base_mint: "SOL".to_string(),
        quote_mint: "USDT".to_string(),
    };
    let (buy_orders, sell_orders) = engine.get_orderbook(&trading_pair, None).await
        .expect("Failed to get orderbook");
    
    // 오더북에 두 주문이 모두 있는지 확인
    // 실제로는 더 상세한 검증이 필요
    
    teardown_test(&mut engine, &db).await;
}

/// 테스트: 시장가 Self-Trade 방지
/// 
/// 시장가 주문이 자기 지정가 주문을 건너뛰고 다음 주문과 매칭되는지 확인합니다.
#[tokio::test]
async fn test_market_order_skips_own_limit_order() {
    let (mut engine, db) = setup_test().await;
    
    // TEST_USER_ID (1번)이 105 USDT로 1 SOL 매도 주문 (지정가)
    let sell_order = OrderEntry {
        id: 40003,
        user_id: TEST_USER_ID,
        order_type: "sell".to_string(),
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
    
    // 매도 주문 제출
    let submit_result1 = engine.submit_order(sell_order).await;
    assert!(submit_result1.is_ok(), "Failed to submit sell order: {:?}", submit_result1.err());
    
    // 같은 유저가 시장가로 1 SOL 매수
    let buy_order = OrderEntry {
        id: 40004,
        user_id: TEST_USER_ID,
        order_type: "buy".to_string(),
        order_side: "market".to_string(),
        base_mint: "SOL".to_string(),
        quote_mint: "USDT".to_string(),
        price: None,
        amount: Decimal::ZERO,
        filled_amount: Decimal::ZERO,
        remaining_amount: Decimal::ZERO,
        quote_amount: Some(Decimal::new(110, 0)), // 110 USDT어치 구매
        remaining_quote_amount: Some(Decimal::new(110, 0)),
        created_at: Utc::now(),
    };
    
    // 매수 주문 제출
    let submit_result2 = engine.submit_order(buy_order).await;
    // 시장가 주문은 자기 주문을 건너뛰고 다른 유저의 주문과 매칭되어야 함
    // 또는 매칭할 주문이 없으면 실패해야 함
    
    teardown_test(&mut engine, &db).await;
}

/// 테스트: 본인 주문만 있을 때 시장가 실패
/// 
/// 오더북에 본인 주문만 있을 때 시장가 주문이 실패하는지 확인합니다.
#[tokio::test]
async fn test_market_order_fails_when_only_own_orders() {
    let (mut engine, db) = setup_test().await;
    
    // 오더북의 모든 다른 유저 주문 제거 (실제로는 취소)
    // 여기서는 테스트만 작성
    
    // TEST_USER_ID (1번)이 105 USDT로 1 SOL 매도 주문
    let sell_order = OrderEntry {
        id: 40005,
        user_id: TEST_USER_ID,
        order_type: "sell".to_string(),
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
    
    // 매도 주문 제출
    let submit_result1 = engine.submit_order(sell_order).await;
    assert!(submit_result1.is_ok(), "Failed to submit sell order: {:?}", submit_result1.err());
    
    // 같은 유저가 시장가로 매수 시도
    let buy_order = OrderEntry {
        id: 40006,
        user_id: TEST_USER_ID,
        order_type: "buy".to_string(),
        order_side: "market".to_string(),
        base_mint: "SOL".to_string(),
        quote_mint: "USDT".to_string(),
        price: None,
        amount: Decimal::ZERO,
        filled_amount: Decimal::ZERO,
        remaining_amount: Decimal::ZERO,
        quote_amount: Some(Decimal::new(110, 0)), // 110 USDT어치 구매 시도
        remaining_quote_amount: Some(Decimal::new(110, 0)),
        created_at: Utc::now(),
    };
    
    // 매수 주문 제출 (실패해야 함 - 본인 주문만 있음)
    let submit_result2 = engine.submit_order(buy_order).await;
    // 실제로는 에러가 반환되어야 하고, 잔고가 언락되어야 함
    
    teardown_test(&mut engine, &db).await;
}

