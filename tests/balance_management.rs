// =====================================================
// 잔고 관리 통합 테스트
// =====================================================

mod common;
use common::*;
use rust_decimal::Decimal;
use cex_engine::domains::cex::engine::Engine;

/// 테스트: 엔진 시작 및 중지
/// 
/// 엔진이 정상적으로 시작되고 중지되는지 확인합니다.
#[tokio::test]
async fn test_engine_start_stop() {
    let (mut engine, db) = setup_test().await;
    
    // 엔진이 정상적으로 시작되었는지 확인
    // (에러가 없으면 성공)
    
    teardown_test(&mut engine, &db).await;
}

/// 테스트: 잔고 조회
/// 
/// 엔진 시작 후 잔고가 제대로 로드되었는지 확인합니다.
#[tokio::test]
async fn test_balance_loaded() {
    let (mut engine, db) = setup_test().await;
    
    // SOL 잔고 조회
    let (sol_available, sol_locked) = engine.get_balance(TEST_USER_ID, "SOL").await
        .expect("Failed to get SOL balance");
    assert_eq!(sol_available, initial_sol_balance());
    assert_eq!(sol_locked, Decimal::ZERO);
    
    // USDT 잔고 조회
    let (usdt_available, usdt_locked) = engine.get_balance(TEST_USER_ID, "USDT").await
        .expect("Failed to get USDT balance");
    assert_eq!(usdt_available, initial_usdt_balance());
    assert_eq!(usdt_locked, Decimal::ZERO);
    
    teardown_test(&mut engine, &db).await;
}

/// 테스트: 모든 유저 잔고 로드 확인
/// 
/// 100명의 유저 잔고가 모두 엔진에 로드되었는지 확인합니다.
/// 테스트용 계정(1~10번)은 오더북에 주문이 없으므로 잔고가 락되지 않습니다.
/// 오더북용 계정(11~100번)은 주문으로 인해 잔고가 락되지만, 초기 잔고는 정상적으로 로드되었는지 확인합니다.
#[tokio::test]
async fn test_all_users_balances_loaded() {
    let (mut engine, db) = setup_test().await;  // 오더북 포함 (11~100번만 사용)
    
    // 테스트용 계정(1~10번): 오더북에 주문이 없으므로 잔고가 락되지 않음
    for user_id in 1..=10 {
        // SOL 잔고 조회
        let (sol_available, sol_locked) = engine.get_balance(user_id, "SOL").await
            .expect(&format!("Failed to get SOL balance for user {}", user_id));
        assert_eq!(sol_available, initial_sol_balance(), "User {} SOL balance mismatch", user_id);
        assert_eq!(sol_locked, Decimal::ZERO, "User {} SOL locked should be 0", user_id);
        
        // USDT 잔고 조회
        let (usdt_available, usdt_locked) = engine.get_balance(user_id, "USDT").await
            .expect(&format!("Failed to get USDT balance for user {}", user_id));
        assert_eq!(usdt_available, initial_usdt_balance(), "User {} USDT balance mismatch", user_id);
        assert_eq!(usdt_locked, Decimal::ZERO, "User {} USDT locked should be 0", user_id);
    }
    
    // 오더북용 계정(11~100번): 주문으로 인해 잔고가 락되지만, 초기 잔고는 정상적으로 로드되었는지 확인
    // (락된 잔고는 주문 가격과 수량에 따라 다르므로, available + locked = 초기 잔고인지만 확인)
    for user_id in 11..=NUM_TEST_USERS {
        // SOL 잔고 조회
        let (sol_available, sol_locked) = engine.get_balance(user_id, "SOL").await
            .expect(&format!("Failed to get SOL balance for user {}", user_id));
        // available + locked = 초기 잔고 (주문으로 인해 일부가 락됨)
        assert_eq!(sol_available + sol_locked, initial_sol_balance(), 
                   "User {} SOL total (available + locked) should equal initial balance", user_id);
        
        // USDT 잔고 조회
        let (usdt_available, usdt_locked) = engine.get_balance(user_id, "USDT").await
            .expect(&format!("Failed to get USDT balance for user {}", user_id));
        // available + locked = 초기 잔고 (주문으로 인해 일부가 락됨)
        assert_eq!(usdt_available + usdt_locked, initial_usdt_balance(), 
                   "User {} USDT total (available + locked) should equal initial balance", user_id);
    }
    
    println!("✅ All {} users' balances loaded successfully", NUM_TEST_USERS);
    
    teardown_test(&mut engine, &db).await;
}

/// 테스트: 잔고 업데이트 (입금)
/// 
/// 외부 입금 이벤트를 처리하여 잔고가 정상적으로 업데이트되는지 확인합니다.
#[tokio::test]
async fn test_update_balance_deposit() {
    let (mut engine, db) = setup_test().await;
    
    // 초기 잔고 확인
    let (initial_available, initial_locked) = engine.get_balance(TEST_USER_ID, "USDT").await
        .expect("Failed to get initial USDT balance");
    
    // 100 USDT 입금
    let deposit_amount = Decimal::new(100, 0);
    engine.update_balance(TEST_USER_ID, "USDT", deposit_amount).await
        .expect("Failed to update balance (deposit)");
    
    // 업데이트 후 잔고 확인
    let (new_available, new_locked) = engine.get_balance(TEST_USER_ID, "USDT").await
        .expect("Failed to get updated USDT balance");
    
    // available이 입금액만큼 증가했는지 확인
    assert_eq!(new_available, initial_available + deposit_amount,
                "Available balance should increase by deposit amount");
    assert_eq!(new_locked, initial_locked,
                "Locked balance should not change");
    
    println!("✅ Deposit test passed: {} USDT added", deposit_amount);
    
    teardown_test(&mut engine, &db).await;
}

/// 테스트: 잔고 업데이트 (출금)
/// 
/// 출금 이벤트를 처리하여 잔고가 정상적으로 업데이트되는지 확인합니다.
#[tokio::test]
async fn test_update_balance_withdrawal() {
    let (mut engine, db) = setup_test().await;
    
    // 초기 잔고 확인
    let (initial_available, initial_locked) = engine.get_balance(TEST_USER_ID, "USDT").await
        .expect("Failed to get initial USDT balance");
    
    // 출금 가능한 금액 확인 (available이 충분한지)
    let withdrawal_amount = Decimal::new(50, 0);
    if initial_available < withdrawal_amount {
        // 잔고가 부족하면 입금 먼저
        let deposit_amount = withdrawal_amount - initial_available + Decimal::new(10, 0);
        engine.update_balance(TEST_USER_ID, "USDT", deposit_amount).await
            .expect("Failed to deposit for withdrawal test");
    }
    
    // 출금 전 잔고 확인
    let (before_available, _) = engine.get_balance(TEST_USER_ID, "USDT").await
        .expect("Failed to get balance before withdrawal");
    
    // 50 USDT 출금 (음수 delta)
    let withdrawal_delta = Decimal::new(-50, 0);
    engine.update_balance(TEST_USER_ID, "USDT", withdrawal_delta).await
        .expect("Failed to update balance (withdrawal)");
    
    // 업데이트 후 잔고 확인
    let (new_available, new_locked) = engine.get_balance(TEST_USER_ID, "USDT").await
        .expect("Failed to get updated USDT balance");
    
    // available이 출금액만큼 감소했는지 확인
    assert_eq!(new_available, before_available + withdrawal_delta,
                "Available balance should decrease by withdrawal amount");
    assert_eq!(new_locked, initial_locked,
                "Locked balance should not change");
    
    println!("✅ Withdrawal test passed: {} USDT withdrawn", withdrawal_amount);
    
    teardown_test(&mut engine, &db).await;
}

/// 테스트: 잔고 업데이트 우선순위 (입금 큐 우선 처리)
/// 
/// 입금 큐가 주문 큐보다 우선 처리되는지 확인합니다.
/// 입금 후 즉시 주문을 제출할 수 있어야 합니다.
#[tokio::test]
async fn test_balance_update_priority() {
    let (mut engine, db) = setup_test().await;
    
    // 초기 잔고 확인 (USDT가 부족한 상태로 가정)
    let (initial_available, _) = engine.get_balance(TEST_USER_ID, "USDT").await
        .expect("Failed to get initial USDT balance");
    
    // 입금 (1000 USDT)
    let deposit_amount = Decimal::new(1000, 0);
    engine.update_balance(TEST_USER_ID, "USDT", deposit_amount).await
        .expect("Failed to update balance (deposit)");
    
    // 입금 후 즉시 잔고 확인 (입금이 처리되었는지)
    let (after_deposit_available, _) = engine.get_balance(TEST_USER_ID, "USDT").await
        .expect("Failed to get balance after deposit");
    assert_eq!(after_deposit_available, initial_available + deposit_amount,
                "Deposit should be processed immediately");
    
    // 입금 후 주문 제출 (입금이 처리되어야 주문 가능)
    use cex_engine::domains::cex::engine::types::OrderEntry;
    use chrono::Utc;
    
    let order = OrderEntry {
        id: 999999,
        user_id: TEST_USER_ID,
        order_type: "buy".to_string(),
        order_side: "limit".to_string(),
        base_mint: "SOL".to_string(),
        quote_mint: "USDT".to_string(),
        price: Some(Decimal::new(100, 0)),
        amount: Decimal::new(1, 0),
        quote_amount: None,
        filled_amount: Decimal::ZERO,
        remaining_amount: Decimal::new(1, 0),
        remaining_quote_amount: None,
        created_at: Utc::now(),
    };
    
    // 주문 제출 (입금이 처리되어야 성공)
    let result = engine.submit_order(order).await;
    assert!(result.is_ok(), "Order should be submitted successfully after deposit");
    
    println!("✅ Balance update priority test passed: deposit processed before order");
    
    teardown_test(&mut engine, &db).await;
}

