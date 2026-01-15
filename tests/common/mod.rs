// =====================================================
// 통합 테스트 공통 헬퍼
// =====================================================
// 목적: 모든 통합 테스트에서 공통으로 사용하는 셋업/티어다운 함수 제공
// 
// 사용법:
// ```rust
// mod common;
// use common::*;
// 
// #[tokio::test]
// async fn test_something() {
//     let (mut engine, db) = setup_test().await;
//     // 테스트 코드...
//     teardown_test(&mut engine, &db).await;
// }
// ```
// =====================================================

use rust_decimal::Decimal;
use cex_engine::shared::database::Database;
use cex_engine::domains::cex::engine::runtime::HighPerformanceEngine;
use cex_engine::domains::cex::engine::types::OrderEntry;
use cex_engine::domains::cex::engine::Engine;

// 테스트용 상수
pub const TEST_DATABASE_URL: &str = "postgresql://root:1234@localhost/cex_test";
pub const TEST_USER_ID: u64 = 1;  // 실제 테스트에 사용할 유저 ID
pub const NUM_TEST_USERS: u64 = 100;  // 오더북 채우기용 유저 수

// 초기 잔고 (함수로 생성)
pub fn initial_sol_balance() -> Decimal {
    Decimal::new(10000, 0)  // 10,000 SOL
}

pub fn initial_usdt_balance() -> Decimal {
    Decimal::new(10000000, 0)  // 10,000,000 USDT
}

/// 테스트 전 초기화
/// 
/// 데이터베이스 연결, 마이그레이션, 초기 잔고 설정을 순차적으로 수행합니다.
/// 
/// # Arguments
/// * `with_orderbook` - 오더북을 미리 채울지 여부 (기본값: true)
pub async fn setup_test() -> (HighPerformanceEngine, Database) {
    setup_test_with_orderbook(true).await
}

/// 테스트 전 초기화 (오더북 옵션 포함)
/// 
/// 데이터베이스 연결, 마이그레이션, 초기 잔고 설정을 순차적으로 수행합니다.
/// 
/// # Arguments
/// * `with_orderbook` - 오더북을 미리 채울지 여부
pub async fn setup_test_with_orderbook(with_orderbook: bool) -> (HighPerformanceEngine, Database) {
    // 1. 데이터베이스 연결
    let db = Database::new(TEST_DATABASE_URL)
        .await
        .expect("Failed to connect to database");
    
    // 2. 마이그레이션 실행
    db.initialize()
        .await
        .expect("Failed to initialize database");
    
    // 3. 테스트 데이터 정리
    cleanup_test_data(&db).await;
    
    // 4. 초기 잔고 설정
    setup_test_balances(&db).await;
    
    // 5. 트랜잭션 커밋이 완전히 반영되도록 짧은 대기
    // (PostgreSQL의 경우 트랜잭션 커밋 후 즉시 반영되지만, 
    //  연결 풀이나 비동기 처리로 인한 타이밍 이슈 방지)
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // 6. 엔진 생성 및 시작
    let mut engine = HighPerformanceEngine::new(db.clone());
    engine.start().await.expect("Failed to start engine");
    
    // 엔진 스레드가 완전히 시작되고 잔고가 로드될 때까지 대기
    // (엔진 스레드는 백그라운드에서 시작되므로, 잠시 대기하여 완전히 초기화되도록 함)
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    // 7. 오더북 쌓기 (옵션)
    if with_orderbook {
        setup_orderbook(&mut engine).await;
    }
    
    (engine, db)
}

/// 테스트 후 정리
/// 
/// 엔진을 중지하고 테스트 데이터를 정리합니다.
pub async fn teardown_test(engine: &mut HighPerformanceEngine, db: &Database) {
    // 엔진 중지
    engine.stop().await.expect("Failed to stop engine");
    
    // 테스트 데이터 정리
    cleanup_test_data(db).await;
}

/// 테스트 데이터 정리
/// 
/// 이전 테스트에서 남은 데이터를 삭제합니다.
pub async fn cleanup_test_data(db: &Database) {
    use sqlx::query;
    
    let pool = db.pool();
    let mut tx = pool.begin().await.unwrap();
    
    query("DELETE FROM trades").execute(&mut *tx).await.unwrap();
    query("DELETE FROM orders").execute(&mut *tx).await.unwrap();
    query("DELETE FROM user_balances").execute(&mut *tx).await.unwrap();
    
    tx.commit().await.unwrap();
}

/// 테스트용 잔고 설정
/// 
/// 여러 테스트 사용자를 생성하고 초기 잔고를 설정합니다.
/// - 유저 100명 생성
/// - 각 유저에게 SOL, USDT 잔고 부여
/// - 실제 테스트는 TEST_USER_ID (1번 유저)만 사용
pub async fn setup_test_balances(db: &Database) {
    use sqlx::query;
    
    let pool = db.pool();
    let mut tx = pool.begin().await.unwrap();
    
    // 유저 100명 생성 및 잔고 설정
    for user_id in 1..=NUM_TEST_USERS {
        // 테스트 사용자 생성 (없으면 생성)
        query(
            r#"
            INSERT INTO users (id, email, password_hash, created_at, updated_at)
            VALUES ($1, $2, $3, NOW(), NOW())
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(user_id as i64)
        .bind(format!("test_user_{}@example.com", user_id))
        .bind("dummy_hash")
        .execute(&mut *tx)
        .await
        .unwrap();
        
        // SOL 잔고
        query(
            r#"
            INSERT INTO user_balances (user_id, mint_address, available, locked, created_at, updated_at)
            VALUES ($1, $2, $3, $4, NOW(), NOW())
            ON CONFLICT (user_id, mint_address) DO UPDATE
            SET available = $3, locked = $4, updated_at = NOW()
            "#,
        )
        .bind(user_id as i64)
        .bind("SOL")
        .bind(initial_sol_balance())
        .bind(Decimal::ZERO)
        .execute(&mut *tx)
        .await
        .unwrap();
        
        // USDT 잔고
        query(
            r#"
            INSERT INTO user_balances (user_id, mint_address, available, locked, created_at, updated_at)
            VALUES ($1, $2, $3, $4, NOW(), NOW())
            ON CONFLICT (user_id, mint_address) DO UPDATE
            SET available = $3, locked = $4, updated_at = NOW()
            "#,
        )
        .bind(user_id as i64)
        .bind("USDT")
        .bind(initial_usdt_balance())
        .bind(Decimal::ZERO)
        .execute(&mut *tx)
        .await
        .unwrap();
    }
    
    tx.commit().await.unwrap();
}

/// 오더북 쌓기 (사전 셋업)
/// 
/// 테스트를 위해 오더북에 주문들을 미리 채워놓습니다.
/// 거래가 일어나지 않도록 매수 주문과 매도 주문의 가격을 겹치지 않게 설정합니다.
/// 
/// 전략:
/// - 유저 1~10번: 테스트용 (오더북에 주문 없음, 잔고 락 안됨)
/// - 유저 11~60번: 매수 주문 (가격 90~100 USDT, 매수 가격 < 매도 가격)
/// - 유저 61~100번: 매도 주문 (가격 101~110 USDT, 매도 가격 > 매수 가격)
/// 
/// 이렇게 하면 매칭되지 않고 오더북에만 쌓이며, 테스트용 계정(1~10번)은 잔고가 락되지 않습니다.
pub async fn setup_orderbook(engine: &mut HighPerformanceEngine) {
    use chrono::Utc;
    
    // 매수 주문: 유저 11~60번 (가격 90~100 USDT)
    // 가격이 낮을수록 매수 의향이 높음 (높은 가격에 사고 싶지 않음)
    // 50명의 유저를 11개 가격 레벨(90~100)에 분배
    for i in 11..=60 {
        let user_id = i as u64;
        // 90부터 100까지 균등 분배 (11개 레벨)
        let price_level = (i - 11) % 11; // 0~10
        let price = Decimal::new(90 + price_level, 0); // 90, 91, 92... 100
        let amount = Decimal::new(100, 0); // 100 SOL
        
        let order = OrderEntry {
            id: user_id * 1000, // 고유 ID 생성
            user_id,
            order_type: "buy".to_string(),
            order_side: "limit".to_string(),
            base_mint: "SOL".to_string(),
            quote_mint: "USDT".to_string(),
            price: Some(price),
            amount,
            filled_amount: Decimal::ZERO,
            remaining_amount: amount,
            quote_amount: None,
            remaining_quote_amount: None,
            created_at: Utc::now(),
        };
        
        engine.submit_order(order).await
            .expect(&format!("Failed to submit buy order for user {}", user_id));
    }
    
    // 매도 주문: 유저 61~100번 (가격 101~110 USDT)
    // 가격이 높을수록 매도 의향이 높음 (낮은 가격에 팔고 싶지 않음)
    // 40명의 유저를 10개 가격 레벨(101~110)에 분배
    for i in 61..=100 {
        let user_id = i as u64;
        // 101부터 110까지 균등 분배 (10개 레벨)
        let price_level = (i - 61) % 10; // 0~9
        let price = Decimal::new(101 + price_level, 0); // 101, 102, 103... 110
        let amount = Decimal::new(100, 0); // 100 SOL
        
        let order = OrderEntry {
            id: user_id * 1000, // 고유 ID 생성
            user_id,
            order_type: "sell".to_string(),
            order_side: "limit".to_string(),
            base_mint: "SOL".to_string(),
            quote_mint: "USDT".to_string(),
            price: Some(price),
            amount,
            filled_amount: Decimal::ZERO,
            remaining_amount: amount,
            quote_amount: None,
            remaining_quote_amount: None,
            created_at: Utc::now(),
        };
        
        engine.submit_order(order).await
            .expect(&format!("Failed to submit sell order for user {}", user_id));
    }
    
    println!("✅ Orderbook populated: {} buy orders (90-100 USDT) + {} sell orders (101-110 USDT)", 
             50, 40);
}

