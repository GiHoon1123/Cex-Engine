use std::sync::Arc;
use crate::shared::database::{Database, UserBalanceRepository};
use crate::domains::cex::models::balance::{UserBalance, UserBalanceCreate};
use crate::domains::cex::engine::{Engine, runtime::HighPerformanceEngine};
use anyhow::{Context, Result};
use rust_decimal::Decimal;
use chrono::Utc;

/// 거래소 잔고 서비스
/// Exchange Balance Service
/// 
/// 역할:
/// - 사용자의 거래소 잔고 조회 및 관리
/// - 잔고 초기화 (입금 시 사용)
/// - 잔고 조회 (API에서 사용)
/// 
/// 변경사항:
/// - 잔고 조회는 엔진 메모리에서 실시간 조회 (실시간 정확성 보장)
/// - DB는 백업/복구용 및 메타데이터(id, created_at, updated_at) 저장용
#[derive(Clone)]
pub struct BalanceService {
    db: Database,
    engine: Arc<tokio::sync::Mutex<HighPerformanceEngine>>,
}

impl BalanceService {
    /// 생성자
    /// Constructor
    /// 
    /// # Arguments
    /// * `db` - 데이터베이스 연결
    /// * `engine` - 체결 엔진 (메모리 잔고 조회용)
    /// 
    /// # Returns
    /// BalanceService 인스턴스
    pub fn new(db: Database, engine: Arc<tokio::sync::Mutex<HighPerformanceEngine>>) -> Self {
        Self { db, engine }
    }


    /// 잔고 초기화 또는 생성
    /// Initialize or create balance for user
    /// 
    /// 주의: 이미 잔고가 있으면 업데이트하지 않고 기존 잔고 반환
    /// Note: If balance already exists, returns existing balance without updating
    /// 
    /// # Arguments
    /// * `user_id` - 사용자 ID
    /// * `mint_address` - 자산 식별자
    /// * `initial_available` - 초기 사용 가능 잔고 (기본값: 0)
    /// 
    /// # Returns
    /// * `Ok(UserBalance)` - 생성 또는 조회된 잔고
    /// * `Err` - 데이터베이스 오류 시
    /// 
    /// # Use Cases
    /// - 입금 시 잔고 레코드 초기화
    /// - 새로운 자산 거래 시작 시 잔고 생성
    pub async fn init_balance(
        &self,
        user_id: u64,
        mint_address: &str,
        initial_available: Decimal,
    ) -> Result<UserBalance> {
        let balance_repo = UserBalanceRepository::new(self.db.pool().clone());

        // 잔고 생성 또는 기존 잔고 조회
        // create_or_get: 이미 있으면 기존 것 반환, 없으면 새로 생성
        // create_or_get: returns existing if exists, creates new if not
        let balance_create = UserBalanceCreate {
            user_id,
            mint_address: mint_address.to_string(),
            available: initial_available,
            locked: Decimal::ZERO, // 초기에는 잠긴 잔고 없음
        };

        let balance = balance_repo
            .create_or_get(&balance_create)
            .await
            .context(format!(
                "Failed to initialize balance for user {} and asset {}",
                user_id, mint_address
            ))?;

        Ok(balance)
    }

    /// 잔고 충분 여부 확인
    /// Check if user has sufficient balance
    /// 
    /// # Arguments
    /// * `user_id` - 사용자 ID
    /// * `mint_address` - 자산 식별자
    /// * `required` - 필요한 수량
    /// 
    /// # Returns
    /// * `Ok(true)` - 잔고가 충분함
    /// * `Ok(false)` - 잔고가 부족함 또는 잔고가 없음
    /// * `Err` - 데이터베이스 오류 시
    pub async fn check_sufficient_balance(
        &self,
        user_id: u64,
        mint_address: &str,
        required: Decimal,
    ) -> Result<bool> {
        let balance_repo = UserBalanceRepository::new(self.db.pool().clone());

        // Repository에서 충분 여부 확인
        // Check sufficiency from repository
        let sufficient = balance_repo
            .check_sufficient_balance(user_id, mint_address, required)
            .await
            .context(format!(
                "Failed to check balance sufficiency for user {} and asset {}",
                user_id, mint_address
            ))?;

        Ok(sufficient)
    }
}