// CEX domain state
// CEX 도메인 상태
use std::sync::Arc;
use crate::shared::database::Database;
use crate::domains::cex::services::{BalanceService, FeeService, OrderService};
use crate::domains::cex::engine::runtime::HighPerformanceEngine;

/// CEX domain state
/// CEX 도메인에서 필요한 서비스들을 포함하는 상태
#[derive(Clone)]
pub struct CexState {
    pub engine: Arc<tokio::sync::Mutex<HighPerformanceEngine>>,
    pub balance_service: BalanceService,
    pub fee_service: FeeService,
    pub order_service: OrderService,
    // trade_service와 position_service는 Java에서 조회 API를 제공하므로 삭제됨
}

impl CexState {
    /// Create CexState with database and engine
    /// CexState 생성 (데이터베이스 + 엔진 필요)
    /// 
    /// # Arguments
    /// * `db` - 데이터베이스 연결
    /// * `engine` - 체결 엔진 (구체 타입 직접 사용)
    pub fn new(db: Database, engine: Arc<tokio::sync::Mutex<HighPerformanceEngine>>) -> Self {
        Self {
            engine: engine.clone(),
            balance_service: BalanceService::new(db.clone(), engine.clone()),
            fee_service: FeeService::new(db.clone()),
            order_service: OrderService::new(db.clone(), engine.clone()),
        }
    }
}

     