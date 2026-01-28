use std::sync::Arc;
use crate::shared::database::Database;
use crate::domains::auth::services::state::AuthState;
use crate::domains::wallet::services::state::WalletState;
use crate::domains::swap::services::state::SwapState;
use crate::domains::cex::services::state::CexState;
use crate::domains::cex::engine::runtime::HighPerformanceEngine;
use crate::domains::auth::services::JwtService;
use anyhow::Result;
use tokio::sync::Mutex;

/// Application state (combines all domain states)
/// 애플리케이션 상태 (모든 도메인 상태를 조합)
/// 
/// 역할: NestJS의 Module에서 모든 Service를 주입하는 것과 유사
/// 각 도메인의 State를 조합하여 전체 애플리케이션 상태를 관리
#[derive(Clone)]
pub struct AppState {
    /// 데이터베이스 연결 (공유)
    /// Database connection (shared)
    pub db: Database,
    pub auth_state: AuthState,
    pub wallet_state: WalletState,
    pub swap_state: SwapState,
    pub cex_state: CexState,
    /// 엔진 인스턴스 (시작/정지용)
    /// Engine instance (for start/stop)
    pub engine: Arc<Mutex<HighPerformanceEngine>>,
}

impl AppState {
    /// Create AppState with database
    /// 모든 도메인 State를 초기화하고 조합
    pub fn new(db: Database) -> Result<Self> {
        // 1. 공유 서비스 생성 (JWT 등)
        // JWT_SECRET 하드코딩 (배포 서버용)
        let jwt_secret = "cex-engine-jwt-secret-key-2024-production".to_string();
        let jwt_service = JwtService::new(jwt_secret);

        // 2. 각 도메인 State 생성
        let auth_state = AuthState::new(db.clone(), jwt_service);
        let wallet_state = WalletState::new(db.clone())?;
        let swap_state = SwapState::new(db.clone());
        
        // CEX 엔진 생성 (HighPerformanceEngine 사용)
        // 하나의 인스턴스만 생성하고 모든 곳에서 공유합니다.
        let engine_instance = HighPerformanceEngine::new(db.clone());
        let engine = Arc::new(Mutex::new(engine_instance));
        
        // 서비스에도 같은 엔진 인스턴스 전달 (Wrapper 불필요)
        let cex_state = CexState::new(db.clone(), engine.clone());
        
        // 3. AppState 조합
        Ok(Self {
            db: db.clone(),
            auth_state,
            wallet_state,
            swap_state,
            cex_state,
            engine,
        })
    }
}
