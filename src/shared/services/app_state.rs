use crate::domains::cex::engine::runtime::HighPerformanceEngine;
use crate::domains::cex::services::state::CexState;
use crate::domains::swap::services::state::SwapState;
use crate::domains::wallet::services::state::WalletState;
use crate::shared::database::Database;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;

/// HTTP 핸들러에서 공유하는 서비스와 엔진 인스턴스.
///
/// TypeScript 백엔드의 의존성 주입 컨테이너처럼, 필요한 서비스를 한 번 생성하고
/// 핸들러에 전달한다. 사용자 인증은 Java 플랫폼에서 담당하므로 인증 서비스나
/// JWT 서명 키를 이 상태에 보관하지 않는다.
#[derive(Clone)]
pub struct AppState {
    /// 데이터베이스 연결 (공유)
    /// Database connection (shared)
    pub db: Database,
    pub wallet_state: WalletState,
    pub swap_state: SwapState,
    pub cex_state: CexState,
    /// 엔진 인스턴스 (시작/정지용)
    /// Engine instance (for start/stop)
    pub engine: Arc<Mutex<HighPerformanceEngine>>,
}

impl AppState {
    /// 데이터베이스 풀과 엔진 한 개를 여러 서비스가 공유하도록 연결한다.
    ///
    /// `Database::clone()`은 연결 풀의 공유 핸들을 복제한다. `Arc::clone()`도
    /// 엔진 자체를 복제하지 않고 같은 인스턴스를 가리키는 참조 수를 늘린다.
    /// `Mutex`는 비동기 요청이 엔진의 시작·정지 등의 작업에 동시에 접근하지
    /// 못하도록 보호하며, 실제 주문 매칭은 엔진 내부 스레드가 담당한다.
    pub fn new(db: Database) -> Result<Self> {
        // 남아 있는 도메인 서비스만 초기화한다. `?`는 지갑 서비스 생성에 실패하면
        // 그 오류를 호출자에게 전달하며, 불완전한 AppState 생성을 중단한다.
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
            wallet_state,
            swap_state,
            cex_state,
            engine,
        })
    }
}
