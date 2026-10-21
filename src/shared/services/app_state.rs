use crate::domains::cex::engine::runtime::EngineHandle;
use crate::domains::cex::services::state::CexState;
use crate::domains::swap::services::state::SwapState;
use crate::domains::wallet::services::state::WalletState;
use crate::shared::database::Database;
use anyhow::Result;

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
    /// 요청용 명령 핸들. 시작·종료와 거래 상태 접근 권한은 없다.
    pub engine: EngineHandle,
}

impl AppState {
    /// 서버 main이 생명주기를 소유하고, 이 컨테이너는 명령 전송 핸들만 받는다.
    /// clone은 채널 송신 권한을 복제하며 주문장·잔액을 공유하지 않는다.
    pub fn new(db: Database, engine: EngineHandle) -> Result<Self> {
        // 남아 있는 도메인 서비스만 초기화한다. `?`는 지갑 서비스 생성에 실패하면
        // 그 오류를 호출자에게 전달하며, 불완전한 AppState 생성을 중단한다.
        let wallet_state = WalletState::new(db.clone())?;
        let swap_state = SwapState::new(db.clone());

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
