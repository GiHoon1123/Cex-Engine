use crate::domains::cex::engine::runtime::EngineHandle;
use crate::domains::cex::services::state::CexState;
use crate::shared::database::Database;

/// HTTP 경계의 거래 서비스와 명령 송신 핸들만 보관한다.
/// 오더북·실행 잔액은 단일 워커가 독점 소유하며 이 객체에 상태 참조/생명주기 권한을 주지 않는다.
#[derive(Clone)]
pub struct AppState {
    pub cex_state: CexState,
    pub engine: EngineHandle,
}
impl AppState {
    /// 플랫폼 주문 원본을 검증하는 DB 연결은 거래 서비스가 사용한다.
    /// 외부 지갑/스왑 클라이언트 초기화가 없어 조립 자체에는 실패할 작업이 없다.
    /// clone은 송신 핸들만 복제하며 새 체결 워커나 공유 상태 락을 만들지 않는다.
    pub fn new(db: Database, engine: EngineHandle) -> Self {
        Self {
            cex_state: CexState::new(db, engine.clone()),
            engine,
        }
    }
}
