use crate::shared::database::Database;
use crate::domains::cex::services::OrderService;
use crate::domains::cex::engine::runtime::EngineHandle;

/// CEX domain state
/// CEX 도메인에서 필요한 서비스들을 포함하는 상태
#[derive(Clone)]
pub struct CexState {
    pub order_service: OrderService,
}

impl CexState {
    /// 주문 원본 DB와 단일 코어 명령 핸들만 HTTP 상태에 연결한다.
    pub fn new(db: Database, engine: EngineHandle) -> Self {
        Self {
            order_service: OrderService::new(db, engine),
        }
    }
}


