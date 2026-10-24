// 거래 명령과 영속 기록에 필요한 공통 모듈이다.
pub mod database;
pub mod services;
pub mod utils;
pub mod handlers;
pub mod kafka;

pub use database::*;
pub use services::*;
pub use utils::*;
pub use handlers::*;
pub use kafka::*;


// 서비스 신원은 HTTP 경계에서 검사한다. 체결 코어의 상태/락과 연결하지 않는다.
pub(crate) mod service_identity;
