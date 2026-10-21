//! 금융 결과·시세 이벤트와 출력 발행을 묶는다. 코어는 이 모듈의 네트워크를 호출하지 않는다.
pub mod events;
pub mod producer;

pub use events::{OrderCancelledEvent, TradeExecutedEvent};
pub use producer::KafkaProducer;
pub(crate) use events::{MarketOrderCompletedEvent, OrderRejectedEvent};
