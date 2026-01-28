/// Kafka 모듈
/// Kafka Module
/// 
/// 역할:
/// - Kafka Producer 구현
/// - 이벤트 발행 (체결, 취소 등)
pub mod producer;
pub mod events;

pub use producer::KafkaProducer;
pub use events::{TradeExecutedEvent, OrderCancelledEvent};
