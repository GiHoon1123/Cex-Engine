// =====================================================
// Kafka Producer - 이벤트 발행
// =====================================================
// 역할: Rust 엔진에서 발생하는 이벤트를 Kafka로 발행
// 
// 발행 이벤트:
// 1. 체결 이벤트 (trade-executed-{asset})
// 2. 취소 이벤트 (order-cancelled-{asset})
//
// 특징:
// - 비동기 발행 (논블로킹)
// - 엔진 성능에 영향 없음
// - 발행 실패해도 엔진 처리에는 영향 없음 (로깅만)
// =====================================================

use anyhow::{Result, Context};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;

/// Kafka Producer
/// 
/// 비동기로 Kafka 이벤트를 발행합니다.
/// 엔진 성능에 영향을 주지 않도록 논블로킹 방식으로 동작합니다.
#[derive(Clone)]
pub struct KafkaProducer {
    producer: Arc<FutureProducer>,
    bootstrap_servers: String,
}

impl KafkaProducer {
    /// 새 Kafka Producer 생성
    /// 
    /// # Arguments
    /// * `bootstrap_servers` - Kafka 브로커 주소 (예: "localhost:9092")
    /// 
    /// # Returns
    /// KafkaProducer 인스턴스
    pub fn new(bootstrap_servers: &str) -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .set("acks", "1") // 리더만 확인 (성능 우선)
            .set("retries", "3")
            .set("batch.size", "16384")
            .set("linger.ms", "10")
            .create()
            .context("Failed to create Kafka producer")?;

        Ok(Self {
            producer: Arc::new(producer),
            bootstrap_servers: bootstrap_servers.to_string(),
        })
    }

    /// 체결 이벤트 발행
    /// Publish trade executed event
    /// 
    /// # Arguments
    /// * `base_mint` - 기준 자산 (예: "SOL")
    /// * `event` - 체결 이벤트 데이터
    /// 
    /// # Returns
    /// 발행 성공 여부 (비동기, 논블로킹)
    /// 
    /// # 파티션 키
    /// - buy_order_id와 sell_order_id 모두 사용 가능
    /// - 같은 주문의 이벤트는 같은 파티션으로 보장
    pub async fn publish_trade_executed(
        &self,
        base_mint: &str,
        event: &crate::shared::kafka::events::TradeExecutedEvent,
    ) -> Result<()> {
        let topic = format!("order-events-{}", base_mint.to_lowercase());
        // 파티션 키: buy_order_id와 sell_order_id 모두 사용
        // 같은 주문의 이벤트는 같은 파티션으로 보장
        let partition_key = format!("{}", event.buy_order_id);
        self.publish(&topic, Some(&partition_key), event).await
    }

    /// 취소 이벤트 발행
    /// Publish order cancelled event
    /// 
    /// # Arguments
    /// * `base_mint` - 기준 자산 (예: "SOL")
    /// * `event` - 취소 이벤트 데이터
    /// 
    /// # Returns
    /// 발행 성공 여부 (비동기, 논블로킹)
    /// 
    /// # 파티션 키
    /// - order_id를 파티션 키로 사용
    /// - 같은 주문의 이벤트는 같은 파티션으로 보장
    pub async fn publish_order_cancelled(
        &self,
        base_mint: &str,
        event: &crate::shared::kafka::events::OrderCancelledEvent,
    ) -> Result<()> {
        let topic = format!("order-events-{}", base_mint.to_lowercase());
        // 파티션 키: order_id 사용
        // 같은 주문의 이벤트는 같은 파티션으로 보장
        let partition_key = format!("{}", event.order_id);
        self.publish(&topic, Some(&partition_key), event).await
    }

    /// Kafka 메시지 발행 (내부 메서드)
    /// 
    /// # Arguments
    /// * `topic` - 토픽 이름
    /// * `key` - 메시지 키 (옵션)
    /// * `value` - 메시지 값 (JSON 직렬화)
    /// 
    /// # Returns
    /// 발행 성공 여부
    async fn publish<T: Serialize>(
        &self,
        topic: &str,
        key: Option<&str>,
        value: &T,
    ) -> Result<()> {
        // JSON 직렬화
        let json = serde_json::to_string(value)
            .context("Failed to serialize event to JSON")?;

        // FutureRecord 생성
        let mut record = FutureRecord::to(topic);
        if let Some(k) = key {
            record = record.key(k);
        }
        record = record.payload(&json);

        // 비동기 발행 (논블로킹)
        // 타임아웃 100ms로 설정하여 빠르게 실패 처리
        match tokio::time::timeout(
            Duration::from_millis(100),
            self.producer.send(record, Duration::from_secs(0)),
        )
        .await
        {
            Ok(Ok((_partition, _offset))) => {
                // 성공
                Ok(())
            }
            Ok(Err((e, _message))) => {
                // Kafka 에러
                eprintln!("[KafkaProducer] Failed to send message to {}: {:?}", topic, e);
                Ok(()) // 에러를 무시하고 계속 진행 (엔진 성능에 영향 없음)
            }
            Err(_) => {
                // 타임아웃
                eprintln!("[KafkaProducer] Timeout sending message to {}", topic);
                Ok(()) // 타임아웃을 무시하고 계속 진행
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Serialize)]
    struct TestEvent {
        order_id: u64,
        user_id: u64,
    }

    #[tokio::test]
    #[ignore] // Kafka 서버 필요
    async fn test_publish_trade_executed() {
        let producer = KafkaProducer::new("localhost:9092").unwrap();
        let event = TestEvent {
            order_id: 1,
            user_id: 100,
        };
        let result = producer.publish_trade_executed("SOL", &event).await;
        assert!(result.is_ok());
    }
}
