// =====================================================
// Kafka Producer - 이벤트 발행
// =====================================================
// 역할: Rust 엔진에서 발생하는 이벤트를 Kafka로 발행
//
// 발행 이벤트:
// 1. 체결 이벤트 (trade_executed)
// 2. 취소 이벤트 (order_cancelled)
//
// 토픽 구조:
// - 금융 결과: order-events, 공개 호가·체결: market-events
// - 초기 구성은 토픽마다 파티션 1개
// - 동일 엔진의 체결·취소는 같은 엔진 식별자를 키로 사용
// - 한 코어의 결과는 같은 파티션에서 순서대로 소비
// - 정산용: trade_executed, order_cancelled만 발행
//
// 특징:
// - 비동기 발행 (논블로킹)
// - 출력 워커에서 완료를 기다림
// - 발행 실패를 오류로 반환해 신규 처리를 차단
// =====================================================

use anyhow::{Context, Result};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Serialize;
use std::time::Duration;

/// Kafka Producer
///
/// 비동기로 Kafka 이벤트를 발행합니다.
/// 완료 대기는 전용 출력 워커에서 수행한다.
#[derive(Clone)]
pub struct KafkaProducer {
    producer: FutureProducer,
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
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("retries", "3")
            .set("batch.size", "16384")
            .set("linger.ms", "10")
            .create()
            .context("Failed to create Kafka producer")?;

        Ok(Self { producer })
    }

    /// 체결 이벤트 발행
    /// Publish trade executed event
    ///
    /// # Arguments
    /// * `event` - 체결 이벤트 데이터
    ///
    /// # Returns
    /// 발행 성공 여부 (비동기, 논블로킹)
    ///
    /// # 파티션 키
    /// - 저널의 엔진 식별자를 키로 사용해 체결과 취소의 상대 순서를 유지한다.
    pub async fn publish_trade_executed(
        &self,
        event: &crate::shared::kafka::events::TradeExecutedEvent,
    ) -> Result<()> {
        eprintln!("[KafkaProducer] trade_executed 이벤트 발행 시작: buy_order_id={}, sell_order_id={}, buyer_id={}, seller_id={}", 
                 event.buy_order_id, event.sell_order_id, event.buyer_id, event.seller_id);
        let topic = "order-events";
        // 같은 코어가 확정한 모든 결과는 동일한 키를 사용한다.
        let partition_key = event.engine_id.clone();
        self.publish(topic, Some(&partition_key), event).await
    }

    /// 취소 이벤트 발행
    /// Publish order cancelled event
    ///
    /// # Arguments
    /// * `event` - 취소 이벤트 데이터
    ///
    /// # Returns
    /// 발행 성공 여부 (비동기, 논블로킹)
    ///
    /// # 파티션 키
    /// - 체결과 동일한 엔진 식별자를 키로 사용한다.
    pub async fn publish_order_cancelled(
        &self,
        event: &crate::shared::kafka::events::OrderCancelledEvent,
    ) -> Result<()> {
        let topic = "order-events";
        // 주문 소유자가 달라도 같은 엔진 결과의 순서는 바꾸지 않는다.
        let partition_key = event.engine_id.clone();
        self.publish(topic, Some(&partition_key), event).await
    }

    /// 호가는 금융 정산 토픽과 분리한다. 같은 엔진의 공개 순서는 한 파티션으로
    /// 고정하며, 브로커 확인 전에는 해당 저널 위치의 전달을 완료하지 않는다.
    pub(crate) async fn publish_market_snapshot(
        &self,
        event: &crate::domains::cex::engine::runtime::market::MarketSnapshot,
    ) -> Result<()> {
        self.publish("market-events", Some(&event.engine_id), event).await
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
    async fn publish<T: Serialize>(&self, topic: &str, key: Option<&str>, value: &T) -> Result<()> {
        // JSON 직렬화
        let json = serde_json::to_string(value).context("Failed to serialize event to JSON")?;

        // FutureRecord 생성
        let mut record = FutureRecord::to(topic);
        if let Some(k) = key {
            record = record.key(k);
        }
        record = record.payload(&json);

        // 출력 워커가 브로커의 완료 확인을 기다린다. 짧은 별도 시간 제한으로
        // 확인 대기를 끊지 않고 클라이언트의 전달 제한 시간을 따른다.
        // 오류는 성공으로 숨기지 않는다. 재시작 재발행은 동일 저널 식별자를
        // 사용하며 플랫폼의 영속 수신 키로 중복 금융 반영을 막는다.
        self.producer
            .send(record, Duration::ZERO)
            .await
            .map(|_| ())
            .map_err(|(error, _)| anyhow::anyhow!("Kafka delivery failed: {}", error))
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
    async fn output_kafka_delivery_failure_is_not_reported_as_success() {
        // 사용하지 않는 루프백 포트로 실제 클라이언트의 전달 실패를 만든다.
        // 기존 브로커나 업무 토픽에는 접근하지 않는다.
        let producer = KafkaProducer::new("127.0.0.1:1").unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(8),
            producer.publish(
                "isolated-output-failure",
                None,
                &TestEvent {
                    order_id: 1,
                    user_id: 1,
                },
            ),
        )
        .await;
        assert!(result
            .expect("client delivery deadline did not finish")
            .is_err());
    }

    #[tokio::test]
    #[ignore] // Kafka 서버 필요
    async fn test_publish_trade_executed() {
        use crate::shared::kafka::events::TradeExecutedEvent;
        use chrono::Utc;
        use rust_decimal::Decimal;

        let producer = KafkaProducer::new("localhost:9092").unwrap();
        let event = TradeExecutedEvent {
            event_id: "example:1:0".into(),
            engine_id: "example".into(),
            event_type: "trade_executed".to_string(),
            buy_order_id: 100,
            sell_order_id: 101,
            buyer_id: 1,
            seller_id: 2,
            price: Decimal::new(10000, 2),
            amount: Decimal::new(1, 0),
            base_mint: "SOL".to_string(),
            quote_mint: "USDT".to_string(),
            timestamp: Utc::now(),
            // 현재 체결 이벤트 스키마에는 잔고 스냅샷도 필수다.
            // 이 예제는 100 USDT로 1 SOL을 체결한 직후의 잔고를 표현한다.
            buyer_base_available: Decimal::ONE,
            buyer_base_locked: Decimal::ZERO,
            buyer_quote_available: Decimal::ZERO,
            buyer_quote_locked: Decimal::ZERO,
            seller_base_available: Decimal::ZERO,
            seller_base_locked: Decimal::ZERO,
            seller_quote_available: Decimal::new(100, 0),
            seller_quote_locked: Decimal::ZERO,
        };
        let result = producer.publish_trade_executed(&event).await;
        assert!(result.is_ok());
    }
}
