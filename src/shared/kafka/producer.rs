// =====================================================
// Kafka 발행기 - 이벤트 발행
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
// - 정산용: 체결·취소·시장가 종료·전체 거절 발행
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

/// Kafka 발행기
///
/// 비동기로 Kafka 이벤트를 발행합니다.
/// 완료 대기는 전용 출력 워커에서 수행한다.
#[derive(Clone)]
pub struct KafkaProducer {
    producer: FutureProducer,
}

impl KafkaProducer {
    /// 새 Kafka 발행기 생성
    ///
    /// # 인자
    /// * `bootstrap_servers` - Kafka 브로커 주소 (예: "localhost:9092")
    ///
    /// # 반환값
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

    /// 호가는 금융 정산 토픽과 분리한다. 같은 엔진의 공개 순서는 한 파티션으로
    /// 고정하며, 브로커 확인 전에는 해당 저널 위치의 전달을 완료하지 않는다.
    pub(crate) async fn publish_market_snapshot(
        &self,
        event: &crate::domains::cex::engine::runtime::market::MarketSnapshot,
    ) -> Result<()> {
        self.publish("market-events", Some(&event.engine_id), event)
            .await
    }

    /// Kafka 메시지 발행 (내부 메서드)
    ///
    /// # 인자
    /// * `topic` - 토픽 이름
    /// * `key` - 메시지 키 (옵션)
    /// * `value` - 메시지 값 (JSON 직렬화)
    ///
    /// # 반환값
    /// 발행 성공 여부
    async fn publish<T: Serialize>(&self, topic: &str, key: Option<&str>, value: &T) -> Result<()> {
        // JSON 직렬화
        let json = serde_json::to_string(value).context("Failed to serialize event to JSON")?;

        self.publish_payload(topic, key, &json).await
    }

    /// 감사 조회와 같은 직렬화 결과를 발행한다. JSON 문자열을 다시 JSON 문자열로 감싸지 않는다.
    pub(crate) async fn publish_financial_payload(
        &self,
        engine_id: &str,
        payload: &str,
    ) -> Result<()> {
        self.publish_payload("order-events", Some(engine_id), payload)
            .await
    }
    async fn publish_payload(&self, topic: &str, key: Option<&str>, json: &str) -> Result<()> {
        let mut record = FutureRecord::to(topic);
        if let Some(k) = key {
            record = record.key(k);
        }
        record = record.payload(json);

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
}
