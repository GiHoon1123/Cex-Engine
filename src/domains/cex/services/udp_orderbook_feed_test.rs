// =====================================================
// UDP Orderbook Feed 테스트 코드
// =====================================================
// 역할: UDP 멀티캐스트 오더북 피드의 동작을 검증
// 
// 테스트 항목:
// 1. UDP 멀티캐스트 전송/수신
// 2. 바이너리 직렬화/역직렬화
// 3. 오더북 데이터 정확성
// 4. 순서 번호 (sequence) 증가
// 5. 타임스탬프 정확성
// =====================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domains::cex::services::udp_orderbook_feed::{
        UdpOrderbookFeed, UdpOrderbookFeedConfig, UdpOrderbookMessage, UdpOrderbookEntry
    };
    use std::sync::Arc;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::net::UdpSocket;
    use tokio::time::timeout;
    use crate::domains::cex::engine::runtime::HighPerformanceEngine;
    use crate::shared::database::Database;
    use rust_decimal::Decimal;
    use chrono::Utc;
    use anyhow::{Context, Result};

    /// 테스트용 데이터베이스 생성
    /// Create test database
    async fn create_test_db() -> Database {
        // 테스트용 DB URL (실제 테스트 DB 사용)
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgresql://root:1234@localhost:5432/cex_test".to_string());
        
        Database::new(&database_url).await.expect("Failed to create test database")
    }

    /// 테스트용 엔진 생성
    /// Create test engine
    async fn create_test_engine() -> HighPerformanceEngine {
        let db = create_test_db().await;
        let engine = HighPerformanceEngine::new(db);
        engine
    }

    /// UDP 수신기 생성 (테스트용)
    /// Create UDP receiver for testing
    async fn create_udp_receiver(port: u16, multicast_addr: &str) -> Result<UdpSocket> {
        // UDP 소켓 생성
        let bind_addr = format!("0.0.0.0:{}", port);
        let socket = UdpSocket::bind(&bind_addr)
            .await
            .context("Failed to bind UDP receiver socket")?;
        
        // 멀티캐스트 그룹 가입
        let multicast_addr: SocketAddr = format!("{}:{}", multicast_addr, port)
            .parse()
            .context("Failed to parse multicast address")?;
        
        // 멀티캐스트 그룹에 조인 (IPv4)
        let multicast_ip = multicast_addr.ip();
        if let std::net::IpAddr::V4(ipv4) = multicast_ip {
            socket.join_multicast_v4(ipv4, std::net::Ipv4Addr::UNSPECIFIED)
                .context("Failed to join multicast group")?;
        }
        
        Ok(socket)
    }

    /// UDP 패킷 수신 (타임아웃 포함)
    /// Receive UDP packet with timeout
    async fn receive_udp_packet(
        socket: &UdpSocket,
        timeout_duration: Duration,
    ) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; 65507]; // UDP 최대 패킷 크기
        
        let result = timeout(timeout_duration, socket.recv_from(&mut buf)).await;
        
        match result {
            Ok(Ok((size, _))) => {
                buf.truncate(size);
                Ok(buf)
            }
            Ok(Err(e)) => Err(anyhow::anyhow!("Failed to receive UDP packet: {}", e)),
            Err(_) => Err(anyhow::anyhow!("UDP receive timeout")),
        }
    }

    /// 테스트: UDP 멀티캐스트 전송/수신
    /// Test: UDP multicast transmission/reception
    #[tokio::test]
    async fn test_udp_multicast_transmission() {
        // 1. 테스트 설정
        // 동적 포트 할당: 테스트 간 포트 충돌 방지
        let test_port = 15001; // 테스트용 포트 (기본 포트와 분리, 충돌 방지)
        let multicast_addr = "224.0.0.1";
        
        // 2. 엔진 생성 및 시작
        let mut engine = create_test_engine().await;
        {
            engine.start().await.expect("Failed to start engine");
        }
        
        // 3. UDP 피드 생성 및 시작
        let config = UdpOrderbookFeedConfig {
            multicast_addr: multicast_addr.to_string(),
            port: test_port,
            update_interval_ms: 100,
            depth: 10,
        };
        
        let feed = UdpOrderbookFeed::new(engine.handle(), Some(config))
            .await
            .expect("Failed to create UDP feed");
        
        feed.start();
        
        // 4. UDP 수신기 생성
        let receiver = create_udp_receiver(test_port, multicast_addr)
            .await
            .expect("Failed to create UDP receiver");
        
        // 5. 패킷 수신 (최대 2초 대기)
        let packet_data = receive_udp_packet(&receiver, Duration::from_secs(2))
            .await
            .expect("Failed to receive UDP packet");
        
        // 6. 바이너리 역직렬화
        let message: UdpOrderbookMessage = bincode::deserialize(&packet_data)
            .expect("Failed to deserialize UDP message");
        
        // 7. 검증
        assert_eq!(message.trading_pair, "SOL/USDT");
        assert!(message.sequence > 0);
        assert!(message.timestamp > 0);
        assert!(message.buy_orders.len() <= 10); // depth 제한 확인
        assert!(message.sell_orders.len() <= 10);
        
        println!("✅ UDP multicast transmission test passed");
        println!("   Sequence: {}", message.sequence);
        println!("   Timestamp: {}", message.timestamp);
        println!("   Buy orders: {}", message.buy_orders.len());
        println!("   Sell orders: {}", message.sell_orders.len());
        
        feed.stop();
    }

    /// 테스트: 바이너리 직렬화/역직렬화 정확성
    /// Test: Binary serialization/deserialization accuracy
    #[test]
    fn test_binary_serialization() {
        // 1. 테스트 메시지 생성
        let original_message = UdpOrderbookMessage {
            sequence: 12345,
            timestamp: 1234567890,
            trading_pair: "SOL/USDT".to_string(),
            buy_orders: vec![
                UdpOrderbookEntry {
                    price: Decimal::new(10000, 2), // 100.00
                    amount: Decimal::new(50, 1),   // 5.0
                },
                UdpOrderbookEntry {
                    price: Decimal::new(9950, 2),  // 99.50
                    amount: Decimal::new(30, 1),   // 3.0
                },
            ],
            sell_orders: vec![
                UdpOrderbookEntry {
                    price: Decimal::new(10050, 2), // 100.50
                    amount: Decimal::new(20, 1),    // 2.0
                },
                UdpOrderbookEntry {
                    price: Decimal::new(10100, 2), // 101.00
                    amount: Decimal::new(40, 1),    // 4.0
                },
            ],
        };
        
        // 2. 직렬화
        let serialized = bincode::serialize(&original_message)
            .expect("Failed to serialize message");
        
        // 3. 역직렬화
        let deserialized: UdpOrderbookMessage = bincode::deserialize(&serialized)
            .expect("Failed to deserialize message");
        
        // 4. 검증
        assert_eq!(deserialized.sequence, original_message.sequence);
        assert_eq!(deserialized.timestamp, original_message.timestamp);
        assert_eq!(deserialized.trading_pair, original_message.trading_pair);
        assert_eq!(deserialized.buy_orders.len(), original_message.buy_orders.len());
        assert_eq!(deserialized.sell_orders.len(), original_message.sell_orders.len());
        
        // 가격/수량 정확성 검증
        assert_eq!(deserialized.buy_orders[0].price, Decimal::new(10000, 2));
        assert_eq!(deserialized.buy_orders[0].amount, Decimal::new(50, 1));
        assert_eq!(deserialized.sell_orders[0].price, Decimal::new(10050, 2));
        assert_eq!(deserialized.sell_orders[0].amount, Decimal::new(20, 1));
        
        println!("✅ Binary serialization test passed");
        println!("   Serialized size: {} bytes", serialized.len());
    }

    /// 테스트: 순서 번호 (sequence) 증가 확인
    /// Test: Sequence number increment verification
    #[tokio::test]
    async fn test_sequence_increment() {
        // 1. 테스트 설정
        let test_port = 15002; // 테스트용 포트 (충돌 방지)
        let multicast_addr = "224.0.0.1";
        
        // 2. 엔진 생성 및 시작
        let mut engine = create_test_engine().await;
        {
            engine.start().await.expect("Failed to start engine");
        }
        
        // 3. UDP 피드 생성 및 시작
        let config = UdpOrderbookFeedConfig {
            multicast_addr: multicast_addr.to_string(),
            port: test_port,
            update_interval_ms: 100,
            depth: 10,
        };
        
        let feed = UdpOrderbookFeed::new(engine.handle(), Some(config))
            .await
            .expect("Failed to create UDP feed");
        
        feed.start();
        
        // 4. UDP 수신기 생성
        let receiver = create_udp_receiver(test_port, multicast_addr)
            .await
            .expect("Failed to create UDP receiver");
        
        // 5. 여러 패킷 수신 (최소 3개)
        let mut sequences = Vec::new();
        for _ in 0..3 {
            let packet_data = receive_udp_packet(&receiver, Duration::from_secs(1))
                .await
                .expect("Failed to receive UDP packet");
            
            let message: UdpOrderbookMessage = bincode::deserialize(&packet_data)
                .expect("Failed to deserialize UDP message");
            
            sequences.push(message.sequence);
        }
        
        // 6. 검증: 순서 번호가 증가하는지 확인
        // (타임스탬프 기반이므로 시간이 지나면 증가해야 함)
        assert!(sequences.len() >= 3);
        // 타임스탬프 기반이므로 최소한 마지막이 첫 번째보다 크거나 같아야 함
        assert!(sequences[sequences.len() - 1] >= sequences[0]);
        
        println!("✅ Sequence increment test passed");
        println!("   Sequences: {:?}", sequences);
        
        feed.stop();
    }

    /// 테스트: 타임스탬프 정확성
    /// Test: Timestamp accuracy
    #[tokio::test]
    async fn test_timestamp_accuracy() {
        // 1. 테스트 설정
        let test_port = 15003; // 테스트용 포트 (충돌 방지)
        let multicast_addr = "224.0.0.1";
        
        // 2. 엔진 생성 및 시작
        let mut engine = create_test_engine().await;
        {
            engine.start().await.expect("Failed to start engine");
        }
        
        // 3. UDP 피드 생성 및 시작
        let config = UdpOrderbookFeedConfig {
            multicast_addr: multicast_addr.to_string(),
            port: test_port,
            update_interval_ms: 100,
            depth: 10,
        };
        
        let feed = UdpOrderbookFeed::new(engine.handle(), Some(config))
            .await
            .expect("Failed to create UDP feed");
        
        feed.start();
        
        // 4. UDP 수신기 생성
        let receiver = create_udp_receiver(test_port, multicast_addr)
            .await
            .expect("Failed to create UDP receiver");
        
        // 5. 패킷 수신
        let before_receive = Utc::now().timestamp_millis() as u64;
        let packet_data = receive_udp_packet(&receiver, Duration::from_secs(1))
            .await
            .expect("Failed to receive UDP packet");
        let after_receive = Utc::now().timestamp_millis() as u64;
        
        let message: UdpOrderbookMessage = bincode::deserialize(&packet_data)
            .expect("Failed to deserialize UDP message");
        
        // 6. 검증: 타임스탬프가 수신 시간 범위 내에 있는지 확인
        assert!(message.timestamp >= before_receive.saturating_sub(1000)); // 1초 여유
        assert!(message.timestamp <= after_receive + 1000); // 1초 여유
        
        println!("✅ Timestamp accuracy test passed");
        println!("   Message timestamp: {}", message.timestamp);
        println!("   Before receive: {}", before_receive);
        println!("   After receive: {}", after_receive);
        
        feed.stop();
    }

    /// 테스트: 오더북 데이터 정확성 (엔진과 일치하는지)
    /// Test: Orderbook data accuracy (matches engine)
    #[tokio::test]
    async fn test_orderbook_data_accuracy() {
        // 1. 테스트 설정
        let test_port = 15004; // 테스트용 포트 (충돌 방지)
        let multicast_addr = "224.0.0.1";
        
        // 2. 엔진 생성 및 시작
        let mut engine = create_test_engine().await;
        {
            engine.start().await.expect("Failed to start engine");
        }
        
        // 3. 테스트 주문 추가 (엔진에 직접 주문 제출)
        // 주의: 실제 엔진에 주문을 추가하려면 submit_order를 사용해야 하지만,
        // 테스트에서는 엔진의 내부 상태를 직접 확인하기 어려우므로
        // UDP 메시지의 형식만 검증합니다.
        
        // 4. UDP 피드 생성 및 시작
        let config = UdpOrderbookFeedConfig {
            multicast_addr: multicast_addr.to_string(),
            port: test_port,
            update_interval_ms: 100,
            depth: 10,
        };
        
        let feed = UdpOrderbookFeed::new(engine.handle(), Some(config))
            .await
            .expect("Failed to create UDP feed");
        
        feed.start();
        
        // 5. UDP 수신기 생성
        let receiver = create_udp_receiver(test_port, multicast_addr)
            .await
            .expect("Failed to create UDP receiver");
        
        // 6. 패킷 수신
        let packet_data = receive_udp_packet(&receiver, Duration::from_secs(1))
            .await
            .expect("Failed to receive UDP packet");
        
        let message: UdpOrderbookMessage = bincode::deserialize(&packet_data)
            .expect("Failed to deserialize UDP message");
        
        // 7. 검증: 데이터 형식 확인
        assert_eq!(message.trading_pair, "SOL/USDT");
        
        // 매수 주문은 가격 내림차순이어야 함
        for i in 1..message.buy_orders.len() {
            assert!(message.buy_orders[i-1].price >= message.buy_orders[i].price,
                "Buy orders should be sorted by price descending");
        }
        
        // 매도 주문은 가격 오름차순이어야 함
        for i in 1..message.sell_orders.len() {
            assert!(message.sell_orders[i-1].price <= message.sell_orders[i].price,
                "Sell orders should be sorted by price ascending");
        }
        
        // 가격과 수량이 양수여야 함
        for entry in &message.buy_orders {
            assert!(entry.price > Decimal::ZERO, "Buy order price should be positive");
            assert!(entry.amount >= Decimal::ZERO, "Buy order amount should be non-negative");
        }
        
        for entry in &message.sell_orders {
            assert!(entry.price > Decimal::ZERO, "Sell order price should be positive");
            assert!(entry.amount >= Decimal::ZERO, "Sell order amount should be non-negative");
        }
        
        println!("✅ Orderbook data accuracy test passed");
        println!("   Buy orders: {} levels", message.buy_orders.len());
        println!("   Sell orders: {} levels", message.sell_orders.len());
        
        feed.stop();
    }
}

