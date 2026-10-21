// =====================================================
// UDP Orderbook Feed - UDP 멀티캐스트 기반 오더북 스트리밍
// =====================================================
// 역할: 오더북 업데이트를 UDP 멀티캐스트로 실시간 전송
// 
// 특징:
// - UDP 멀티캐스트 사용 (한 번 전송으로 여러 클라이언트 수신)
// - 바이너리 직렬화 (bincode)로 빠른 전송
// - 주기적 업데이트 (100ms마다)
// - 손실 허용 가능한 데이터에만 사용 (오더북 스냅샷/업데이트)
// 
// 사용 목적:
// - 실시간 오더북 스트리밍
// - 낮은 지연시간 (TCP 대비 30-50% 감소)
// - 높은 처리량 (여러 클라이언트에 동시 전송)
// 
// 주의사항:
// - 패킷 손실 가능 (신뢰성 낮음)
// - 주문/체결은 TCP 유지 (신뢰성 필요)
// =====================================================

use std::sync::Arc;
use std::net::SocketAddr;
use anyhow::{Context, Result};
use tokio::net::UdpSocket;
use tokio::time::{interval, Duration};
use rust_decimal::Decimal;
use serde::{Serialize, Deserialize};
use crate::domains::cex::engine::runtime::EngineHandle;
use crate::domains::cex::engine::Engine;
use crate::domains::cex::engine::types::TradingPair;

/// UDP 오더북 피드 설정
/// UDP Orderbook Feed Configuration
#[derive(Debug, Clone)]
pub struct UdpOrderbookFeedConfig {
    /// 멀티캐스트 주소 (예: "224.0.0.1")
    /// Multicast address
    pub multicast_addr: String,
    
    /// 멀티캐스트 포트 (예: 5000)
    /// Multicast port
    pub port: u16,
    
    /// 업데이트 주기 (밀리초, 기본값: 100ms)
    /// Update interval in milliseconds
    pub update_interval_ms: u64,
    
    /// 오더북 깊이 (가격 레벨 개수, 기본값: 20)
    /// Orderbook depth (number of price levels)
    pub depth: usize,
}

impl Default for UdpOrderbookFeedConfig {
    fn default() -> Self {
        Self {
            multicast_addr: "224.0.0.1".to_string(),
            port: 5000,
            update_interval_ms: 100, // 100ms마다 업데이트
            depth: 20,
        }
    }
}

/// UDP 오더북 피드 서비스
/// UDP Orderbook Feed Service
/// 
/// 엔진에서 오더북을 주기적으로 가져와서 UDP 멀티캐스트로 전송합니다.
pub struct UdpOrderbookFeed {
    /// 엔진 참조 (오더북 조회용)
    /// Engine reference for orderbook queries
    engine: EngineHandle,
    
    /// UDP 소켓 (Arc로 감싸서 공유)
    /// UDP socket for multicast transmission (wrapped in Arc for sharing)
    socket: Arc<UdpSocket>,
    
    /// 멀티캐스트 주소
    /// Multicast address
    multicast_addr: SocketAddr,
    
    /// 설정
    /// Configuration
    config: UdpOrderbookFeedConfig,
    
    /// 실행 중 여부 플래그
    /// Running flag
    running: Arc<std::sync::atomic::AtomicBool>,
}

impl UdpOrderbookFeed {
    /// 새 UDP 오더북 피드 생성
    /// Create new UDP orderbook feed
    /// 
    /// # Arguments
    /// * `engine` - 엔진 참조
    /// * `config` - 설정 (None이면 기본값 사용)
    /// 
    /// # Returns
    /// * `Ok(UdpOrderbookFeed)` - 생성 성공
    /// * `Err` - 생성 실패 (소켓 바인딩 실패 등)
    /// 
    /// # Examples
    /// ```
    /// let feed = UdpOrderbookFeed::new(engine, None).await?;
    /// ```
    pub async fn new(
        engine: EngineHandle,
        config: Option<UdpOrderbookFeedConfig>,
    ) -> Result<Self> {
        let config = config.unwrap_or_default();
        
        // UDP 소켓 생성 (송신자는 포트 바인딩 불필요)
        // UDP multicast sender doesn't need to bind to a specific port
        // Binding to 0.0.0.0:0 allows OS to assign an ephemeral port
        let socket = UdpSocket::bind("0.0.0.0:0")
            .await
            .context("Failed to bind UDP socket")?;
        
        // 멀티캐스트 루프백 (로컬에서도 수신 가능하도록)
        // Set multicast loopback (allow local reception)
        socket.set_multicast_loop_v4(true)
            .context("Failed to set multicast loopback")?;
        
        // 멀티캐스트 주소 파싱
        // Parse multicast address
        let multicast_addr = format!("{}:{}", config.multicast_addr, config.port)
            .parse::<SocketAddr>()
            .context("Failed to parse multicast address")?;
        
        Ok(Self {
            engine,
            socket: Arc::new(socket),
            multicast_addr,
            config,
            running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }
    
    /// UDP 피드 시작
    /// Start UDP feed
    /// 
    /// 백그라운드 태스크를 시작하여 주기적으로 오더북을 UDP로 전송합니다.
    /// 
    /// # 처리 과정
    /// 1. 주기적으로 엔진에서 오더북 조회
    /// 2. 바이너리 직렬화 (bincode)
    /// 3. UDP 멀티캐스트로 전송
    /// 
    /// # Examples
    /// ```
    /// feed.start().await?;
    /// ```
    pub fn start(&self) {
        let engine = self.engine.clone();
        // UDP 소켓은 이미 Arc로 감싸져 있음
        let socket = Arc::clone(&self.socket);
        let multicast_addr = self.multicast_addr;
        let update_interval = Duration::from_millis(self.config.update_interval_ms);
        let depth = self.config.depth;
        let running = Arc::clone(&self.running);
        
        // 실행 플래그 설정
        running.store(true, std::sync::atomic::Ordering::Relaxed);
        
        tokio::spawn(async move {
            let mut interval = interval(update_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            
            eprintln!("[UDP Orderbook Feed] Started: multicast={}:{}, interval={}ms, depth={}",
                multicast_addr.ip(), multicast_addr.port(), 
                update_interval.as_millis(), depth);
            
            loop {
                // 실행 플래그 확인
                if !running.load(std::sync::atomic::Ordering::Relaxed) {
                    eprintln!("[UDP Orderbook Feed] Stopped");
                    break;
                }
                
                interval.tick().await;
                
                // 모든 거래쌍의 오더북 전송
                // Send orderbook for all trading pairs
                if let Err(e) = Self::send_orderbook_for_all_pairs(
                    &engine,
                    &socket,
                    &multicast_addr,
                    depth,
                ).await {
                    eprintln!("[UDP Orderbook Feed] Failed to send orderbook: {}", e);
                    // 에러가 발생해도 계속 진행 (다음 주기에서 재시도)
                }
            }
        });
    }
    
    /// UDP 피드 중지
    /// Stop UDP feed
    /// 
    /// 실행 플래그를 해제하여 백그라운드 태스크를 종료합니다.
    pub fn stop(&self) {
        self.running.store(false, std::sync::atomic::Ordering::Relaxed);
        eprintln!("[UDP Orderbook Feed] Stop requested");
    }
    
    /// 모든 거래쌍의 오더북 전송 (내부 헬퍼)
    /// Send orderbook for all trading pairs (internal helper)
    /// 
    /// # Arguments
    /// * `engine` - 엔진 참조
    /// * `socket` - UDP 소켓 (Arc로 감싼)
    /// * `multicast_addr` - 멀티캐스트 주소
    /// * `depth` - 오더북 깊이
    /// 
    /// # 처리 과정
    /// 1. 엔진에서 오더북 조회
    /// 2. UDP Orderbook Message로 변환
    /// 3. 바이너리 직렬화
    /// 4. UDP 멀티캐스트로 전송
    async fn send_orderbook_for_all_pairs(
        engine: &EngineHandle,
        socket: &Arc<UdpSocket>,
        multicast_addr: &SocketAddr,
        depth: usize,
    ) -> Result<()> {
        // 현재는 SOL/USDT만 지원 (나중에 확장 가능)
        // Currently only supports SOL/USDT (can be extended later)
        let trading_pair = TradingPair::new("SOL".to_string(), "USDT".to_string());
        
        // 엔진에서 오더북 조회
        // 명령 핸들로 조회한다. 응답을 기다리는 동안 다른 요청의 진입을 잠그지 않는다.
        let (buy_entries, sell_entries) = {
            Engine::get_orderbook(engine, &trading_pair, Some(depth))
                .await
                .context("Failed to get orderbook from engine")?
        };
        
        // OrderEntry를 UDP Orderbook Entry로 변환
        // Convert OrderEntry to UDP Orderbook Entry
        let buy_orders: Vec<UdpOrderbookEntry> = buy_entries
            .into_iter()
            .map(|entry| UdpOrderbookEntry {
                price: entry.price.unwrap_or(Decimal::ZERO),
                amount: entry.remaining_amount,
            })
            .collect();
        
        let sell_orders: Vec<UdpOrderbookEntry> = sell_entries
            .into_iter()
            .map(|entry| UdpOrderbookEntry {
                price: entry.price.unwrap_or(Decimal::ZERO),
                amount: entry.remaining_amount,
            })
            .collect();
        
        // UDP Orderbook Message 생성
        // Create UDP Orderbook Message
        let message = UdpOrderbookMessage {
            sequence: chrono::Utc::now().timestamp_millis() as u64, // 순서 번호 (타임스탬프 기반)
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            trading_pair: trading_pair.to_string(),
            buy_orders,
            sell_orders,
        };
        
        // 바이너리 직렬화 (bincode 사용)
        // Binary serialization using bincode
        let serialized = bincode::serialize(&message)
            .context("Failed to serialize orderbook message")?;
        
        // UDP 멀티캐스트로 전송
        // Send via UDP multicast
        socket.send_to(&serialized, multicast_addr)
            .await
            .context("Failed to send UDP packet")?;
        
        Ok(())
    }
}

/// UDP 오더북 메시지 구조체
/// UDP Orderbook Message Structure
/// 
/// UDP로 전송되는 오더북 데이터 형식입니다.
/// 바이너리 직렬화를 위해 bincode를 사용합니다.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UdpOrderbookMessage {
    /// 순서 번호 (패킷 손실 감지용)
    /// Sequence number (for packet loss detection)
    pub sequence: u64,
    
    /// 타임스탬프 (밀리초)
    /// Timestamp in milliseconds
    pub timestamp: u64,
    
    /// 거래쌍 (예: "SOL/USDT")
    /// Trading pair (e.g., "SOL/USDT")
    pub trading_pair: String,
    
    /// 매수 주문 목록 (가격 내림차순)
    /// Buy orders (sorted by price descending)
    pub buy_orders: Vec<UdpOrderbookEntry>,
    
    /// 매도 주문 목록 (가격 오름차순)
    /// Sell orders (sorted by price ascending)
    pub sell_orders: Vec<UdpOrderbookEntry>,
}

/// UDP 오더북 엔트리 (가격 레벨)
/// UDP Orderbook Entry (Price Level)
/// 
/// 각 가격 레벨의 정보를 나타냅니다.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UdpOrderbookEntry {
    /// 가격 (String으로 직렬화하여 bincode 호환성 확보)
    /// Price (serialized as String for bincode compatibility)
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    
    /// 수량 (String으로 직렬화하여 bincode 호환성 확보)
    /// Amount (serialized as String for bincode compatibility)
    #[serde(with = "rust_decimal::serde::str")]
    pub amount: Decimal,
}

