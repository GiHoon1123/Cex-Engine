use std::collections::HashMap;
use anyhow::{Context, Result};
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use crate::domains::bot::services::binance_client::{BinanceClient, BinanceOrderbookUpdate};
use crate::domains::bot::services::bot_manager::BotManager;
use crate::domains::cex::services::order_service::OrderService;
use crate::domains::cex::models::order::CreateOrderRequest;

/// 봇 주문 정보
/// Bot order information
/// 
/// 봇이 생성한 주문을 추적하기 위한 구조체
#[derive(Debug, Clone)]
struct BotOrder {
    /// 주문 ID
    order_id: u64,
    
    /// 가격
    price: Decimal,
    
    /// 수량
    amount: Decimal,
}

/// 오더북 동기화 서비스
/// Orderbook Synchronization Service
/// 
/// 역할:
/// - 바이낸스 오더북 업데이트 수신
/// - 기존 봇 주문 취소
/// - 새로운 봇 주문 생성 (바이낸스와 동일한 지정가 주문)
/// 
/// 처리 흐름:
/// 1. 바이낸스 오더북 업데이트 수신
/// 2. 기존 봇 주문 모두 취소
/// 3. 새로운 봇 주문 생성 (상위 N개만)
/// 
/// 주의사항:
/// - bot1은 매수 전용, bot2는 매도 전용
/// - 바이낸스 오더북의 각 호가에 대해 고정 수량으로 주문 생성
pub struct OrderbookSync {
    /// 봇 관리자
    bot_manager: BotManager,
    
    /// 주문 서비스
    order_service: OrderService,
    
    /// 바이낸스 클라이언트
    binance_client: BinanceClient,
    
    /// 봇 1 (매수)의 활성 주문 추적
    /// Bot 1 (buy) active orders tracking
    /// Key: 가격 (Decimal), Value: 주문 정보
    bot1_orders: HashMap<String, BotOrder>,
    
    /// 봇 2 (매도)의 활성 주문 추적
    /// Bot 2 (sell) active orders tracking
    /// Key: 가격 (Decimal), Value: 주문 정보
    bot2_orders: HashMap<String, BotOrder>,
    
    /// 데이터베이스 연결 (주문 조회용)
    db: crate::shared::database::Database,
}

impl OrderbookSync {
    /// 생성자
    /// Constructor
    /// 
    /// # Arguments
    /// * `bot_manager` - 봇 관리자
    /// * `order_service` - 주문 서비스
    /// * `binance_client` - 바이낸스 클라이언트
    /// * `db` - 데이터베이스 연결
    /// 
    /// # Returns
    /// OrderbookSync 인스턴스
    pub fn new(
        bot_manager: BotManager,
        order_service: OrderService,
        binance_client: BinanceClient,
        db: crate::shared::database::Database,
    ) -> Self {
        Self {
            bot_manager,
            order_service,
            binance_client,
            bot1_orders: HashMap::new(),
            bot2_orders: HashMap::new(),
            db,
        }
    }

    /// 오더북 동기화 시작
    /// Start orderbook synchronization
    /// 
    /// 바이낸스 WebSocket을 시작하고, 오더북 업데이트를 처리합니다.
    /// 
    /// # Returns
    /// * `Ok(())` - 동기화 시작 성공
    /// * `Err` - 동기화 시작 실패
    /// 
    /// # 처리 과정
    /// 1. 바이낸스 WebSocket 연결
    /// 2. 오더북 업데이트 수신 루프
    /// 3. 기존 주문 취소
    /// 4. 새 주문 생성
    pub async fn start(&mut self) -> Result<()> {
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 1. 바이낸스 WebSocket 연결
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        let (update_tx, mut update_rx) = mpsc::unbounded_channel();
        
        self.binance_client
            .start(update_tx)
            .await
            .context("Failed to start Binance WebSocket")?;
        
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 2. 오더북 업데이트 수신 루프
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        while let Some(update) = update_rx.recv().await {
            if let Err(e) = self.handle_orderbook_update(update).await {
                eprintln!("[Orderbook Sync] Failed to handle update: {}", e);
                // 에러가 발생해도 계속 진행
            }
        }
        Ok(())
    }

    /// 오더북 업데이트 처리
    /// Handle orderbook update
    /// 
    /// 바이낸스 오더북 업데이트를 받아서 봇 주문을 동기화합니다.
    /// 
    /// # Arguments
    /// * `update` - 바이낸스 오더북 업데이트
    /// 
    /// # 처리 과정
    /// 1. 바이낸스 오더북 파싱
    /// 2. 기존 봇 주문 모두 취소
    /// 3. 새로운 봇 주문 생성 (상위 N개만)
    async fn handle_orderbook_update(
        &mut self,
        update: BinanceOrderbookUpdate,
    ) -> Result<()> {
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 1. 바이낸스 오더북 파싱
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        let (bids, asks) = BinanceClient::parse_orderbook_update(&update)
            .context("Failed to parse Binance orderbook update")?;
        
        let config = self.bot_manager.config();
        let depth = config.orderbook_depth;
        let order_quantity = config.order_quantity;
        
        // 상위 N개만 사용 (참조가 아닌 값으로 복사)
        let top_bids: Vec<_> = bids.iter().take(depth).cloned().collect();
        let top_asks: Vec<_> = asks.iter().take(depth).cloned().collect();
        
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 2. 기존 봇 주문 모두 취소 (user_id 먼저 가져오기)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        let bot1_user_id = self.bot_manager.bot1_user_id();
        let bot2_user_id = self.bot_manager.bot2_user_id();
        
        // 주문 ID 리스트 먼저 수집 (borrow checker 문제 해결)
        let bot1_order_ids: Vec<u64> = self.bot1_orders.values().map(|o| o.order_id).collect();
        let bot2_order_ids: Vec<u64> = self.bot2_orders.values().map(|o| o.order_id).collect();
        
        // Bot 1 (매수) 주문 취소
        if let Some(user_id) = bot1_user_id {
            for order_id in bot1_order_ids {
                // 주문이 이미 체결되었거나 취소되었을 수 있으므로 에러는 무시
                let _ = self.order_service.cancel_order(user_id, order_id).await;
            }
            self.bot1_orders.clear();
        }
        
        // Bot 2 (매도) 주문 취소
        if let Some(user_id) = bot2_user_id {
            for order_id in bot2_order_ids {
                // 주문이 이미 체결되었거나 취소되었을 수 있으므로 에러는 무시
                let _ = self.order_service.cancel_order(user_id, order_id).await;
            }
            self.bot2_orders.clear();
        }
        
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 3. 새로운 봇 주문 생성
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // Bot 1 (매수): 바이낸스 매수 호가와 동일한 지정가 매수 주문
        // 주문 생성 결과를 먼저 수집 (borrow checker 문제 해결)
        let mut bot1_new_orders = Vec::new();
        if let Some(user_id) = bot1_user_id {
            for bid in top_bids {
                let order = self.create_bot_order_internal(
                    user_id,
                    "buy",
                    bid.price,
                    order_quantity,
                )
                .await?;
                
                if let Some(order) = order {
                    bot1_new_orders.push((bid.price, order.id));
                }
            }
        }
        
        // Bot 2 (매도): 바이낸스 매도 호가와 동일한 지정가 매도 주문
        // 주문 생성 결과를 먼저 수집 (borrow checker 문제 해결)
        let mut bot2_new_orders = Vec::new();
        if let Some(user_id) = bot2_user_id {
            for ask in top_asks {
                let order = self.create_bot_order_internal(
                    user_id,
                    "sell",
                    ask.price,
                    order_quantity,
                )
                .await?;
                
                if let Some(order) = order {
                    bot2_new_orders.push((ask.price, order.id));
                }
            }
        }
        
        // 주문 맵에 추가 (이제 self를 mutable로 빌릴 수 있음)
        for (price, order_id) in bot1_new_orders {
            let price_key = price.to_string();
            self.bot1_orders.insert(price_key, BotOrder {
                order_id,
                price,
                amount: order_quantity,
            });
        }
        
        for (price, order_id) in bot2_new_orders {
            let price_key = price.to_string();
            self.bot2_orders.insert(price_key, BotOrder {
                order_id,
                price,
                amount: order_quantity,
            });
        }
        
        Ok(())
    }


    /// 봇 주문 생성 (내부 함수)
    /// Create bot order (internal function)
    /// 
    /// # Arguments
    /// * `user_id` - 봇 사용자 ID
    /// * `order_type` - 주문 타입 ("buy" 또는 "sell")
    /// * `price` - 가격
    /// * `amount` - 수량
    /// 
    /// # Returns
    /// * `Ok(Some(Order))` - 주문 생성 성공
    /// * `Ok(None)` - 주문 생성 실패 (로그만 출력)
    /// * `Err` - 주문 생성 실패
    async fn create_bot_order_internal(
        &self,
        user_id: u64,
        order_type: &str,
        price: Decimal,
        amount: Decimal,
    ) -> Result<Option<crate::domains::cex::models::order::Order>> {
        // 주문 생성 요청
        let request = CreateOrderRequest {
            user_id, // user_id 추가
            order_type: order_type.to_string(),
            order_side: "limit".to_string(), // 항상 지정가
            base_mint: "SOL".to_string(),
            quote_mint: Some("USDT".to_string()),
            price: Some(price),
            amount: Some(amount),
            quote_amount: None, // 지정가는 수량 기반
        };
        
        // 주문 생성
        match self.order_service.create_order(user_id, request).await {
            Ok(order) => Ok(Some(order)),
            Err(e) => {
                // 주문 생성 실패 (잔고 부족 등) - 로그만 출력하고 계속 진행
                eprintln!(
                    "[Orderbook Sync] Failed to create bot order: user_id={}, type={}, price={}, amount={}, error={}",
                    user_id, order_type, price, amount, e
                );
                Ok(None)
            }
        }
    }

}

