// =====================================================
// Matcher - 주문 매칭 로직
// =====================================================
// 역할: OrderBook에서 주문을 매칭하고 체결 가능한 주문들을 찾음
// 
// 핵심 알고리즘:  ㅇ
// 1. 가격 우선: 높은 매수가 vs 낮은 매도가
// 2. 시간 우선: 같은 가격이면 먼저 온 주문
// 
// 처리 흐름:
// 1. 신규 주문 받기
// 2. 반대편 호가와 비교 (매수면 매도와, 매도면 매수와)
// 3. 매칭 가능한 주문 찾기
// 4. 체결 실행 (수량 차감)
// 5. MatchResult 반환
// =====================================================

use rust_decimal::Decimal;
use crate::domains::cex::engine::types::{OrderEntry, MatchResult};
use crate::domains::cex::engine::orderbook::OrderBook;

/// 매칭 엔진
/// OrderBook을 받아서 매칭 로직을 실행
pub struct Matcher;

impl Matcher {
    /// 새 Matcher 생성
    pub fn new() -> Self {
        Self
    }
    
    /// 주문 매칭 실행
    /// 
    /// # Arguments
    /// * `incoming_order` - 새로 들어온 주문 (가변 참조, 수량이 변경될 수 있음)
    /// * `orderbook` - 호가창 (가변 참조, 주문이 제거/수정됨)
    /// 
    /// # Returns
    /// 체결된 매칭 결과들 (Vec<MatchResult>)
    /// 
    /// # Logic
    /// 1. 주문 타입 확인 (buy/sell, limit/market)
    /// 2. 반대편 최선가 확인
    /// 3. 매칭 가능 여부 판단
    /// 4. 매칭 실행 (FIFO, Price-Time Priority)
    /// 5. 주문 수량 업데이트
    pub fn match_order(
        &self,
        incoming_order: &mut OrderEntry,
        orderbook: &mut OrderBook,
    ) -> Vec<MatchResult> {
        let mut matches = Vec::new();
        
        // 주문이 이미 완전히 체결되었으면 종료
        // 시장가 매수는 remaining_quote_amount를 사용하므로, remaining_amount만 확인하면 안 됨
        let has_remaining = if let Some(remaining_quote) = incoming_order.remaining_quote_amount {
            remaining_quote > Decimal::ZERO
        } else {
            incoming_order.remaining_amount > Decimal::ZERO
        };
        
        if !has_remaining {
            return matches;
        }
        
        // 주문 타입에 따라 매칭
        match incoming_order.order_type.as_str() {
            "buy" => self.match_buy_order(incoming_order, orderbook, &mut matches),
            "sell" => self.match_sell_order(incoming_order, orderbook, &mut matches),
            _ => {} // 잘못된 타입 무시
        }
        
        matches
    }
    
    /// 매수 주문 매칭 (매도 호가와 매칭)
    /// 
    /// 매칭 조건:
    /// - 지정가: 매수 가격 >= 매도 가격
    /// - 시장가: 무조건 매칭 (최선 매도가로)
    fn match_buy_order(
        &self,
        buy_order: &mut OrderEntry,
        orderbook: &mut OrderBook,
        matches: &mut Vec<MatchResult>,
    ) {
        // 매도 호가가 비어있으면 매칭 불가 (정상적인 상황 - 매도 주문이 아직 없을 수 있음)
        let best_ask = match orderbook.get_best_ask() {
            Some(price) => price,
            None => {
                // 매도 호가가 없으면 매칭 불가 (로그 제거 - 정상적인 상황)
                return; // 매도 호가 없음
            }
        };
        
        // 디버깅: 시장가 매수 주문의 remaining_quote_amount 확인
        if buy_order.order_side == "market" {
            #[cfg(not(feature = "bench_mode"))]
            {
                eprintln!(
                    "[Matcher] Market buy order {}: remaining_quote_amount={:?}, remaining_amount={}, best_ask={}",
                    buy_order.id, buy_order.remaining_quote_amount, buy_order.remaining_amount, best_ask
                );
            }
        }
        
        // 지정가 주문: 가격 확인
        if buy_order.order_side == "limit" {
            let buy_price = buy_order.price.expect("Limit order must have price");
            if buy_price < best_ask {
                return; // 매칭 불가 (매수 가격이 낮음)
            }
        }
        // 시장가 주문: 항상 매칭 시도
        
        // 매도 호가 순회 (낮은 가격부터)
        loop {
            // 시장가 매수 금액 기반: remaining_quote_amount 확인
            // 수량 기반: remaining_amount 확인
            let is_fully_filled = if let Some(remaining_quote) = buy_order.remaining_quote_amount {
                // 금액 기반: 남은 금액이 0이면 완전 체결
                remaining_quote <= Decimal::ZERO
            } else {
                // 수량 기반: 남은 수량이 0이면 완전 체결
                buy_order.remaining_amount == Decimal::ZERO
            };
            
            if is_fully_filled {
                break;
            }
            
            // 현재 최선 매도가 가져오기
            let current_ask = match orderbook.get_best_ask() {
                Some(price) => price,
                None => break, // 더 이상 매도 호가 없음
            };
            
            // 지정가 매수: 가격 재확인
            if buy_order.order_side == "limit" {
                let buy_price = buy_order.price.unwrap();
                if buy_price < current_ask {
                    break; // 더 이상 매칭 불가
                }
            }
            
            // 해당 가격의 매도 주문들 가져오기
            let sell_orders = match orderbook.sell_orders.get_orders_at_price_mut(&current_ask) {
                Some(orders) => orders,
                None => break,
            };
            
            // FIFO: 가장 오래된 주문부터 매칭
            let initial_queue_len = sell_orders.len(); // 초기 큐 길이
            let mut processed_count = 0; // 처리한 주문 수 (무한 루프 방지)
            
            while let Some(mut sell_order) = sell_orders.pop_front() {
                processed_count += 1;
                
                // 무한 루프 방지: 초기 큐 길이만큼 처리했는데도 매칭이 안 되면 종료
                if processed_count > initial_queue_len * 2 {
                    // 모든 주문이 같은 user_id일 가능성이 높음
                    break;
                }
                
                // Self-Trade 금지: 같은 유저의 주문은 매칭하지 않음
                if buy_order.user_id == sell_order.user_id {
                    // 같은 유저의 주문이므로 다시 큐에 추가하고 다음 주문으로
                    sell_orders.push_back(sell_order);
                    continue;
                }
                
                // 매칭 수량 계산
                let match_amount = if let Some(remaining_quote) = buy_order.remaining_quote_amount {
                    // 시장가 매수 금액 기반: remaining_quote_amount / price로 수량 계산
                    let max_amount_from_quote = remaining_quote / current_ask;
                    max_amount_from_quote.min(sell_order.remaining_amount)
                } else {
                    // 수량 기반: 둘 중 작은 것
                    buy_order.remaining_amount.min(sell_order.remaining_amount)
                };
                
                if match_amount <= Decimal::ZERO {
                    break; // 더 이상 매칭 불가
                }
                
                // 체결 가격 (Taker가 받아들이는 가격 = 매도 가격)
                let match_price = current_ask;
                
                // MatchResult 생성
                eprintln!("[Matcher] MatchResult 생성: buy_order.id={}, sell_order.id={}, buy_order.user_id={}, sell_order.user_id={}", 
                         buy_order.id, sell_order.id, buy_order.user_id, sell_order.user_id);
                let match_result = MatchResult {
                    buy_order_id: buy_order.id,
                    sell_order_id: sell_order.id,
                    buyer_id: buy_order.user_id,
                    seller_id: sell_order.user_id,
                    price: match_price,
                    amount: match_amount,
                    base_mint: buy_order.base_mint.clone(),
                    quote_mint: buy_order.quote_mint.clone(),
                };
                
                // 주문 수량/금액 차감
                if let Some(ref mut remaining_quote) = buy_order.remaining_quote_amount {
                    // 시장가 매수 금액 기반: quote_amount 차감
                    let quote_used = match_amount * match_price;
                    *remaining_quote -= quote_used;
                    
                    // amount 업데이트 (매칭된 수량 누적)
                    // 초기 amount는 0이었고, 매칭될 때마다 증가
                    buy_order.amount += match_amount;
                    buy_order.filled_amount += match_amount;
                    // remaining_amount = amount - filled_amount (자동 계산)
                    buy_order.remaining_amount = buy_order.amount - buy_order.filled_amount;
                } else {
                    // 수량 기반: amount 차감
                buy_order.remaining_amount -= match_amount;
                buy_order.filled_amount += match_amount;
                }
                
                sell_order.remaining_amount -= match_amount;
                sell_order.filled_amount += match_amount;
                
                // 매도 주문이 남아있으면 다시 큐에 추가
                if sell_order.remaining_amount > Decimal::ZERO {
                    sell_orders.push_front(sell_order);
                }
                
                // 매칭 결과 저장
                matches.push(match_result);
                
                // 매수 주문이 완전히 체결되었는지 확인
                let is_fully_filled = if let Some(remaining_quote) = buy_order.remaining_quote_amount {
                    remaining_quote <= Decimal::ZERO
                } else {
                    buy_order.remaining_amount == Decimal::ZERO
                };
                
                if is_fully_filled {
                    break;
                }
            }
            
            // 해당 가격의 주문이 모두 소진되었는지 확인
            if sell_orders.is_empty() {
                // OrderBook에서 해당 가격 레벨 제거
                orderbook.sell_orders.orders.remove(&current_ask);
            }
        }
    }
    
    /// 매도 주문 매칭 (매수 호가와 매칭)
    /// 
    /// 매칭 조건:
    /// - 지정가: 매도 가격 <= 매수 가격
    /// - 시장가: 무조건 매칭 (최선 매수가로)
    fn match_sell_order(
        &self,
        sell_order: &mut OrderEntry,
        orderbook: &mut OrderBook,
        matches: &mut Vec<MatchResult>,
    ) {
        // 매수 호가가 비어있으면 매칭 불가
        let best_bid = match orderbook.get_best_bid() {
            Some(price) => price,
            None => return, // 매수 호가 없음
        };
        
        // 지정가 주문: 가격 확인
        if sell_order.order_side == "limit" {
            let sell_price = sell_order.price.expect("Limit order must have price");
            if sell_price > best_bid {
                return; // 매칭 불가 (매도 가격이 높음)
            }
        }
        // 시장가 주문: 항상 매칭 시도
        
        // 매수 호가 순회 (높은 가격부터)
        loop {
            // 매도 주문이 완전히 체결되었으면 종료
            if sell_order.remaining_amount == Decimal::ZERO {
                break;
            }
            
            // 현재 최선 매수가 가져오기
            let current_bid = match orderbook.get_best_bid() {
                Some(price) => price,
                None => break, // 더 이상 매수 호가 없음
            };
            
            // 지정가 매도: 가격 재확인
            if sell_order.order_side == "limit" {
                let sell_price = sell_order.price.unwrap();
                if sell_price > current_bid {
                    break; // 더 이상 매칭 불가
                }
            }
            
            // 해당 가격의 매수 주문들 가져오기
            let buy_orders = match orderbook.buy_orders.get_orders_at_price_mut(&current_bid) {
                Some(orders) => orders,
                None => break,
            };
            
            // FIFO: 가장 오래된 주문부터 매칭
            let initial_queue_len = buy_orders.len(); // 초기 큐 길이
            let mut processed_count = 0; // 처리한 주문 수 (무한 루프 방지)
            
            while let Some(mut buy_order) = buy_orders.pop_front() {
                processed_count += 1;
                
                // 무한 루프 방지: 초기 큐 길이만큼 처리했는데도 매칭이 안 되면 종료
                if processed_count > initial_queue_len * 2 {
                    // 모든 주문이 같은 user_id일 가능성이 높음
                    break;
                }
                
                // Self-Trade 금지: 같은 유저의 주문은 매칭하지 않음
                if buy_order.user_id == sell_order.user_id {
                    // 같은 유저의 주문이므로 다시 큐에 추가하고 다음 주문으로
                    buy_orders.push_back(buy_order);
                    continue;
                }
                
                // 매칭 수량 계산 (둘 중 작은 것)
                let match_amount = sell_order.remaining_amount.min(buy_order.remaining_amount);
                
                // 체결 가격 (Maker가 제시한 가격 = 매수 가격)
                let match_price = current_bid;
                
                // MatchResult 생성
                eprintln!("[Matcher] MatchResult 생성 (limit, sell order): buy_order.id={}, sell_order.id={}, buy_order.user_id={}, sell_order.user_id={}", 
                         buy_order.id, sell_order.id, buy_order.user_id, sell_order.user_id);
                let match_result = MatchResult {
                    buy_order_id: buy_order.id,
                    sell_order_id: sell_order.id,
                    buyer_id: buy_order.user_id,
                    seller_id: sell_order.user_id,
                    price: match_price,
                    amount: match_amount,
                    base_mint: sell_order.base_mint.clone(),
                    quote_mint: sell_order.quote_mint.clone(),
                };
                
                // 주문 수량 차감
                sell_order.remaining_amount -= match_amount;
                sell_order.filled_amount += match_amount;
                buy_order.remaining_amount -= match_amount;
                buy_order.filled_amount += match_amount;
                
                // 매수 주문이 남아있으면 다시 큐에 추가
                if buy_order.remaining_amount > Decimal::ZERO {
                    buy_orders.push_front(buy_order);
                }
                
                // 매칭 결과 저장
                matches.push(match_result);
                
                // 매도 주문이 완전히 체결되었으면 종료
                if sell_order.remaining_amount == Decimal::ZERO {
                    break;
                }
            }
            
            // 해당 가격의 주문이 모두 소진되었는지 확인
            if buy_orders.is_empty() {
                // OrderBook에서 해당 가격 레벨 제거
                orderbook.buy_orders.orders.remove(&current_bid);
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::domains::cex::engine::types::TradingPair;
    use chrono::Utc;
    
    fn create_test_order(
        id: u64,
        user_id: u64,
        order_type: &str,
        order_side: &str,
        price: Option<f64>,
        amount: f64,
    ) -> OrderEntry {
        OrderEntry {
            id,
            user_id,
            order_type: order_type.to_string(),
            order_side: order_side.to_string(),
            base_mint: "SOL".to_string(),
            quote_mint: "USDT".to_string(),
            price: price.map(|p| Decimal::from_f64_retain(p).unwrap()),
            amount: Decimal::from_f64_retain(amount).unwrap(),
            quote_amount: None,
            filled_amount: Decimal::ZERO,
            remaining_amount: Decimal::from_f64_retain(amount).unwrap(),
            remaining_quote_amount: None,
            created_at: Utc::now(),
        }
    }
    
    #[test]
    fn test_match_limit_buy_with_sell() {
        // 매수 지정가 주문이 매도 호가와 매칭되는지 테스트
        let pair = TradingPair::new("SOL".to_string(), "USDT".to_string());
        let mut orderbook = OrderBook::new(pair);
        let matcher = Matcher::new();
        
        // 매도 호가 추가: 100 USDT (2 SOL), 101 USDT (1 SOL)
        orderbook.add_order(create_test_order(1, 100, "sell", "limit", Some(100.0), 2.0));
        orderbook.add_order(create_test_order(2, 101, "sell", "limit", Some(101.0), 1.0));
        
        // 매수 주문: 100.5 USDT로 1.5 SOL 매수
        let mut buy_order = create_test_order(3, 200, "buy", "limit", Some(100.5), 1.5);
        
        // 매칭 실행
        let matches = matcher.match_order(&mut buy_order, &mut orderbook);
        
        // 검증
        assert_eq!(matches.len(), 1); // 1건 체결
        assert_eq!(matches[0].price, Decimal::from_f64_retain(100.0).unwrap()); // 100 USDT에 체결
        assert_eq!(matches[0].amount, Decimal::from_f64_retain(1.5).unwrap()); // 1.5 SOL 체결
        assert_eq!(matches[0].buyer_id, 200);
        assert_eq!(matches[0].seller_id, 100);
        
        // 매수 주문 완전 체결 확인
        assert_eq!(buy_order.remaining_amount, Decimal::ZERO);
        assert_eq!(buy_order.filled_amount, Decimal::from_f64_retain(1.5).unwrap());
        
        // 매도 호가에 0.5 SOL 남음 (2 - 1.5)
        assert_eq!(orderbook.total_sell_orders(), 2); // 두 가격 레벨
    }
    
    #[test]
    fn test_match_limit_sell_with_buy() {
        // 매도 지정가 주문이 매수 호가와 매칭되는지 테스트
        let pair = TradingPair::new("SOL".to_string(), "USDT".to_string());
        let mut orderbook = OrderBook::new(pair);
        let matcher = Matcher::new();
        
        // 매수 호가 추가: 100 USDT (1 SOL), 99 USDT (2 SOL)
        orderbook.add_order(create_test_order(1, 100, "buy", "limit", Some(100.0), 1.0));
        orderbook.add_order(create_test_order(2, 101, "buy", "limit", Some(99.0), 2.0));
        
        // 매도 주문: 99.5 USDT로 1.5 SOL 매도
        let mut sell_order = create_test_order(3, 200, "sell", "limit", Some(99.5), 1.5);
        
        // 매칭 실행
        let matches = matcher.match_order(&mut sell_order, &mut orderbook);
        
        // 검증
        assert_eq!(matches.len(), 1); // 1건 체결
        assert_eq!(matches[0].price, Decimal::from_f64_retain(100.0).unwrap()); // 100 USDT에 체결
        assert_eq!(matches[0].amount, Decimal::from_f64_retain(1.0).unwrap()); // 1 SOL 체결
        
        // 매도 주문 부분 체결 확인 (0.5 SOL 남음)
        assert_eq!(sell_order.remaining_amount, Decimal::from_f64_retain(0.5).unwrap());
        assert_eq!(sell_order.filled_amount, Decimal::from_f64_retain(1.0).unwrap());
    }
    
    #[test]
    fn test_match_market_buy() {
        // 시장가 매수 주문 테스트
        let pair = TradingPair::new("SOL".to_string(), "USDT".to_string());
        let mut orderbook = OrderBook::new(pair);
        let matcher = Matcher::new();
        
        // 매도 호가 추가
        orderbook.add_order(create_test_order(1, 100, "sell", "limit", Some(100.0), 1.0));
        orderbook.add_order(create_test_order(2, 101, "sell", "limit", Some(101.0), 2.0));
        
        // 시장가 매수: 2.5 SOL
        let mut market_buy = create_test_order(3, 200, "buy", "market", None, 2.5);
        
        // 매칭 실행
        let matches = matcher.match_order(&mut market_buy, &mut orderbook);
        
        // 검증
        assert_eq!(matches.len(), 2); // 2건 체결 (두 가격에서)
        
        // 첫 번째: 100 USDT에 1 SOL
        assert_eq!(matches[0].price, Decimal::from_f64_retain(100.0).unwrap());
        assert_eq!(matches[0].amount, Decimal::from_f64_retain(1.0).unwrap());
        
        // 두 번째: 101 USDT에 1.5 SOL
        assert_eq!(matches[1].price, Decimal::from_f64_retain(101.0).unwrap());
        assert_eq!(matches[1].amount, Decimal::from_f64_retain(1.5).unwrap());
        
        // 완전 체결 확인
        assert_eq!(market_buy.remaining_amount, Decimal::ZERO);
    }
    
    #[test]
    fn test_no_match_price_mismatch() {
        // 가격 불일치로 매칭 안 되는 경우
        let pair = TradingPair::new("SOL".to_string(), "USDT".to_string());
        let mut orderbook = OrderBook::new(pair);
        let matcher = Matcher::new();
        
        // 매도 호가: 101 USDT
        orderbook.add_order(create_test_order(1, 100, "sell", "limit", Some(101.0), 1.0));
        
        // 매수 주문: 99 USDT (너무 낮음)
        let mut buy_order = create_test_order(2, 200, "buy", "limit", Some(99.0), 1.0);
        
        // 매칭 실행
        let matches = matcher.match_order(&mut buy_order, &mut orderbook);
        
        // 매칭 안 됨
        assert_eq!(matches.len(), 0);
        assert_eq!(buy_order.remaining_amount, Decimal::from_f64_retain(1.0).unwrap());
        assert_eq!(buy_order.filled_amount, Decimal::ZERO);
    }
    
    #[test]
    fn test_partial_fill() {
        // 부분 체결 테스트
        let pair = TradingPair::new("SOL".to_string(), "USDT".to_string());
        let mut orderbook = OrderBook::new(pair);
        let matcher = Matcher::new();
        
        // 매도 호가: 100 USDT에 0.5 SOL만 있음
        orderbook.add_order(create_test_order(1, 100, "sell", "limit", Some(100.0), 0.5));
        
        // 매수 주문: 100 USDT로 2 SOL 요청
        let mut buy_order = create_test_order(2, 200, "buy", "limit", Some(100.0), 2.0);
        
        // 매칭 실행
        let matches = matcher.match_order(&mut buy_order, &mut orderbook);
        
        // 부분 체결 (0.5 SOL만)
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].amount, Decimal::from_f64_retain(0.5).unwrap());
        
        // 1.5 SOL 남음
        assert_eq!(buy_order.remaining_amount, Decimal::from_f64_retain(1.5).unwrap());
        assert_eq!(buy_order.filled_amount, Decimal::from_f64_retain(0.5).unwrap());
    }
    
    #[test]
    fn test_fifo_time_priority() {
        // Time Priority (FIFO) 테스트
        let pair = TradingPair::new("SOL".to_string(), "USDT".to_string());
        let mut orderbook = OrderBook::new(pair);
        let matcher = Matcher::new();
        
        // 같은 가격(100 USDT)에 여러 매도 주문
        orderbook.add_order(create_test_order(1, 100, "sell", "limit", Some(100.0), 1.0)); // 첫 번째
        orderbook.add_order(create_test_order(2, 101, "sell", "limit", Some(100.0), 1.0)); // 두 번째
        orderbook.add_order(create_test_order(3, 102, "sell", "limit", Some(100.0), 1.0)); // 세 번째
        
        // 매수 주문: 1 SOL
        let mut buy_order = create_test_order(4, 200, "buy", "limit", Some(100.0), 1.0);
        
        // 매칭 실행
        let matches = matcher.match_order(&mut buy_order, &mut orderbook);
        
        // FIFO: 첫 번째 주문(id=1)과 매칭되어야 함
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].sell_order_id, 1); // 가장 먼저 온 주문
        assert_eq!(matches[0].seller_id, 100);
    }
    
    /// 테스트: Self-Trade 방지 (매수)
    /// 
    /// 같은 user_id의 매수/매도 주문이 매칭되지 않는지 확인합니다.
    #[test]
    fn test_self_trade_prevention_buy() {
        let pair = TradingPair::new("SOL".to_string(), "USDT".to_string());
        let mut orderbook = OrderBook::new(pair);
        let matcher = Matcher::new();
        
        let user_id = 100;
        
        // 같은 유저의 매도 주문 추가
        orderbook.add_order(create_test_order(1, user_id, "sell", "limit", Some(100.0), 1.0));
        
        // 같은 유저의 매수 주문
        let mut buy_order = create_test_order(2, user_id, "buy", "limit", Some(100.0), 1.0);
        
        // 매칭 실행
        let matches = matcher.match_order(&mut buy_order, &mut orderbook);
        
        // Self-Trade 방지: 매칭되지 않아야 함
        assert_eq!(matches.len(), 0);
        
        // 매수 주문이 오더북에 남아있어야 함 (매칭되지 않았으므로)
        // 실제로는 매칭 실패 시 오더북에 추가되지 않지만, 여기서는 매칭 결과만 확인
    }
    
    /// 테스트: Self-Trade 방지 (매도)
    /// 
    /// 같은 user_id의 매도/매수 주문이 매칭되지 않는지 확인합니다.
    #[test]
    fn test_self_trade_prevention_sell() {
        let pair = TradingPair::new("SOL".to_string(), "USDT".to_string());
        let mut orderbook = OrderBook::new(pair);
        let matcher = Matcher::new();
        
        let user_id = 100;
        
        // 같은 유저의 매수 주문 추가
        orderbook.add_order(create_test_order(1, user_id, "buy", "limit", Some(100.0), 1.0));
        
        // 같은 유저의 매도 주문
        let mut sell_order = create_test_order(2, user_id, "sell", "limit", Some(100.0), 1.0);
        
        // 매칭 실행
        let matches = matcher.match_order(&mut sell_order, &mut orderbook);
        
        // Self-Trade 방지: 매칭되지 않아야 함
        assert_eq!(matches.len(), 0);
    }
    
    /// 테스트: 가격 우선순위 매칭
    /// 
    /// 더 좋은 가격의 주문이 먼저 매칭되는지 확인합니다.
    #[test]
    fn test_price_priority_matching() {
        let pair = TradingPair::new("SOL".to_string(), "USDT".to_string());
        let mut orderbook = OrderBook::new(pair);
        let matcher = Matcher::new();
        
        // 매도 호가: 100, 101, 102 (낮은 가격 우선)
        orderbook.add_order(create_test_order(1, 100, "sell", "limit", Some(102.0), 1.0));
        orderbook.add_order(create_test_order(2, 101, "sell", "limit", Some(101.0), 1.0));
        orderbook.add_order(create_test_order(3, 102, "sell", "limit", Some(100.0), 1.0));
        
        // 매수 주문: 3 SOL
        let mut buy_order = create_test_order(4, 200, "buy", "limit", Some(105.0), 3.0);
        
        // 매칭 실행
        let matches = matcher.match_order(&mut buy_order, &mut orderbook);
        
        // 가격 우선순위: 100 → 101 → 102 순으로 매칭되어야 함
        assert_eq!(matches.len(), 3);
        assert_eq!(matches[0].price, Decimal::from_f64_retain(100.0).unwrap()); // 가장 낮은 가격 먼저
        assert_eq!(matches[1].price, Decimal::from_f64_retain(101.0).unwrap());
        assert_eq!(matches[2].price, Decimal::from_f64_retain(102.0).unwrap());
    }
}