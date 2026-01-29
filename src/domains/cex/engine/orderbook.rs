// =====================================================
// OrderBook - 인메모리 호가창
// =====================================================
// 역할: 매수/매도 주문을 가격별로 관리하는 자료구조
// 
// 핵심 설계:
// 1. BTreeMap으로 가격별 정렬 (O(log n))
// 2. VecDeque로 같은 가격 내 Time Priority (FIFO)
// 3. 매수/매도 분리하여 best bid/ask 빠른 조회
//
// Price-Time Priority:
// - 먼저 가격으로 매칭 (높은 매수 vs 낮은 매도)
// - 같은 가격이면 시간 순서 (먼저 온 주문 우선)
// =====================================================

use std::collections::{BTreeMap, VecDeque};
use rust_decimal::Decimal;
use crate::domains::cex::engine::types::{OrderEntry, TradingPair};

/// 호가창 한쪽 방향 (매수 또는 매도)
/// BTreeMap { 100.5 -> [주문1, 주문2], 100.0 -> [주문3], 99.5 -> [주문4] }
/// VecDeque 사용 이유: front()로 가장 오래된 주문, push_back()으로 추가, O(1) 삽입/삭제
pub struct OrderBookSide {
    /// 가격별 주문 큐 (Key: 가격, Value: 해당 가격의 주문들)
    pub orders: BTreeMap<Decimal, VecDeque<OrderEntry>>,
    /// 전체 주문 수 (캐싱)
    total_orders: usize,
}

impl OrderBookSide {
    /// 새로운 OrderBookSide 생성
    pub fn new() -> Self {
        Self {
            orders: BTreeMap::new(),
            total_orders: 0,
        }
    }
    
    /// 주문 추가 - 주문 가격의 큐 맨 뒤에 추가 (Time Priority)
    pub fn add_order(&mut self, order: OrderEntry) {
        let price = order.price.expect("Limit order must have price");
        self.orders.entry(price).or_insert_with(VecDeque::new).push_back(order);
        self.total_orders += 1;
    }
    
    /// 주문 제거 (주문 ID로) - O(log n + m) where n=가격 레벨 수, m=해당 가격의 주문 수
    pub fn remove_order(&mut self, order_id: u64, price: Decimal) -> Option<OrderEntry> {
        if let Some(queue) = self.orders.get_mut(&price) {
            if let Some(pos) = queue.iter().position(|o| o.id == order_id) {
                let order = queue.remove(pos)?;
                if queue.is_empty() {
                    self.orders.remove(&price);
                }
                self.total_orders -= 1;
                return Some(order);
            }
        }
        None
    }
    
    /// 최선 가격 조회 (매수: 최고가, 매도: 최저가) - O(1)
    pub fn get_best_price(&self, is_buy: bool) -> Option<Decimal> {
        if is_buy {
            self.orders.keys().next_back().copied() // 매수: 가장 높은 가격
        } else {
            self.orders.keys().next().copied() // 매도: 가장 낮은 가격
        }
    }
    
    /// 특정 가격의 주문들 조회 (불변)
    pub fn get_orders_at_price(&self, price: &Decimal) -> Option<&VecDeque<OrderEntry>> {
        self.orders.get(price)
    }
    
    /// 특정 가격의 주문들 조회 (가변)
    pub fn get_orders_at_price_mut(&mut self, price: &Decimal) -> Option<&mut VecDeque<OrderEntry>> {
        self.orders.get_mut(price)
    }
    
    /// 전체 주문 수
    pub fn total_orders(&self) -> usize {
        self.total_orders
    }
    
    /// 가격 레벨 수
    pub fn price_levels(&self) -> usize {
        self.orders.len()
    }
    
    /// 호가창이 비었는지 확인
    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }
    
    /// 모든 가격 레벨 순회 (Iterator)
    pub fn iter(&self) -> impl Iterator<Item = (&Decimal, &VecDeque<OrderEntry>)> {
        self.orders.iter()
    }
}

/// 완전한 호가창 (매수 + 매도)
/// buy_orders: { 100.5 -> [매수1], 100.0 -> [매수2] }  (높은 가격 우선)
/// sell_orders: { 101.0 -> [매도1], 101.5 -> [매도2] }  (낮은 가격 우선)
/// Spread = 101.0 - 100.5 = 0.5 USDT
pub struct OrderBook {
    /// 거래 쌍 (예: SOL/USDT)
    trading_pair: TradingPair,
    /// 매수 호가 (가격 내림차순)
    pub buy_orders: OrderBookSide,
    /// 매도 호가 (가격 오름차순)
    pub sell_orders: OrderBookSide,
}

impl OrderBook {
    /// 새 OrderBook 생성
    pub fn new(trading_pair: TradingPair) -> Self {
        Self {
            trading_pair,
            buy_orders: OrderBookSide::new(),
            sell_orders: OrderBookSide::new(),
        }
    }
    
    /// 주문 추가 - 매수/매도에 따라 적절한 side에 추가
    pub fn add_order(&mut self, order: OrderEntry) {
        match order.order_type.as_str() {
            "buy" => self.buy_orders.add_order(order),
            "sell" => self.sell_orders.add_order(order),
            _ => {} // 잘못된 타입 무시
        }
    }
    
    /// 주문 제거
    pub fn remove_order(&mut self, order_id: u64, order_type: &str, price: Decimal) -> Option<OrderEntry> {
        match order_type {
            "buy" => self.buy_orders.remove_order(order_id, price),
            "sell" => self.sell_orders.remove_order(order_id, price),
            _ => None,
        }
    }
    
    /// 최선 매수 가격 (Best Bid) - 가장 높은 매수 가격
    pub fn get_best_bid(&self) -> Option<Decimal> {
        self.buy_orders.get_best_price(true)
    }
    
    /// 최선 매도 가격 (Best Ask) - 가장 낮은 매도 가격
    pub fn get_best_ask(&self) -> Option<Decimal> {
        self.sell_orders.get_best_price(false)
    }
    
    /// 스프레드 (Best Ask - Best Bid)
    pub fn get_spread(&self) -> Option<Decimal> {
        match (self.get_best_ask(), self.get_best_bid()) {
            (Some(ask), Some(bid)) => Some(ask - bid),
            _ => None,
        }
    }
    
    /// 중간 가격 (Mid Price) - (Best Ask + Best Bid) / 2
    pub fn get_mid_price(&self) -> Option<Decimal> {
        match (self.get_best_ask(), self.get_best_bid()) {
            (Some(ask), Some(bid)) => Some((ask + bid) / Decimal::TWO),
            _ => None,
        }
    }
    
    /// 매수 호가 조회 (상위 N개)
    pub fn get_buy_orders(&self, depth: usize) -> Vec<(Decimal, Decimal)> {
        self.buy_orders.orders.iter()
            .rev() // 높은 가격부터
            .take(depth)
            .map(|(price, queue)| {
                let total_amount: Decimal = queue.iter().map(|o| o.remaining_amount).sum();
                (*price, total_amount)
            })
            .collect()
    }
    
    /// 매도 호가 조회 (상위 N개)
    pub fn get_sell_orders(&self, depth: usize) -> Vec<(Decimal, Decimal)> {
        self.sell_orders.orders.iter()
            .take(depth)
            .map(|(price, queue)| {
                let total_amount: Decimal = queue.iter().map(|o| o.remaining_amount).sum();
                (*price, total_amount)
            })
            .collect()
    }
    
    /// 거래 쌍
    pub fn trading_pair(&self) -> &TradingPair {
        &self.trading_pair
    }
    
    /// 전체 매수 주문 수
    pub fn total_buy_orders(&self) -> usize {
        self.buy_orders.total_orders()
    }
    
    /// 전체 매도 주문 수
    pub fn total_sell_orders(&self) -> usize {
        self.sell_orders.total_orders()
    }
    
    /// 호가창이 비었는지 확인
    pub fn is_empty(&self) -> bool {
        self.buy_orders.is_empty() && self.sell_orders.is_empty()
    }
}