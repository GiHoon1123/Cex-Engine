//! 확정 저널을 읽는 출력 워커 전용 호가 집계다. 코어 상태를 공유하지 않는다.
//! 금융 결과를 다시 계산하지 않고 확정된 체결 수량으로 공개용 가격별 잔량만 갱신한다.
use super::{
    journal::{store::Record, Command, Outcome},
    output::OutputBatch,
};
use crate::domains::cex::engine::{amount, types::TradingPair};
use anyhow::{ensure, Result};
use num_bigint::BigInt;
use rust_decimal::Decimal;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};

/// 식별자·순번·금액을 문자열로 보낸다. 자바스크립트 숫자의 정밀도 손실을 막는다.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct MarketSnapshot {
    pub event_type: &'static str,
    pub event_id: String,
    pub engine_id: String,
    pub sequence: String,
    pub previous_sequence: String,
    pub base_mint: String,
    pub quote_mint: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub bids: Vec<[String; 2]>,
    pub asks: Vec<[String; 2]>,
    pub trades: Vec<MarketTrade>,
}
#[derive(Clone, Debug, Serialize)]
pub(crate) struct MarketTrade {
    pub id: String,
    pub price: String,
    pub amount: String,
    pub side: String,
}
#[derive(Default)]
struct Book {
    bids: BTreeMap<Decimal, BigInt>,
    asks: BTreeMap<Decimal, BigInt>,
    sequence: u64,
}
/// 주문 식별자는 내부 차감 검증에만 사용하고 사용자 식별자는 공개하지 않는다.
struct Resting {
    pair: TradingPair,
    buy: bool,
    price: Decimal,
    remaining: Decimal,
}
#[derive(Default)]
pub(super) struct Projection {
    orders: HashMap<u64, Resting>,
    books: HashMap<TradingPair, Book>,
    pub latest: HashMap<TradingPair, MarketSnapshot>,
}
/// 가격 레벨의 고정 소수 단위를 정확한 십진 문자열로 바꾼다.
fn quantity_text(units: &BigInt) -> String {
    let mut text = units.to_string();
    if text.len() <= 9 {
        text = format!("{:0>10}", text);
    }
    text.insert(text.len() - 9, '.');
    text.trim_end_matches('0').trim_end_matches('.').to_string()
}
impl Projection {
    fn reduce(&mut self, id: u64, quantity: Decimal) -> Result<()> {
        let order = self
            .orders
            .get_mut(&id)
            .ok_or_else(|| anyhow::anyhow!("Missing resting market order {id}"))?;
        order.remaining = amount::subtract(order.remaining, quantity)?;
        let book = self
            .books
            .get_mut(&order.pair)
            .expect("resting book exists");
        let levels = if order.buy {
            &mut book.bids
        } else {
            &mut book.asks
        };
        let level = levels.get_mut(&order.price).expect("resting price exists");
        let change = BigInt::from(amount::units(quantity)?);
        ensure!(*level >= change, "Negative public price level");
        *level -= change;
        if *level == BigInt::from(0) {
            levels.remove(&order.price);
        }
        if order.remaining.is_zero() {
            self.orders.remove(&id);
        }
        Ok(())
    }
    /// 전달 커서 이전 이력도 읽어 집계 상태를 재구성한다. 기존 금융 WAL의 형식이나
    /// 확정 결과를 변경하지 않으므로 과거 SOL 기록도 원래 거래쌍으로 복원한다.
    pub fn apply(&mut self, record: &Record, engine_id: &str) -> Result<Option<MarketSnapshot>> {
        let mut trades = Vec::new();
        let pair = match (&record.command, &record.outcome) {
            (
                Command::Submit(order),
                Outcome::Applied(Some(OutputBatch::Submitted {
                    trades: outputs, ..
                })),
            ) => {
                let pair = TradingPair::new(order.base_mint.clone(), order.quote_mint.clone());
                let mut filled = Decimal::ZERO;
                for (index, output) in outputs.iter().enumerate() {
                    let t = &output.trade;
                    self.reduce(
                        if order.order_type == "buy" {
                            t.sell_order_id
                        } else {
                            t.buy_order_id
                        },
                        t.amount,
                    )?;
                    filled = amount::add(filled, t.amount)?;
                    trades.push(MarketTrade {
                        id: format!("{engine_id}:{}:{index}", record.sequence),
                        price: t.price.to_string(),
                        amount: t.amount.to_string(),
                        side: order.order_type.clone(),
                    });
                }
                if order.order_side == "limit" {
                    let remaining = amount::subtract(order.amount, filled)?;
                    if remaining > Decimal::ZERO {
                        let price = order.price.expect("accepted limit price");
                        let book = self.books.entry(pair.clone()).or_default();
                        let buy = order.order_type == "buy";
                        let levels = if buy { &mut book.bids } else { &mut book.asks };
                        let level = levels.entry(price).or_default();
                        // 여러 사용자의 잔량 합은 개별 잔액 컬럼 상한보다 클 수 있다.
                        // 기존 정수 라이브러리로 1e-9 단위를 합해 반올림을 없앤다.
                        *level += BigInt::from(amount::units(remaining)?);
                        ensure!(
                            self.orders
                                .insert(
                                    order.id,
                                    Resting {
                                        pair: pair.clone(),
                                        buy,
                                        price,
                                        remaining
                                    }
                                )
                                .is_none(),
                            "Duplicate resting order"
                        );
                    }
                }
                pair
            }
            (_, Outcome::Applied(Some(OutputBatch::Cancelled(order)))) => {
                let pair = TradingPair::new(order.base_mint.clone(), order.quote_mint.clone());
                self.reduce(order.id, order.remaining_amount)?;
                pair
            }
            _ => return Ok(None),
        };
        let book = self.books.entry(pair.clone()).or_default();
        let event = MarketSnapshot {
            event_type: "market_snapshot",
            event_id: format!("{engine_id}:{}:market", record.sequence),
            engine_id: engine_id.into(),
            sequence: record.sequence.to_string(),
            previous_sequence: book.sequence.to_string(),
            base_mint: pair.base_mint.clone(),
            quote_mint: pair.quote_mint.clone(),
            timestamp: record.timestamp,
            // 상위 20개 전체 스냅샷을 택해 누락 뒤에도 최신 호가로 바로 재동기화한다.
            // 델타보다 전송량이 크지만 코어의 복사 비용과 클라이언트의 델타 병합 상태가 없다.
            bids: book
                .bids
                .iter()
                .rev()
                .take(20)
                .map(|(p, q)| [p.to_string(), quantity_text(q)])
                .collect(),
            asks: book
                .asks
                .iter()
                .take(20)
                .map(|(p, q)| [p.to_string(), quantity_text(q)])
                .collect(),
            trades,
        };
        book.sequence = record.sequence;
        self.latest.insert(pair, event.clone());
        Ok(Some(event))
    }
}

#[cfg(test)]
mod tests {
    use super::super::{engine::EngineState, journal};
    use super::*;
    use crate::domains::cex::engine::types::OrderEntry;
    fn order(id: u64, user: u64, buy: bool, quantity: i64) -> OrderEntry {
        OrderEntry {
            fee_rate: rust_decimal::Decimal::ZERO,
            id,
            user_id: user,
            order_type: if buy { "buy" } else { "sell" }.into(),
            order_side: "limit".into(),
            base_mint: "BTC".into(),
            quote_mint: "USDT".into(),
            price: Some(Decimal::from(100)),
            amount: Decimal::from(quantity),
            quote_amount: None,
            filled_amount: Decimal::ZERO,
            remaining_amount: Decimal::from(quantity),
            remaining_quote_amount: None,
            created_at: chrono::Utc::now(),
        }
    }
    fn state() -> EngineState {
        let mut state = EngineState::new();
        for (user, asset, quantity) in [(1, "USDT", 10000), (2, "BTC", 100), (3, "BTC", 100)] {
            state.executor.balance_cache_mut().set_balance(
                user,
                asset,
                Decimal::from(quantity),
                Decimal::ZERO,
            );
        }
        state
    }
    fn record(state: &mut EngineState, sequence: u64, command: Command) -> Record {
        let outcome = journal::apply(state, &command);
        Record {
            sequence,
            timestamp: chrono::Utc::now(),
            command,
            outcome,
        }
    }
    #[test]
    fn market_projection_tracks_real_partial_fill_cancel_and_replay() -> Result<()> {
        let mut core = state();
        let mut projection = Projection::default();
        let engine = uuid::Uuid::new_v4().to_string();
        let commands = vec![
            Command::Submit(order(1, 2, false, 3)),
            Command::Submit(order(2, 1, true, 1)),
            Command::Cancel {
                order_id: 1,
                user_id: 2,
                pair: TradingPair::new("BTC".into(), "USDT".into()),
            },
        ];
        let records: Vec<_> = commands
            .into_iter()
            .enumerate()
            .map(|(i, c)| record(&mut core, i as u64 + 1, c))
            .collect();
        let first = projection.apply(&records[0], &engine)?.unwrap();
        assert_eq!(first.asks, vec![["100".to_string(), "3".to_string()]]);
        let second = projection.apply(&records[1], &engine)?.unwrap();
        assert_eq!(second.asks[0][1], "2");
        assert_eq!(second.previous_sequence, "1");
        assert_eq!(second.trades[0].side, "buy");
        assert_eq!(second.trades[0].id, format!("{engine}:2:0"));
        let last = projection.apply(&records[2], &engine)?.unwrap();
        assert!(last.asks.is_empty());
        assert!(last.bids.is_empty());
        let mut recovered = Projection::default();
        for r in &records {
            recovered.apply(r, &engine)?;
        }
        assert_eq!(
            serde_json::to_value(recovered.latest.values().next().unwrap())?,
            serde_json::to_value(last)?
        );
        Ok(())
    }
    #[test]
    fn market_projection_skips_duplicates_and_rejections_and_preserves_other_pairs() -> Result<()> {
        let mut core = state();
        let mut projection = Projection::default();
        let id = uuid::Uuid::new_v4().to_string();
        let original = order(1, 2, false, 3);
        assert!(projection
            .apply(
                &record(&mut core, 1, Command::Submit(original.clone())),
                &id
            )?
            .is_some());
        assert!(projection
            .apply(&record(&mut core, 2, Command::Submit(original)), &id)?
            .is_none());
        assert!(projection
            .apply(
                &record(&mut core, 3, Command::Submit(order(3, 99, true, 1))),
                &id
            )?
            .is_none());
        core.executor
            .balance_cache_mut()
            .set_balance(3, "ETH", Decimal::from(10), Decimal::ZERO);
        let mut eth = order(4, 3, false, 2);
        eth.base_mint = "ETH".into();
        let event = projection
            .apply(&record(&mut core, 4, Command::Submit(eth)), &id)?
            .unwrap();
        assert_eq!(event.previous_sequence, "0");
        assert_eq!(projection.latest.len(), 2);
        assert_eq!(
            projection.latest[&TradingPair::new("BTC".into(), "USDT".into())].asks[0][1],
            "3"
        );
        Ok(())
    }
    #[test]
    fn market_projection_market_buy_never_leaves_resting_budget() -> Result<()> {
        let mut core = state();
        let mut projection = Projection::default();
        let id = uuid::Uuid::new_v4().to_string();
        projection.apply(
            &record(&mut core, 1, Command::Submit(order(1, 2, false, 1))),
            &id,
        )?;
        let mut buy = order(2, 1, true, 0);
        buy.order_side = "market".into();
        buy.price = None;
        buy.quote_amount = Some(Decimal::from(150));
        buy.remaining_quote_amount = buy.quote_amount;
        let event = projection
            .apply(&record(&mut core, 2, Command::Submit(buy)), &id)?
            .unwrap();
        assert_eq!(event.trades.len(), 1);
        assert!(event.bids.is_empty());
        assert!(event.asks.is_empty());
        assert!(projection.orders.is_empty());
        Ok(())
    }
    #[test]
    fn market_projection_depth_is_sorted_bounded_and_promotes_hidden_levels() -> Result<()> {
        let mut core = state();
        let mut projection = Projection::default();
        let id = uuid::Uuid::new_v4().to_string();
        for n in 1..=25 {
            let mut sell = order(n, 2, false, 1);
            sell.price = Some(Decimal::from(100 + n));
            projection.apply(&record(&mut core, n, Command::Submit(sell)), &id)?;
        }
        let pair = TradingPair::new("BTC".into(), "USDT".into());
        assert_eq!(projection.latest[&pair].asks.len(), 20);
        assert_eq!(projection.latest[&pair].asks[0][0], "101");
        let event = projection
            .apply(
                &record(
                    &mut core,
                    26,
                    Command::Cancel {
                        order_id: 1,
                        user_id: 2,
                        pair,
                    },
                ),
                &id,
            )?
            .unwrap();
        assert_eq!(event.asks[0][0], "102");
        assert_eq!(event.asks[19][0], "121");
        Ok(())
    }

    #[test]
    fn market_projection_aggregates_beyond_one_account_limit_without_rounding() -> Result<()> {
        let mut core = state();
        let mut projection = Projection::default();
        let id = uuid::Uuid::new_v4().to_string();
        let large = Decimal::from_str_exact("900000000000000000000")?;
        for user in [2, 3] {
            core.executor
                .balance_cache_mut()
                .set_balance(user, "BTC", large, Decimal::ZERO);
            let mut sell = order(user, user, false, 1);
            sell.price = Some(Decimal::ONE);
            sell.amount = large;
            sell.remaining_amount = large;
            projection.apply(&record(&mut core, user, Command::Submit(sell)), &id)?;
        }
        let pair = TradingPair::new("BTC".into(), "USDT".into());
        assert_eq!(
            projection.latest[&pair].asks[0][1],
            "1800000000000000000000"
        );
        let event = projection
            .apply(
                &record(
                    &mut core,
                    4,
                    Command::Cancel {
                        order_id: 2,
                        user_id: 2,
                        pair,
                    },
                ),
                &id,
            )?
            .unwrap();
        assert_eq!(event.asks[0][1], "900000000000000000000");
        Ok(())
    }
}
