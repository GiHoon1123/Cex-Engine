//! 가격·시간 순서대로 후보를 계산한다. 원본 주문장·잔액·이벤트는 수정하지 않는다.
//! 확정은 호출자가 같은 주문장 락을 유지한 채 전체 잔액 검증 후 수행한다.
use super::{
    amount,
    orderbook::OrderBook,
    types::{MatchResult, OrderEntry},
};
use anyhow::{ensure, Result};
use rust_decimal::Decimal;
use std::collections::{BTreeMap, HashMap, VecDeque};

pub struct Matcher;

/// 변경된 maker만 보관한다. 전체 주문장이나 가격 큐를 복제하지 않는다.
pub(crate) struct MatchPlan {
    pub order: OrderEntry,
    pub matches: Vec<MatchResult>,
    pub total_amount: Decimal,
    pub total_quote: Decimal,
    makers: BTreeMap<Decimal, HashMap<u64, OrderEntry>>,
    maker_buy: bool,
}

impl MatchPlan {
    /// 계획 중 읽은 원본을 다른 작업이 바꾸지 못하게 호출자가 write 락을 유지한다.
    /// 이 단계에는 금액 계산·DB·채널·Result 오류가 없다. 일반적인 검증 실패는
    /// commit에 도달하기 전에 끝난다. 프로세스 종료·할당 실패의 영속성은 별도다.
    pub fn commit_book(&mut self, book: &mut OrderBook) {
        let side = if self.maker_buy {
            &mut book.buy_orders
        } else {
            &mut book.sell_orders
        };
        side.apply_prepared_orders(std::mem::take(&mut self.makers));
        if self.order.order_side == "limit" && self.order.remaining_amount > Decimal::ZERO {
            book.add_order(self.order.clone());
        }
    }
}

impl Matcher {
    pub fn new() -> Self {
        Self
    }

    pub(crate) fn plan_order(&self, incoming: &OrderEntry, book: &OrderBook) -> Result<MatchPlan> {
        validate(incoming, true)?;
        let mut plan = MatchPlan {
            order: incoming.clone(),
            matches: Vec::new(),
            total_amount: Decimal::ZERO,
            total_quote: Decimal::ZERO,
            makers: BTreeMap::new(),
            maker_buy: incoming.order_type == "sell",
        };
        if plan.maker_buy {
            self.plan_levels(&mut plan, book.buy_orders.orders.iter().rev())?;
        } else {
            self.plan_levels(&mut plan, book.sell_orders.orders.iter())?;
        }
        Ok(plan)
    }

    fn plan_levels<'a>(
        &self,
        plan: &mut MatchPlan,
        levels: impl Iterator<Item = (&'a Decimal, &'a VecDeque<OrderEntry>)>,
    ) -> Result<()> {
        for (&price, queue) in levels {
            if let Some(limit) = plan.order.price {
                if (plan.maker_buy && price < limit) || (!plan.maker_buy && price > limit) {
                    break;
                }
            }
            for original in queue {
                if plan
                    .order
                    .remaining_quote_amount
                    .map_or(plan.order.remaining_amount == Decimal::ZERO, |budget| {
                        budget == Decimal::ZERO
                    })
                {
                    return Ok(());
                }
                validate(original, false)?;
                ensure!(
                    original.price == Some(price)
                        && original.order_side == "limit"
                        && (original.order_type == "buy") == plan.maker_buy
                        && original.base_mint == plan.order.base_mint
                        && original.quote_mint == plan.order.quote_mint,
                    "Maker does not match its orderbook level"
                );
                ensure!(
                    original.id != plan.order.id,
                    "Incoming order already exists in orderbook"
                );
                // 기존 동일 고객 비체결 방침을 유지하되 큐를 회전하지 않는다.
                // 자기 주문만 있어도 유한 순회로 끝나고 다른 주문의 FIFO가 보존된다.
                if original.user_id == plan.order.user_id {
                    continue;
                }
                let quantity = match plan.order.remaining_quote_amount {
                    Some(budget) => {
                        amount::quantity_for_budget(budget, price, original.remaining_amount)?
                    }
                    None => plan.order.remaining_amount.min(original.remaining_amount),
                };
                if quantity == Decimal::ZERO {
                    continue;
                }
                let quote = amount::quote(price, quantity)?;
                let mut maker = original.clone();
                maker.remaining_amount = amount::subtract(maker.remaining_amount, quantity)?;
                maker.filled_amount = amount::add(maker.filled_amount, quantity)?;
                if let Some(budget) = plan.order.remaining_quote_amount {
                    plan.order.remaining_quote_amount = Some(amount::subtract(budget, quote)?);
                    plan.order.amount = amount::add(plan.order.amount, quantity)?;
                } else {
                    plan.order.remaining_amount =
                        amount::subtract(plan.order.remaining_amount, quantity)?;
                }
                plan.order.filled_amount = amount::add(plan.order.filled_amount, quantity)?;
                plan.total_amount = amount::add(plan.total_amount, quantity)?;
                plan.total_quote = amount::add(plan.total_quote, quote)?;
                let (buy, sell) = if plan.maker_buy {
                    (&maker, &plan.order)
                } else {
                    (&plan.order, &maker)
                };
                plan.matches.push(MatchResult {
                    buy_order_id: buy.id,
                    sell_order_id: sell.id,
                    buyer_id: buy.user_id,
                    seller_id: sell.user_id,
                    price,
                    amount: quantity,
                    base_mint: plan.order.base_mint.clone(),
                    quote_mint: plan.order.quote_mint.clone(),
                });
                ensure!(
                    plan.makers
                        .entry(price)
                        .or_default()
                        .insert(maker.id, maker)
                        .is_none(),
                    "Duplicate maker order id"
                );
            }
        }
        Ok(())
    }
}

/// HTTP 경계와 별도로 내부 후보의 불변조건을 검사한다. 손상된 maker를 건너뛰어
/// 더 불리한 가격으로 거래하거나, 잘못된 신규 명령으로 예약을 변경하지 않는다.
fn validate(order: &OrderEntry, incoming: bool) -> Result<()> {
    ensure!(
        order.id > 0
            && order.id <= i64::MAX as u64
            && order.user_id > 0
            && order.user_id <= i64::MAX as u64,
        "Invalid order identity"
    );
    ensure!(
        matches!(order.order_type.as_str(), "buy" | "sell")
            && matches!(order.order_side.as_str(), "limit" | "market"),
        "Invalid order kind"
    );
    for asset in [&order.base_mint, &order.quote_mint] {
        ensure!(
            !asset.is_empty() && asset.trim() == asset.as_str() && asset.len() <= 255,
            "Invalid order asset"
        );
    }
    ensure!(
        order.base_mint != order.quote_mint,
        "Identical order assets"
    );
    if incoming {
        ensure!(
            order.filled_amount == Decimal::ZERO,
            "Incoming order is not fresh"
        );
    }
    if order.order_side == "market" && order.order_type == "buy" {
        let budget = order
            .quote_amount
            .ok_or_else(|| anyhow::anyhow!("Missing market budget"))?;
        ensure!(
            budget > Decimal::ZERO
                && order.price.is_none()
                && order.amount == Decimal::ZERO
                && order.remaining_amount == Decimal::ZERO
                && order.remaining_quote_amount == Some(budget),
            "Invalid market buy"
        );
        amount::units(budget)?;
    } else {
        ensure!(
            order.amount > Decimal::ZERO
                && order.remaining_amount > Decimal::ZERO
                && order.quote_amount.is_none()
                && order.remaining_quote_amount.is_none(),
            "Invalid quantity order"
        );
        ensure!(
            amount::add(order.filled_amount, order.remaining_amount)? == order.amount,
            "Inconsistent order quantity"
        );
        if order.order_side == "limit" {
            let price = order
                .price
                .ok_or_else(|| anyhow::anyhow!("Missing limit price"))?;
            ensure!(price > Decimal::ZERO, "Invalid limit price");
            amount::quote(price, order.amount)?;
        } else {
            ensure!(order.price.is_none(), "Market sell has price");
        }
    }
    Ok(())
}
