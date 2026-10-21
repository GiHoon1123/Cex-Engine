//! 한 신규 주문의 후보 매칭·전체 정산·잔여 예약을 검증하고 메모리에 확정한다.
//! 제품 워커와 벤치가 같은 함수를 쓴다. DB·Kafka·WAL을 여기서 호출하지 않는다.
use crate::domains::cex::engine::{
    amount,
    balance_cache::BalanceCache,
    executor::Executor,
    matcher::Matcher,
    orderbook::OrderBook,
    types::{MatchResult, OrderEntry, TradingPair},
};
use anyhow::Result;
use rust_decimal::Decimal;
use std::collections::HashMap;

pub(crate) struct AppliedSubmission {
    pub order: OrderEntry,
    pub matches: Vec<MatchResult>,
    pub total_quote: Decimal,
    pub total_amount: Decimal,
}

/// 호출자는 함수 시작부터 종료까지 orderbooks → Executor 순서의 두 락을 유지한다.
/// 모든 Result 실패는 원본 변경 전에 일어난다. 원본 전체 복제·사후 역이체는 없다.
pub(crate) fn apply(
    order: OrderEntry,
    books: &mut HashMap<TradingPair, OrderBook>,
    matcher: &Matcher,
    executor: &mut Executor,
) -> Result<AppliedSubmission> {
    let pair = TradingPair::new(order.base_mint.clone(), order.quote_mint.clone());
    let empty = OrderBook::new(pair.clone());
    let book = books.get(&pair).unwrap_or(&empty);
    let mut plan = matcher.plan_order(&order, book)?;
    let buy = order.order_type == "buy";
    let refund_mint = if buy {
        &order.quote_mint
    } else {
        &order.base_mint
    };
    let reserved = if buy {
        match order.price {
            Some(price) => amount::quote(price, order.amount)?,
            None => order.quote_amount.expect("validated market buy budget"),
        }
    } else {
        order.amount
    };
    let mut prepared = BalanceCache::with_capacity(1 + plan.matches.len() * 4);
    let source = executor.balance_cache();
    prepared.copy_account_from(source, order.user_id, refund_mint);
    for trade in &plan.matches {
        for (user, asset) in [
            (trade.buyer_id, &trade.base_mint),
            (trade.buyer_id, &trade.quote_mint),
            (trade.seller_id, &trade.base_mint),
            (trade.seller_id, &trade.quote_mint),
        ] {
            prepared.copy_account_from(source, user, asset);
        }
    }
    // 플랫폼 DB 예약은 이미 커밋돼 있다. 여기서는 기존 캐시 스냅샷의 실행용
    // 예약만 계산한다. 실패해도 플랫폼 예약·영속 접수를 지우거나 해제하지 않는다.
    prepared.lock_balance(order.user_id, refund_mint, reserved)?;
    for trade in &plan.matches {
        prepared.settle_trade(trade)?;
    }
    let remaining = if order.order_side == "market" {
        Decimal::ZERO
    } else if buy {
        amount::quote(order.price.unwrap(), plan.order.remaining_amount)?
    } else {
        plan.order.remaining_amount
    };
    let used = if buy {
        plan.total_quote
    } else {
        plan.total_amount
    };
    let refund = amount::subtract(amount::subtract(reserved, used)?, remaining)?;
    if refund > Decimal::ZERO {
        prepared.unlock_balance(order.user_id, refund_mint, refund)?;
    }
    // 이 아래에는 Result 오류·DB·채널이 없다. 두 락을 함께 쥔 호출자만
    // 완성된 다음 상태를 게시한다. OOM·프로세스 종료의 내구성은 보장하지 않는다.
    plan.commit_book(
        books
            .entry(pair.clone())
            .or_insert_with(|| OrderBook::new(pair)),
    );
    executor.balance_cache_mut().commit_prepared(prepared);
    Ok(AppliedSubmission {
        order: plan.order,
        matches: plan.matches,
        total_quote: plan.total_quote,
        total_amount: plan.total_amount,
    })
}
