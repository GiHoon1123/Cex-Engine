//! 한 체결의 두 자산 이동을 검증한 뒤 실행용 캐시에 함께 반영한다.
//! DB·주문장·이벤트를 포함하는 트랜잭션이 아니며 외부 부작용을 만들지 않는다.

use super::super::amount::{decimal, quote, units, MAX_UNITS};
use super::{Balance, BalanceCache};
use crate::domains::cex::engine::types::MatchResult;
use anyhow::{ensure, Context, Result};
use rust_decimal::Decimal;
use std::collections::HashMap;

impl BalanceCache {
    /// 성공 시 양쪽 자산이 모두 바뀌고, Result 오류 시 행 생성까지 포함해 그대로다.
    /// &mut self와 호출자의 Executor 락이 다른 작업의 중간 상태 관찰을 막는다.
    pub fn settle_trade(&mut self, trade: &MatchResult) -> Result<()> {
        ensure!(
            trade.buyer_id > 0 && trade.seller_id > 0,
            "Invalid settlement user"
        );
        ensure!(
            trade.buy_order_id > 0
                && trade.sell_order_id > 0
                && trade.buy_order_id != trade.sell_order_id,
            "Invalid settlement order ids"
        );
        for asset in [&trade.base_mint, &trade.quote_mint] {
            ensure!(
                !asset.is_empty() && asset.trim() == asset.as_str() && asset.len() <= 255,
                "Invalid settlement asset"
            );
        }
        ensure!(
            trade.base_mint != trade.quote_mint,
            "Settlement assets must differ"
        );
        ensure!(
            trade.price > Decimal::ZERO && trade.amount > Decimal::ZERO,
            "Settlement price and amount must be positive"
        );

        let amount = units(trade.amount)?;
        let quote = units(quote(trade.price, trade.amount)?)?;
        let changes = [
            (trade.buyer_id, &trade.quote_mint, 0, -quote),
            (trade.seller_id, &trade.quote_mint, quote, 0),
            (trade.seller_id, &trade.base_mint, 0, -amount),
            (trade.buyer_id, &trade.base_mint, amount, 0),
        ];
        let mut staged: HashMap<(u64, String), (i128, i128)> = HashMap::with_capacity(4);
        for (user, asset, available_delta, locked_delta) in changes {
            let key = (user, asset.clone());
            if !staged.contains_key(&key) {
                // 없는 수신 자산은 성공할 때만 생성한다. 실패한 검증이 원본에 0 잔액을
                // 추가하지 않도록 get_balance_mut를 쓰지 않는다. 같은 고객의 두 주문은
                // 동일 키에 변화량을 합친다. 이 계산은 자전거래 방지 정책이 아니다.
                let balance = self
                    .balances
                    .get(&key)
                    .cloned()
                    .unwrap_or_else(Balance::new);
                staged.insert(
                    key.clone(),
                    (units(balance.available)?, units(balance.locked)?),
                );
            }
            let balance = staged.get_mut(&key).expect("staged balance exists");
            balance.0 = balance
                .0
                .checked_add(available_delta)
                .context("Available overflow")?;
            balance.1 = balance
                .1
                .checked_add(locked_delta)
                .context("Locked overflow")?;
            ensure!(
                balance.0 >= 0 && balance.1 >= 0,
                "Insufficient settlement balance"
            );
            ensure!(
                balance.0 <= MAX_UNITS && balance.1 <= MAX_UNITS,
                "Settlement balance exceeds NUMERIC(30,9)"
            );
        }
        // 마지막 수신 계정의 Decimal 변환이 실패해도 앞 계정이 바뀌지 않게,
        // 모든 변환을 먼저 끝낸다. HashMap 전체를 복제하지 않고 최대 네 키만 복사한다.
        let prepared = staged
            .into_iter()
            .map(|(key, (available, locked))| {
                Ok((
                    key,
                    Balance {
                        available: decimal(available)?,
                        locked: decimal(locked)?,
                    },
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        for (key, balance) in prepared {
            self.balances.insert(key, balance);
        }
        Ok(())
    }
}
