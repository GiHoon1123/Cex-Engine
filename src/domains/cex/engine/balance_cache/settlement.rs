//! 한 체결의 두 자산 이동을 검증한 뒤 실행용 캐시에 함께 반영한다.
//! DB·주문장·이벤트를 포함하는 트랜잭션이 아니며 외부 부작용을 만들지 않는다.

use super::{Balance, BalanceCache};
use crate::domains::cex::engine::types::MatchResult;
use anyhow::{ensure, Context, Result};
use num_bigint::BigInt;
use rust_decimal::Decimal;
use std::collections::HashMap;

// 현재 저장 계약 NUMERIC(30,9)을 따른다. 자산별 설정·추측한 반올림 정책은 만들지 않는다.
const SCALE: u32 = 9;
const FACTOR: i128 = 1_000_000_000;
const MAX_UNITS: i128 = 999_999_999_999_999_999_999_999_999_999;

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

        let price = units(trade.price)?;
        let amount = units(trade.amount)?;
        // Decimal.checked_mul도 유효 자릿수가 넘으면 반올림할 수 있다.
        // 최대 30자리인 두 정수의 곱을 기존 BigInt로 정확히 계산하고 9자리 단위로 나눈다.
        let product = BigInt::from(price) * BigInt::from(amount);
        ensure!(
            &product % FACTOR == BigInt::from(0),
            "Quote value requires rounding"
        );
        let quote =
            i128::try_from(product / FACTOR).context("Quote value exceeds settlement range")?;
        ensure!(
            quote > 0 && quote <= MAX_UNITS,
            "Quote value exceeds NUMERIC(30,9)"
        );

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

/// 지수나 큰 숫자를 무제한 확장하지 않고 고정 9자리 단위의 정수로 검사한다.
fn units(value: Decimal) -> Result<i128> {
    let value = value.normalize();
    ensure!(
        value >= Decimal::ZERO && value.scale() <= SCALE,
        "Invalid settlement precision or negative balance"
    );
    let amount = value
        .mantissa()
        .checked_mul(10i128.pow(SCALE - value.scale()))
        .context("Settlement value exceeds integer range")?;
    ensure!(
        amount <= MAX_UNITS,
        "Settlement value exceeds NUMERIC(30,9)"
    );
    Ok(amount)
}

/// 최종 값은 저장 컬럼과 Decimal 모두에 정확히 표현돼야 한다. 임의 반올림은 없다.
fn decimal(amount: i128) -> Result<Decimal> {
    let text = format!("{}.{:09}", amount / FACTOR, amount % FACTOR);
    // 단위 정수의 끝 0은 실제 소수 정밀도가 아니다. 표현상 0을 제거해 정확히
    // 표현 가능한 큰 정수가 불필요한 scale 때문에 거부되지 않게 한다.
    Decimal::from_str_exact(text.trim_end_matches('0').trim_end_matches('.'))
        .context("Settlement balance is not exactly representable as Decimal")
}
