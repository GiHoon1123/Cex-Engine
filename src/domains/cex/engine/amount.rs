//! 현재 NUMERIC(30,9)와 Decimal에 정확히 표현되는 양수·잔액 연산.
//! 반올림 없이 실패를 반환하며, 후보 계산과 캐시 정산이 같은 계약을 사용한다.
use anyhow::{ensure, Context, Result};
use num_bigint::BigInt;
use rust_decimal::Decimal;

// 현재 저장 계약 NUMERIC(30,9)을 따른다. 자산별 설정·추측한 반올림 정책은 만들지 않는다.
const SCALE: u32 = 9;
const FACTOR: i128 = 1_000_000_000;
pub(crate) const MAX_UNITS: i128 = 999_999_999_999_999_999_999_999_999_999;

/// 지수나 큰 숫자를 무제한 확장하지 않고 고정 9자리 단위의 정수로 검사한다.
pub(crate) fn units(value: Decimal) -> Result<i128> {
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
pub(crate) fn decimal(amount: i128) -> Result<Decimal> {
    let text = format!("{}.{:09}", amount / FACTOR, amount % FACTOR);
    // 단위 정수의 끝 0은 실제 소수 정밀도가 아니다. 표현상 0을 제거해 정확히
    // 표현 가능한 큰 정수가 불필요한 scale 때문에 거부되지 않게 한다.
    Decimal::from_str_exact(text.trim_end_matches('0').trim_end_matches('.'))
        .context("Settlement balance is not exactly representable as Decimal")
}

/// 정확한 체결 대금. 유효한 입력도 곱의 최종 정밀도·범위가 넘으면 거부한다.
pub(crate) fn quote(price: Decimal, amount: Decimal) -> Result<Decimal> {
    let product = BigInt::from(units(price)?) * BigInt::from(units(amount)?);
    ensure!(
        &product % FACTOR == BigInt::from(0),
        "Quote value requires rounding"
    );
    let value = i128::try_from(product / FACTOR).context("Quote value exceeds settlement range")?;
    ensure!(value <= MAX_UNITS, "Quote exceeds NUMERIC(30,9)");
    decimal(value)
}

pub(crate) fn add(left: Decimal, right: Decimal) -> Result<Decimal> {
    let value = units(left)?
        .checked_add(units(right)?)
        .context("Amount addition overflow")?;
    ensure!(value <= MAX_UNITS, "Amount exceeds NUMERIC(30,9)");
    decimal(value)
}

pub(crate) fn subtract(left: Decimal, right: Decimal) -> Result<Decimal> {
    let value = units(left)? - units(right)?;
    ensure!(value >= 0, "Amount would become negative");
    decimal(value)
}

/// 예산·상대 잔량 안에서, 수량과 대금을 모두 1e-9 단위로 정확히 표현할 최대 수량.
/// 부동소수점·Decimal 나눗셈 반올림을 사용하지 않는다. 수량은 예산 기반 주문의
/// 계산 결과이므로 더 작은 정확한 수량을 선택하고 지출하지 못한 예산은 반환한다.
pub(crate) fn quantity_for_budget(
    budget: Decimal,
    price: Decimal,
    remaining: Decimal,
) -> Result<Decimal> {
    let price = units(price)?;
    ensure!(price > 0, "Invalid maker price");
    let maximum =
        (BigInt::from(units(budget)?) * FACTOR / price).min(BigInt::from(units(remaining)?));
    let maximum = i128::try_from(maximum)?;
    // p*a가 FACTOR로 나눠떨어지려면 a는 FACTOR/gcd(p,FACTOR)의 배수다.
    // 두 수가 1e9 이하인 유클리드 알고리즘으로 고정 계약의 최소 수량 단위를 구한다.
    let (mut a, mut b) = (price % FACTOR, FACTOR);
    while a != 0 {
        let remainder = b % a;
        b = a;
        a = remainder;
    }
    let step = FACTOR / b;
    decimal(maximum / step * step)
}
