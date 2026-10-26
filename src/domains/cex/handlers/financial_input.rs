//! 내부 금융 HTTP의 사용자·자산·정확한 양수 문자열 검증을 공유한다.
use crate::domains::cex::engine::amount;
use rust_decimal::Decimal;

/// 설치된 Decimal의 정확 변환과 엔진 단위를 함께 검사한다. 지수·부호·반올림을 허용하지 않는다.
pub(super) fn validated(user_id: u64, asset: &str, text: &str) -> Option<Decimal> {
    if user_id == 0
        || user_id > i64::MAX as u64
        || asset.is_empty()
        || asset.len() > 128
        || !asset
            .bytes()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
        || text.is_empty()
        || text.len() > 64
    {
        return None;
    }
    let mut parts = text.split('.');
    let integral = parts.next()?;
    let fractional = parts.next();
    if integral.is_empty()
        || !integral.bytes().all(|c| c.is_ascii_digit())
        || parts.next().is_some()
        || fractional.is_some_and(|s| s.is_empty() || !s.bytes().all(|c| c.is_ascii_digit()))
    {
        return None;
    }
    let value = Decimal::from_str_exact(text).ok()?;
    if value <= Decimal::ZERO || amount::units(value).is_err() {
        return None;
    }
    Some(value)
}
