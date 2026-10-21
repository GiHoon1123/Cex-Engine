//! 시작 검증과 주문 접수에서 DB 금액을 반올림 없이 읽는다.

use rust_decimal::Decimal;
use sqlx::{postgres::PgRow, Row};

/// 호출 SQL은 대상 컬럼을 `::text AS 컬럼명`으로 읽어야 한다.
/// SQLx 0.6 NUMERIC 바이너리 변환은 96비트 초과 계수에서 panic할 수 있다.
/// 문자열과 Decimal 검사 API로 변환해 실패를 컬럼명 있는 DB 오류로 반환한다.
pub(crate) fn read_decimal(row: &PgRow, column: &str) -> Result<Decimal, sqlx::Error> {
    parse(&row.try_get::<String, _>(column)?, column)
}

pub(crate) fn read_optional_decimal(
    row: &PgRow,
    column: &str,
) -> Result<Option<Decimal>, sqlx::Error> {
    row.try_get::<Option<String>, _>(column)?
        .map(|value| parse(&value, column))
        .transpose()
}

fn parse(value: &str, column: &str) -> Result<Decimal, sqlx::Error> {
    // NUMERIC(30,9)의 표시용 0은 값의 정밀도가 아니다. 소수 끝의 0만
    // 제거한다. 정수 1000의 끝을 제거하면 1이 되므로 소수점이 있을 때만 처리한다.
    let normalized = if value.contains('.') {
        value.trim_end_matches('0').trim_end_matches('.')
    } else {
        value
    };
    Decimal::from_str_exact(normalized).map_err(|source| sqlx::Error::ColumnDecode {
        index: column.into(),
        source: Box::new(source),
    })
}
