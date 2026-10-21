//! 요청을 Decimal로 읽는 순간 반올림하면 원본 대조가 무의미해진다.
use super::*;

fn request() -> serde_json::Value {
    serde_json::json!({"order_id":1,"user_id":101,"order_type":"buy","order_side":"limit",
        "base_mint":"SOL","price":"100","amount":"2"})
}

#[test]
fn request_decimals_reject_precision_loss_in_every_amount_field() {
    for field in ["price", "amount", "quote_amount"] {
        for value in [
            "2.00000000000000000000000000001",
            "0.00000000000000000000000000001",
            "999999999999999999999.123456789",
        ] {
            let mut body = request();
            body[field] = serde_json::json!(value);
            assert!(
                serde_json::from_value::<CreateOrderRequest>(body).is_err(),
                "{field}={value}"
            );
        }
    }
}

#[test]
fn request_decimals_preserve_exact_strings_and_optional_absence() {
    let mut body = request();
    body["amount"] = serde_json::json!("79228162514264337593.5439503350");
    let parsed: CreateOrderRequest = serde_json::from_value(body).unwrap();
    assert_eq!(
        parsed.amount,
        Some(Decimal::from_str_exact("79228162514264337593.543950335").unwrap())
    );
    assert!(parsed.quote_amount.is_none());
    let mut body = request();
    body["amount"] = serde_json::Value::Null;
    let parsed: CreateOrderRequest = serde_json::from_value(body).unwrap();
    assert!(parsed.amount.is_none());
}

#[test]
fn request_decimals_require_strings_instead_of_json_floating_point() {
    let mut body = request();
    body["price"] = serde_json::json!(0.1);
    assert!(serde_json::from_value::<CreateOrderRequest>(body).is_err());
}
