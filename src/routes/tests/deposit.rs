//! 이 모듈의 벤치 워커 시험은 HTTP 입력·서비스 신원·멱등 계약만 확인한다.
//! 실제 저널 확정·손상·복구는 별도의 기록 워커 회귀에서 검증한다.
use super::*;
use crate::domains::cex::engine::Engine;
use rust_decimal::Decimal;
use serde_json::{json, Value};
use uuid::Uuid;

fn body(id: Uuid) -> Value {
    json!({"deposit_id":id,"user_id":3,"asset":"BTC","amount":"0.12345678"})
}
fn request(value: impl ToString) -> Request<Body> {
    Request::post("/api/cex/deposits/credits")
        .header(
            "x-engine-service-token",
            "isolated-engine-service-token-32-bytes",
        )
        .header("content-type", "application/json")
        .body(Body::from(value.to_string()))
        .unwrap()
}
async fn payload(response: axum::response::Response) -> Value {
    assert_eq!(response.headers()["cache-control"], "no-store");
    serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap()
}
#[tokio::test]
async fn exact_string_amount_is_forwarded_once_without_rounding() {
    let (router, mut engine) = app().await;
    let id = Uuid::new_v4();
    for _ in 0..2 {
        let response = router.clone().oneshot(request(body(id))).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            payload(response).await,
            json!({"code":"DURABLY_CREDITED","deposit_id":id})
        );
    }
    assert_eq!(
        engine.handle().get_balance(3, "BTC").await.unwrap(),
        (
            Decimal::from_str_exact("0.12345678").unwrap(),
            Decimal::ZERO
        )
    );
    engine.stop().await.unwrap();
}
#[tokio::test]
async fn equivalent_decimal_text_has_the_same_financial_identity() {
    let (router, mut engine) = app().await;
    let mut value = body(Uuid::new_v4());
    for amount in ["0.12345678", "00.123456780"] {
        value["amount"] = json!(amount);
        assert_eq!(
            router
                .clone()
                .oneshot(request(&value))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }
    assert_eq!(
        engine.handle().get_balance(3, "BTC").await.unwrap().0,
        Decimal::from_str_exact("0.12345678").unwrap()
    );
    engine.stop().await.unwrap();
}
#[tokio::test]
async fn changed_content_is_rejected_without_increasing_the_balance() {
    let (router, mut engine) = app().await;
    let original = body(Uuid::new_v4());
    assert_eq!(
        router
            .clone()
            .oneshot(request(&original))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    for (field, value) in [
        ("amount", json!("1")),
        ("user_id", json!(4)),
        ("asset", json!("USDT")),
    ] {
        let mut changed = original.clone();
        changed[field] = value;
        let response = router.clone().oneshot(request(changed)).await.unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(payload(response).await["code"], "DURABLY_REJECTED");
    }
    assert_eq!(
        engine.handle().get_balance(3, "BTC").await.unwrap().0,
        Decimal::from_str_exact("0.12345678").unwrap()
    );
    engine.stop().await.unwrap();
}
#[tokio::test]
async fn malformed_or_unknown_fields_cannot_enter_the_worker() {
    let (router, mut engine) = app().await;
    let original = body(Uuid::new_v4());
    let mut extra = original.clone();
    extra["available_delta"] = json!("1");
    for value in [
        "{}".to_owned(),
        "{".to_owned(),
        extra.to_string(),
        original
            .to_string()
            .replace(r#""amount":"#, r#""amount":"1","amount":"#),
    ] {
        let response = router.clone().oneshot(request(value)).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(payload(response).await["code"], "INVALID_DEPOSIT_CREDIT");
    }
    assert_eq!(
        engine.handle().get_balance(3, "BTC").await.unwrap().0,
        Decimal::ZERO
    );
    engine.stop().await.unwrap();
}
#[tokio::test]
async fn numeric_json_and_non_decimal_strings_are_rejected() {
    let (router, mut engine) = app().await;
    for amount in [
        json!(0.1),
        json!(null),
        json!("1e-8"),
        json!(" 1"),
        json!("+1"),
        json!("-1"),
        json!("1_000"),
        json!("NaN"),
        json!("1."),
        json!(".1"),
        json!("1.2.3"),
    ] {
        let mut value = body(Uuid::new_v4());
        value["amount"] = amount;
        assert_eq!(
            router
                .clone()
                .oneshot(request(value))
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        engine.handle().get_balance(3, "BTC").await.unwrap().0,
        Decimal::ZERO
    );
    engine.stop().await.unwrap();
}
#[tokio::test]
async fn zero_excess_precision_and_overflow_never_get_rounded() {
    let (router, mut engine) = app().await;
    for amount in [
        "0",
        "0.0000000001",
        "0.123456789123456789123456789123456789",
        "1000000000000000000000",
        "99999999999999999999999999999999999999999999999999999999999999999",
    ] {
        let mut value = body(Uuid::new_v4());
        value["amount"] = json!(amount);
        assert_eq!(
            router
                .clone()
                .oneshot(request(value))
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        engine.handle().get_balance(3, "BTC").await.unwrap().0,
        Decimal::ZERO
    );
    engine.stop().await.unwrap();
}
#[tokio::test]
async fn invalid_identity_and_asset_are_rejected_before_dispatch() {
    let (router, mut engine) = app().await;
    for (field, value) in [
        ("deposit_id", json!(Uuid::nil())),
        ("user_id", json!(0)),
        ("user_id", json!(u64::MAX)),
        ("asset", json!("")),
        ("asset", json!(" BTC")),
        ("asset", json!("btc")),
        ("asset", json!("BTC\n")),
        ("asset", json!("X".repeat(129))),
    ] {
        let mut request_body = body(Uuid::new_v4());
        request_body[field] = value;
        assert_eq!(
            router
                .clone()
                .oneshot(request(request_body))
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        engine.handle().get_balance(3, "BTC").await.unwrap().0,
        Decimal::ZERO
    );
    engine.stop().await.unwrap();
}
#[tokio::test]
async fn only_the_platform_service_identity_can_send_credit() {
    let (router, mut engine) = app().await;
    for key in [None, Some("wrong-service-token")] {
        let mut req = Request::post("/api/cex/deposits/credits")
            .header("authorization", "Bearer customer-token")
            .header("content-type", "application/json");
        if let Some(key) = key {
            req = req.header("x-engine-service-token", key);
        }
        assert_eq!(
            router
                .clone()
                .oneshot(
                    req.body(Body::from(body(Uuid::new_v4()).to_string()))
                        .unwrap()
                )
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(
        engine.handle().get_balance(3, "BTC").await.unwrap().0,
        Decimal::ZERO
    );
    engine.stop().await.unwrap();
}
#[tokio::test]
async fn oversized_body_is_rejected_without_echoing_its_content() {
    let (router, mut engine) = app().await;
    let mut value = body(Uuid::new_v4());
    value["amount"] = json!("7".repeat(5000));
    let response = router.oneshot(request(value)).await.unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(payload(response).await["code"], "INVALID_DEPOSIT_CREDIT");
    assert_eq!(
        engine.handle().get_balance(3, "BTC").await.unwrap().0,
        Decimal::ZERO
    );
    engine.stop().await.unwrap();
}
#[tokio::test]
async fn closed_worker_is_this_attempt_not_accepted() {
    let (router, mut engine) = app().await;
    engine.stop().await.unwrap();
    let id = Uuid::new_v4();
    let response = router.oneshot(request(body(id))).await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        payload(response).await,
        json!({"code":"NOT_ACCEPTED","deposit_id":id})
    );
}
