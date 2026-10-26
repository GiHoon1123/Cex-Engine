//! 실제 라우터·단일 벤치 워커의 입력/서비스 신원/명령 계약을 검증한다. WAL 확정은 별도 시험이다.
use super::*;
use crate::domains::cex::engine::{runtime::DepositCredit, Engine};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use uuid::Uuid;
fn body(id: Uuid, action: &str) -> Value {
    json!({"withdrawal_id":id,"user_id":3,"asset":"BTC","amount":"0.4","action":action})
}
fn request(value: impl ToString) -> Request<Body> {
    Request::post("/api/cex/withdrawals/reservations")
        .header(
            "x-engine-service-token",
            "isolated-engine-service-token-32-bytes",
        )
        .header("content-type", "application/json")
        .body(Body::from(value.to_string()))
        .unwrap()
}
async fn funded() -> (Router, HighPerformanceEngine) {
    let (router, engine) = app().await;
    engine
        .handle()
        .credit_deposit(DepositCredit {
            deposit_id: Uuid::new_v4(),
            user_id: 3,
            asset: "BTC".into(),
            amount: Decimal::ONE,
        })
        .await
        .unwrap();
    (router, engine)
}
async fn payload(response: axum::response::Response) -> Value {
    assert_eq!(response.headers()["cache-control"], "no-store");
    serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap()
}
#[tokio::test]
async fn withdrawal_http_reserve_and_release_are_exact_and_idempotent() {
    let (router, mut engine) = funded().await;
    let id = Uuid::new_v4();
    for (action, code) in [
        ("reserve", "DURABLY_RESERVED"),
        ("release", "DURABLY_RELEASED"),
    ] {
        for _ in 0..2 {
            let response = router
                .clone()
                .oneshot(request(body(id, action)))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                payload(response).await,
                json!({"code":code,"withdrawal_id":id,"action":action})
            );
        }
        let available = if action == "reserve" {
            Decimal::new(6, 1)
        } else {
            Decimal::ONE
        };
        assert_eq!(
            engine.handle().get_balance(3, "BTC").await.unwrap(),
            (available, Decimal::ONE - available)
        );
    }
    assert_eq!(
        router
            .oneshot(request(body(id, "reserve")))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    engine.stop().await.unwrap();
}
#[tokio::test]
async fn withdrawal_http_completion_cannot_be_released_or_reserved_again() {
    let (router, mut engine) = funded().await;
    let id = Uuid::new_v4();
    for action in ["reserve", "complete", "complete"] {
        assert_eq!(
            router
                .clone()
                .oneshot(request(body(id, action)))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }
    for action in ["release", "reserve"] {
        assert_eq!(
            router
                .clone()
                .oneshot(request(body(id, action)))
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );
    }
    assert_eq!(
        engine.handle().get_balance(3, "BTC").await.unwrap(),
        (Decimal::new(6, 1), Decimal::ZERO)
    );
    engine.stop().await.unwrap();
}
#[tokio::test]
async fn withdrawal_http_changed_original_never_spends_another_account() {
    let (router, mut engine) = funded().await;
    let id = Uuid::new_v4();
    assert_eq!(
        router
            .clone()
            .oneshot(request(body(id, "reserve")))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    for action in ["reserve", "release", "complete"] {
        for (field, value) in [
            ("user_id", json!(4)),
            ("asset", json!("USDT")),
            ("amount", json!("0.5")),
        ] {
            let mut changed = body(id, action);
            changed[field] = value;
            let response = router.clone().oneshot(request(changed)).await.unwrap();
            assert_eq!(response.status(), StatusCode::CONFLICT);
            assert_eq!(payload(response).await["code"], "DURABLY_REJECTED");
        }
    }
    assert_eq!(
        engine.handle().get_balance(3, "BTC").await.unwrap(),
        (Decimal::new(6, 1), Decimal::new(4, 1))
    );
    assert_eq!(
        engine.handle().get_balance(4, "BTC").await.unwrap(),
        (Decimal::ZERO, Decimal::ZERO)
    );
    engine.stop().await.unwrap();
}
#[tokio::test]
async fn withdrawal_http_rejects_malformed_unknown_and_duplicate_fields() {
    let (router, mut engine) = funded().await;
    let original = body(Uuid::new_v4(), "reserve");
    let mut extra = original.clone();
    extra["destination"] = json!("코어에 주소/서명 정책을 넣지 않는다");
    let mut invalid_action = original.clone();
    invalid_action["action"] = json!("refund");
    for value in [
        "{".into(),
        "{}".into(),
        extra.to_string(),
        invalid_action.to_string(),
        original
            .to_string()
            .replacen("\"action\":", "\"action\":\"complete\",\"action\":", 1),
    ] {
        let response = router.clone().oneshot(request(value)).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            payload(response).await["code"],
            "INVALID_WITHDRAWAL_COMMAND"
        );
    }
    assert_eq!(
        engine.handle().get_balance(3, "BTC").await.unwrap(),
        (Decimal::ONE, Decimal::ZERO)
    );
    engine.stop().await.unwrap();
}
#[tokio::test]
async fn withdrawal_http_never_rounds_or_coerces_the_amount() {
    let (router, mut engine) = funded().await;
    for amount in [
        json!(1),
        json!(null),
        json!("0"),
        json!("-1"),
        json!("+1"),
        json!(" 1"),
        json!("1 "),
        json!("1e-1"),
        json!(".1"),
        json!("1."),
        json!("1_0"),
        json!("NaN"),
        json!("0.0000000001"),
        json!("1000000000000000000000"),
        json!("1".repeat(65)),
    ] {
        let mut command = body(Uuid::new_v4(), "reserve");
        command["amount"] = amount;
        let response = router.clone().oneshot(request(command)).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    assert_eq!(
        engine.handle().get_balance(3, "BTC").await.unwrap(),
        (Decimal::ONE, Decimal::ZERO)
    );
    engine.stop().await.unwrap();
}
#[tokio::test]
async fn withdrawal_http_requires_the_service_identity() {
    let (router, mut engine) = funded().await;
    for token in [None, Some("틀린 서비스 신원")] {
        let mut req = request(body(Uuid::new_v4(), "reserve"));
        req.headers_mut().remove("x-engine-service-token");
        if let Some(token) = token {
            req.headers_mut()
                .insert("x-engine-service-token", token.parse().unwrap());
        }
        assert_eq!(
            router.clone().oneshot(req).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(
        engine.handle().get_balance(3, "BTC").await.unwrap(),
        (Decimal::ONE, Decimal::ZERO)
    );
    engine.stop().await.unwrap();
}
#[tokio::test]
async fn withdrawal_http_rejects_invalid_identity_and_oversized_body() {
    let (router, mut engine) = funded().await;
    for (field, value) in [
        ("withdrawal_id", json!(Uuid::nil())),
        ("user_id", json!(0)),
        ("user_id", json!(u64::MAX)),
        ("asset", json!("btc")),
        ("asset", json!("BTC ")),
    ] {
        let mut changed = body(Uuid::new_v4(), "reserve");
        changed[field] = value;
        assert_eq!(
            router
                .clone()
                .oneshot(request(changed))
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
    let mut large = body(Uuid::new_v4(), "reserve");
    large["asset"] = json!("A".repeat(5000));
    assert_eq!(
        router.oneshot(request(large)).await.unwrap().status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    engine.stop().await.unwrap();
}
