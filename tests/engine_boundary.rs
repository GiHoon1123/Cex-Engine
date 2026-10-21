//! 엔진에 외부 시세 구독과 자동 주문 생성 책임이 다시 들어오지 않도록 검사한다.
//! 실제 주문 매칭 동작은 별도의 단일 워커 회귀 테스트가 검증한다.
use std::path::Path;

#[test]
fn engine_does_not_own_a_trading_bot() {
    // 비활성 파일도 새 기능의 구현 근거로 오인될 수 있어 디렉터리 자체를 허용하지 않는다.
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(!root.join("src/domains/bot").exists());
    let modules = std::fs::read_to_string(root.join("src/domains/mod.rs")).unwrap();
    assert!(!modules.contains("mod bot"));
}

#[test]
fn engine_has_no_external_market_websocket_client_dependency() {
    // 직접 의존성만 검사한다. 지갑이나 HTTP 라이브러리의 전이 의존성까지
    // 제거하면 실제 필요한 기능을 훼손하므로 잠금 파일 전체를 금지하지 않는다.
    let manifest = include_str!("../Cargo.toml");
    for dependency in ["tokio-tungstenite =", "hyper-util =", "tokio-util =", "url ="] {
        assert!(!manifest.contains(dependency), "{dependency}");
    }
}
