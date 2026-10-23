# 엔진의 비활성 봇과 외부 시세 클라이언트 제거

자동 주문 생성 책임을 플랫폼으로 모으고, 엔진에 남은 비활성 코드를 제거한다. 실행 중인 봇을 끄는 변경과 사용하지 않는 소스 삭제를 구분한다.

## 문제와 기존 동작

`src/domains/bot/services/orderbook_sync.rs` 한 개가 남아 있었다. `domains/mod.rs`에서 선언하지 않아 빌드되지 않았고, 참조하는 봇 관리자·바이낸스 클라이언트도 존재하지 않았다. 현재 동작하는 구현으로 오인해 복구하거나 중복 개발할 위험이 있었다. 플랫폼에는 이미 일반 주문 서비스를 호출하는 봇 구현이 있다.

## 선택과 트레이드오프

- 비활성 파일을 삭제한다. 엔진 단독 봇 실행 기능을 제공하지 않는 대신 자동 주문의 인증·예약·중복 처리 경로를 플랫폼에서 관리한다.
- 기존 [LMAX 소비자 역할 분리](https://lmax-exchange.github.io/disruptor/user-guide/)를 따른다. 핵심 계산과 외부 통신 책임을 나눈다. 해당 라이브러리를 도입한 것은 아니다.
- 소스·테스트·벤치의 사용 여부를 확인한 후 직접 의존성 `tokio-tungstenite`, `tokio-util`, `url`, `hyper`, `hyper-util`을 제거했다. 지갑·스왑·HTTP 등 다른 기능이 사용하는 전이 의존성은 유지한다.
- 이미 사용하는 플랫폼 봇의 활성화나 바이낸스 가격 구독을 이번 삭제 검증에서 시작하지 않는다.

## 관련 코드와 문법

[도메인 선언](../../src/domains/mod.rs)의 `pub mod`는 Rust가 컴파일할 모듈을 선언한다. 디렉터리가 존재한다는 이유만으로 코드가 실행되지는 않는다. TypeScript에서도 진입점에서 가져오지 않는 파일이 실행되지 않는 것과 비슷하지만 Rust는 명시적인 모듈 트리를 사용한다.

### 현재 코드로 연결해 읽기

봇 제거 뒤 현재 도메인 선언은 다음과 같다. 아래는 2026-10-23 현재 [실제 소스](../../src/domains/mod.rs#L2)의 연속 행 발췌다. 메서드 일부일 수 있으므로 독립 실행 예제로 사용하지 않고 앞뒤 검증과 함께 읽는다. 과거 사이클의 코드/측정 시점은 본문의 기록을 유지한다.

```rust
pub mod wallet;
pub mod swap;
pub mod cex;
```

`pub mod`는 공개할 Rust 모듈을 선언한다. 봇 모듈의 삭제를 보여주는 최소 발췌이며, 플랫폼의 봇 활성화나 실행을 추가한 것이 아니다. 남은 도메인의 책임은 각 소스에서 확인한다.

## 작성한 테스트

[엔진 경계 검사](../../tests/engine_boundary.rs):

- `engine_does_not_own_a_trading_bot`: 봇 디렉터리와 모듈 선언이 없는지 검사한다.
- `engine_has_no_external_market_websocket_client_dependency`: 제거한 외부 클라이언트 직접 의존성의 재도입을 검사한다. 전이 의존성을 금지하는 검사는 아니다.

기존 `ownership_worker_moves_state_and_matches_direct_core_results`와 `ownership_worker_failure_preserves_orderbook_and_balance`를 포함한 단일 워커 관련 테스트도 실행했다. 실제 명령 루프의 매칭·잔액·실패 보존과 큐 포화를 확인한다.

## 실행 명령과 결과

2026-10-21, Rust 1.91.0, 외부 DB·Kafka·봇 없이 실행했다.

```sh
cargo test --offline --test engine_boundary
cargo test --locked --lib ownership_
```

각각 2개, 6개 통과, 실패 0개다. 기존 미사용 코드 경고 및 `solana-client`·`sqlx-core`의 향후 Rust 호환 경고는 남아 있다. 전체 의존성 재생성 명령은 기존 잠금 버전의 철회된 전이 의존성 때문에 실패해 사용하지 않았다. 기존 잠금 파일을 기준으로 필요한 의존성만 정리한 테스트 명령은 성공했다. 실행 중인 로컬 서비스는 재시작하지 않았으며 성능 수치는 측정하지 않았다.
