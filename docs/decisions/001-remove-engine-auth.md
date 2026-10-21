# 엔진에서 사용자 인증 제거하기

사용자 인증은 Java 플랫폼에 모으고, Rust 엔진에서는 주문 처리에 필요한 코드에 집중하도록 인증 도메인을 제거했다. 이 글에서는 삭제 범위를 결정한 이유, 기존 HTTP 동작을 확인한 테스트, 변경을 이해하는 데 필요한 Rust 문법을 설명한다.

## 문제와 기존 구조

로그인과 토큰 발급은 `Cex-Platform-Service`에서 제공한다. Rust의 `/api/auth/*` 경로는 이미 메인 라우터에서 비활성화되어 있었지만, `AppState::new`는 매번 `AuthState`와 `JwtService`를 생성했다. 인증용 저장소, 비밀번호 해싱, 토큰 발급 코드도 컴파일 대상에 남아 있었다.

HTTP 경로를 등록하지 않는 것과 코드를 컴파일하지 않는 것은 다르다. Rust는 `mod` 선언을 따라 하위 모듈을 컴파일하므로, 라우트 등록만 주석 처리해도 인증 모듈의 타입과 의존성은 계속 관리해야 했다.

변경 전 의존 관계는 다음과 같았다.

```text
AppState → AuthState → AuthService / JwtService
인증 핸들러 → AuthState
인증 미들웨어 → JwtService
비활성 지갑 HTTP 핸들러 → 인증 미들웨어
인증 저장소 → 인증 모델
```

## 기존 제품에서 참고한 책임 구분

Coinbase Exchange는 [매칭 엔진 문서](https://docs.cdp.coinbase.com/exchange/concepts/matching-engine)에서 가격·시간 우선 매칭과 주문 상태를 설명하고, [REST 인증 문서](https://docs.cdp.coinbase.com/exchange/rest-api/authentication)에서 비공개 API의 서명과 권한을 설명한다.

이 구분을 참고해 이 프로젝트에서는 Java 플랫폼이 사용자 인증을 담당하고, Rust 엔진은 사용자 ID를 포함한 거래 명령을 처리하도록 책임을 나눴다. 이는 이 프로젝트의 설계 결정이다. 위 공개 문서가 Coinbase 내부 서비스 배치나 구현 언어까지 설명하는 것은 아니다.

## 선택과 트레이드오프

| 선택지 | 이점 | 비용과 제약 | 결정 |
| --- | --- | --- | --- |
| 비활성 인증 코드를 계속 보관 | 기존 구현을 다시 연결하기 쉽다 | 실제 사용하지 않는 코드와 인증 의존성을 계속 관리해야 한다 | 제외 |
| `auth/services`만 삭제 | 직접 수정하는 파일이 적다 | 핸들러·미들웨어·상태 초기화가 삭제한 타입을 참조해 컴파일이 깨진다 | 제외 |
| 인증 도메인과 사용하지 않는 연결 경로 삭제 | 인증 책임과 엔진의 의존 관계가 명확해진다 | Rust에서 독립적으로 로그인·토큰 발급 기능을 다시 사용할 수 없고, Java 인증 경계에 의존한다 | 채택 |

서비스 간 인증을 새로 구현하지는 않았다. 현재 엔진은 요청의 `user_id`를 받으며, 호출자가 Java 플랫폼인지를 검증하지 않는다. 따라서 이번 변경을 접근 통제 구현으로 해석하면 안 된다. 운영에서 엔진을 외부에 노출하려면 서비스 접근 제한을 별도로 설계해야 한다.

## 삭제한 코드와 남은 책임

- `src/domains/auth/`: 서비스, 상태, 핸들러, 라우터, 모델 전체를 삭제했다.
- `src/shared/database/repositories/auth/`: Rust 인증 서비스에서만 사용하는 저장소를 삭제했다.
- `src/shared/middleware/`와 `src/shared/errors/auth_error.rs`: 사용자 토큰 검증과 인증 오류 타입을 삭제했다.
- `src/domains/wallet/handlers/`와 `src/domains/wallet/routes.rs`: 삭제한 인증 미들웨어에 의존하는 비활성 HTTP 경로를 삭제했다. 인증 검사를 생략한 대체 핸들러는 만들지 않았다.
- [AppState](../../src/shared/services/app_state.rs): `AuthState`, `JwtService`, 엔진 내부 JWT 서명 키와 초기화를 제거했다.
- [메인 라우터](../../src/routes/mod.rs)와 [서버 진입점](../../src/main.rs): 삭제한 인증·지갑 HTTP 경로의 주석과 Swagger 참조를 정리했다.
- [주문 핸들러](../../src/domains/cex/handlers/order_handler.rs): 실제 동작과 다르던 JWT 인증 설명을 수정했다.
- [Cargo.toml](../../Cargo.toml): 인증 전용 직접 의존성 `argon2`, `jsonwebtoken`, `sha2`, `rand`를 제거하고 잠금 파일을 갱신했다. 다른 라이브러리가 필요로 하는 간접 의존성은 Cargo가 유지한다.

지갑 서비스·모델과 스왑 기능의 나머지 정리는 별도 작업이다. 현재 변경은 인증 의존성을 끊는 데 필요한 지갑 HTTP 경로까지 포함한다. 공유 DB의 `users`와 `refresh_tokens` 테이블은 Java에서 사용하며 주문 테이블도 사용자를 참조하므로, 기존 SQL 파일과 데이터는 삭제하지 않았다.

## 관련 Rust 코드 이해하기

### 모듈 선언과 가져오기

```rust
pub mod cex;
use crate::domains::cex::routes::create_cex_router;
```

`pub mod cex;`는 `cex` 하위 모듈을 컴파일 대상에 포함하고 외부에 공개한다. `use`는 이미 선언된 항목을 현재 파일에서 짧은 이름으로 사용할 수 있게 한다. TypeScript의 `export`와 `import`에 대응해 생각할 수 있지만, Rust의 `mod`는 컴파일러가 소스 모듈을 찾는 구조도 정의한다. 폴더를 삭제하면 그 폴더를 가리키는 `mod`, `use`, `pub use`도 함께 정리해야 한다.

자세한 규칙은 [Rust 공식 모듈 문서](https://doc.rust-lang.org/book/ch07-02-defining-modules-to-control-scope-and-privacy.html)에서 확인할 수 있다.

### 공유 상태와 오류 전달

[AppState](../../src/shared/services/app_state.rs)는 TypeScript 백엔드의 의존성 주입 컨테이너처럼 서비스와 엔진을 연결한다.

```rust
let wallet_state = WalletState::new(db.clone())?;
let engine_instance = HighPerformanceEngine::new(db.clone());
let engine = Arc::new(Mutex::new(engine_instance));
let cex_state = CexState::new(db.clone(), engine.clone());
```

`db.clone()`은 DB 전체나 개별 연결을 복사하지 않고 연결 풀의 공유 핸들을 복제한다. `Arc<T>`는 여러 곳에서 같은 값을 소유하도록 참조 수를 관리한다. `engine.clone()`도 엔진을 새로 만들지 않고 같은 엔진의 공유 핸들을 복제한다.

`Mutex<T>`는 공유 값에 대한 접근을 한 번에 하나로 제한한다. TypeScript의 객체 참조 공유와 달리, Rust는 여러 스레드의 접근과 소유권을 타입으로 표현한다. `?`는 서비스 생성 결과가 오류이면 호출자에게 오류를 전달해 초기화를 중단한다.

### 테스트에서만 포함하는 코드

```rust
#[cfg(test)]
mod tests;
```

`#[cfg(test)]`는 테스트를 빌드할 때만 해당 모듈을 포함한다. `#[tokio::test]`는 비동기 테스트에 필요한 Tokio 실행 환경을 제공한다. [테스트 코드](../../src/routes/tests.rs)는 실제 Axum 라우터에 Tower의 `oneshot`으로 요청한다. HTTP 포트를 열거나 요청 결과를 가짜로 대체하지 않는다.

## 작성한 테스트

회귀 테스트는 모두 [src/routes/tests.rs](../../src/routes/tests.rs)에 있다. SQLx의 `connect_lazy`로 DB 연결을 미루고, 기존 Bench 엔진으로 Kafka·저장 워커를 만들지 않는다. 테스트는 DB 쿼리를 수행하기 전의 라우팅과 입력 검증을 확인하므로 PostgreSQL·Kafka·Solana 서버 없이 실행할 수 있다.

테스트 도구는 이미 설치된 Tower의 `util` 기능을 개발 의존성에서 활성화했다. 별도 HTTP 테스트 프레임워크 없이 실제 라우터를 검사할 수 있는 대신, 포트를 통한 통신과 외부 저장소 연동은 이 테스트로 확인할 수 없다. 이 차이를 보완하기 위해 서버 재시작 후 실제 HTTP 응답도 별도로 확인했다.

| 테스트 함수 | 확인하는 동작 | 기대 결과 |
| --- | --- | --- |
| `health_check_remains_available` | 인증 도메인 제거 후 헬스체크 | 200, 본문 `status`가 `ok` |
| `removed_auth_routes_return_not_found` | 토큰을 보내도 signup·signin·refresh·logout·me 경로가 없음 | 5개 경로 모두 404 |
| `retired_wallet_routes_return_not_found` | 비활성 지갑 핸들러를 공개 API로 다시 연결하지 않음 | 6개 경로 모두 404 |
| `trading_routes_still_validate_request_bodies` | 주문 생성과 잔고 동기화 라우트에서 필수 필드를 검사 | 빈 JSON 요청에 각각 422 |
| `cancellation_still_requires_user_id` | 주문 취소의 사용자 ID 필수 검사 | 400, `Missing user_id query parameter` |

401은 인증 실패를 뜻한다. 삭제한 경로에서는 인증에 실패하는 것이 아니라 경로 자체가 없어야 하므로 404를 검사한다. 반대로 남겨야 할 거래 명령에서는 404 대신 기존 입력 오류 응답을 검사한다.

기존 단위 테스트의 컴파일을 막던 오류도 먼저 수정했다. [bench_submit_direct](../../src/domains/cex/engine/runtime/engine.rs)는 `process_submit_order` 호출에 Kafka 인자를 누락했고, [Kafka 테스트 이벤트](../../src/shared/kafka/producer.rs)는 필수 잔고 스냅샷 8개를 누락했다. `#[ignore]` 테스트도 컴파일은 되므로, 실행을 건너뛰는 것으로는 누락된 필드를 해결할 수 없다.

## 실행 명령과 결과

검증일은 2026년 10월 21일이다. macOS arm64, Rust 1.91.0 환경에서 엔진 루트를 기준으로 실행했다.

```bash
# 추가한 HTTP 경로 회귀 테스트
cargo test --locked --lib routes::tests

# 기존 UDP 메시지의 직렬화 회귀 확인
cargo test --locked --lib test_binary_serialization

# 기존 통합 테스트를 포함한 테스트 코드 컴파일
cargo test --locked --no-run

# 서버 바이너리 빌드
cargo build --locked
```

| 검증 | 결과 |
| --- | --- |
| 새 HTTP 경로 회귀 테스트 | 5개 통과, 실패 0개 |
| 기존 바이너리 직렬화 테스트 | 1개 통과, 실패 0개 |
| 전체 테스트 코드 컴파일 | 성공 |
| 서버 빌드 | 성공. 기존 미사용 코드 경고 등은 남아 있음 |

새 바이너리를 로컬 DB `127.0.0.1:5433`, Kafka `127.0.0.1:9092`에 연결해 재시작한 뒤 다음 응답도 확인했다. Java의 봇 수는 0으로 유지했다.

| 실제 HTTP 요청 | 결과 |
| --- | --- |
| 엔진 `GET /api/health` | 200 |
| 엔진 `POST /api/auth/signin` | 404 |
| 엔진 `GET /api/wallets/my` | 404 |
| 엔진 `POST /api/cex/orders`에 빈 JSON | 422 |
| 엔진 `POST /api/cex/balances/sync`에 빈 JSON | 422 |
| 엔진 `DELETE /api/cex/orders/1`에 user_id 생략 | 400 |
| Java `POST /api/auth/signin`에 존재하지 않는 계정 | 401 |
| 프론트엔드 `GET /` | 200 |

이번 테스트는 인증 코드 삭제 후 HTTP 경계와 초기화가 유지되는지 확인한다. 실제 주문 체결·잔고 정합성·복구를 검증한 결과는 아니다. 기존 DB 통합 테스트 전체는 실행하지 않았다. 해당 테스트의 공통 헬퍼는 DB 주소를 `localhost/cex_test`로 고정하고 데이터를 삭제하므로, 이번 로컬 DB와 분리한 실행 환경이 필요하다.

## 관련 커밋

- `889d148`: 기존 테스트의 인자·이벤트 필드 누락 수정.
- `13c7de0`: 사용자 인증 도메인과 비활성 HTTP 경로 제거, 회귀 테스트 추가.
