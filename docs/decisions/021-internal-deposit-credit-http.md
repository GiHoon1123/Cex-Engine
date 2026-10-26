# 플랫폼의 원본 식별 입금 명령을 내부 HTTP와 영속 결과에 연결하기

플랫폼 서비스 신원을 확인한 HTTP 요청에서 기존 입금 명령 핸들을 호출한다. 성공·영속 거절·이번 미접수·결과 불명을 고정 코드로 구분하며, 정확한 금액과 안정 원본을 유지한다. 체결 코어·저널·출력 워커의 제품 구현은 변경하지 않았다. 공통 사이클123·엔진 결정021이다.

후속 [022 최초 기동 보완](022-first-bootstrap-current-history.md)은 현재 플랫폼 수신·입금 전송 이력도 확인해 새 경로를 최초 시작점으로 오인하지 못하게 한다. 기존 자체 저널 재시작은 계속 DB 없이 복원한다.

## 문제와 기존 동작

[018](018-idempotent-deposit-credit.md)은 안정 입금 ID와 금융 내용을 한 번만 반영하고 WAL로 복구한다. [020](020-typed-deposit-outcomes.md)은 핸들의 오류를 타입으로 나눴다. 하지만 플랫폼이 이 명령을 호출할 HTTP 경계는 없었다. 삭제한 잔액 변화량 API를 되살리면 원본과 중복 방지 계약을 잃는다.

## 기존 제품과 공식 자료의 접근

2026-10-25 [AWS의 멱등 재시도 설명](https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/)을 다시 확인했다. 호출자의 안정 식별자와 의도를 보존하고, 응답 유실 후에도 같은 내용을 재확인하는 패턴을 따른다. 특정 서비스의 보존 기간이나 내부 구현을 복제한 것은 아니다.

Cargo.lock의 Axum0.7.9·rust_decimal1.40.0·Serde·Tower를 확인했다. docs.rs 웹 조회는 실패했지만 설치된 해당 버전의 `axum-0.7.9/src/json.rs`, `rust_decimal-1.40.0/src/serde.rs`의 실제 문서/구현을 읽었다. [Axum 소스](https://github.com/tokio-rs/axum/blob/axum-v0.7.9/axum/src/json.rs)와 [Decimal 소스](https://github.com/paupino/rust-decimal/blob/1.40.0/src/serde.rs)를 참고 경로로 연결한다.

이미 활성화된 serde-with-str의 문자열 방문자는 from_str 뒤 from_scientific도 시도한다. 금융 경계에서 지수 표기나 정밀도 축소를 허용하지 않기 위해 금액은 String으로 받은 뒤 설치된 Decimal::from_str_exact와 기존 amount::units를 사용한다. 흔한 JSON 파서·금액 라이브러리를 새로 구현하거나 의존성을 추가하지 않았다.

## 선택과 트레이드오프

| 선택 | 얻는 점 | 비용·제약·검토한 대안 |
| --- | --- | --- |
| 기존 명령 핸들만 HTTP로 연결 | 단일 소유 코어·기록 확정·멱등 계산을 그대로 사용 | HTTP 대기는 기존3초 계약이며 실제 플랫폼 금융 반영은 별도 |
| 문자열의 정확한 십진 변환 | JSON 실수·지수/공백/부호·반올림으로 다른 금액 반입 방지 | 클라이언트는 금액을 문자열로 보내야 한다. 기본 Decimal 문자열 방문자를 그대로 쓰지 않았다 |
| 고정 네 가지 결과 | 문구 파싱이나 실패=미반영 추정을 피함 | 503/504만 보고 과거 실행 여부를 부정할 수 없다. 안정 원본 보존·재확인이 필요 |
| 공통 플랫폼 서비스 키 | 고객 JWT가 엔진 금융 명령 권한이 되지 않음 | 플랫폼 키 유출은 중대한 권한 침해다. 사설 연결/루프백·키 관리·네트워크 제한은 운영 조건 |
| 입금 전용4KiB 본문/고정 필드 | 과도한 본문·알 수 없는 잔액 필드 차단 | 이 내부 계약 밖의 필드를 보내면 거부한다. 기존 주문의 크기 계약을 바꾸지 않았다 |
| 응답에 고정 코드와 입금 ID만 | 내부 SQL·저널 오류 사유 비노출 | 세부 원인은 제한된 운영 증적에서 확인해야 함 |

## API와 금융 확정 계약

`POST /api/cex/deposits/credits`는 기존 x-engine-service-token 검증 아래 있다. 사용자 JWT만 보내면401이다. 이 경로는 프론트의 진입점이 아니며 체인 확인·KYC·출처/위험 판단·플랫폼 원장은 플랫폼 책임이다.

```json
{"deposit_id":"b04ec271-c9d8-4555-9314-1f7033b00cd7","user_id":3,"asset":"BTC","amount":"0.12345678"}
```

예시 UUID는 설명용이다. 실제 호출은 플랫폼이 영속한 동일 입금 원본을 사용한다. nil UUID·0/Java long 범위를 벗어난 고객·빈/소문자/공백/비ASCII 자산·수치 JSON 금액·음수/0·허용 범위/정밀도 초과·알 수 없는 필드를 거부한다. 자산 문자열은 대문자 ASCII와 숫자128바이트 이내다.

현재 엔진 정산 정밀도는 NUMERIC(30,9) 계약이다. 이 API가 BTC 수탁망의8자리나 향후 ETH18자리 원자 단위를 검증/지원 완료했다는 뜻은 아니다. 플랫폼의 실제 수탁 원본 정밀도와 엔진 정산 정밀도는 구분한다. 같은 값의 `0.12345678`/`00.123456780`은 동일 금융 내용이며 반영을 늘리지 않는다.

| HTTP | code | 의미 |
| --- | --- | --- |
| 200 | DURABLY_CREDITED | 기존 핸들이 자체 저널 확정 뒤 반환. 같은 원본 재확인도200이며 새 반영 여부를 추측한 플래그를 만들지 않음 |
| 409 | DURABLY_REJECTED | 현재 명령의 거절을 자체 저널에 확정한 응답. 기존 성공 원본의 다른 금액/고객/자산 요청도 거절 |
| 503 | NOT_ACCEPTED | 이번 큐 전송이 접수되지 않음. 과거 같은 입금이 미반영됐다는 뜻은 아님 |
| 504 | OUTCOME_UNKNOWN | 큐 진입 뒤 응답 유실/시간 초과/기록 실패. 실제 성공·거절 모두 가능하므로 같은 원본으로 재확인 |
| 400/413 | INVALID_DEPOSIT_CREDIT | 입력/크기 검증에서 이번 명령을 보내지 않음. 이전 요청의 실행 여부를 증명하지 않음 |

핸들러 응답은 Cache-Control:no-store다. 서비스 신원 거부는 기존 상위 미들웨어의401이며 금융 본문을 읽거나 원문 오류를 내보내지 않는다. 성공은 브로커 수신이나 플랫폼 금융 반영 완료를 의미하지 않는다. 고객 PENDING 해제/AVAILABLE 반입을 HTTP200만으로 실행하는 코드는 이번에 추가하지 않았다.

## 구현과 실제 코드·문법

출처: [deposit_handler.rs](../../src/domains/cex/handlers/deposit_handler.rs#L16), 현재 소스의 부분 발췌.

```rust
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreditRequest {
    deposit_id: Uuid,
    user_id: u64,
    asset: String,
    amount: String,
}
impl CreditRequest {
```

derive는 Serde의 실제 역직렬화를 생성한다. deny_unknown_fields는 허용하지 않은 입력을 런타임에 거부한다. String은 소유한 문자열이며 TypeScript의 정적 인터페이스 선언과 달리 실제 검증이 수행된다.

출처: [financial_input.rs](../../src/domains/cex/handlers/financial_input.rs#L29), 현재137에서 입출금이 공유하는 소스의 부분 발췌.

```rust
    let value = Decimal::from_str_exact(text).ok()?;
    if value <= Decimal::ZERO || amount::units(value).is_err() {
        return None;
    }
    Some(value)
}
```

현재137에서는 같은 검증을 financial_input으로 옮겨 입출금 HTTP가 함께 사용한다. 아래021 실행 결과는 당시 기록이다. 정확히 표현하지 못하면 None으로 입력을 거부한다. Rust의 ?는 여기서 Option의 실패를 즉시 반환하며, 기존 units가 정산 정밀도·범위를 검사한다. 임의 반올림이나 새 자산 설정 계층은 없다.

출처: [deposit_handler.rs](../../src/domains/cex/handlers/deposit_handler.rs#L64), 현재 소스의 부분 발췌.

```rust
    match state.engine.credit_deposit(credit).await {
        Ok(()) => reply(StatusCode::OK, "DURABLY_CREDITED", Some(id)),
        Err(DepositCreditError::NotAccepted) => {
            reply(StatusCode::SERVICE_UNAVAILABLE, "NOT_ACCEPTED", Some(id))
        }
        Err(DepositCreditError::Rejected { .. }) => {
            reply(StatusCode::CONFLICT, "DURABLY_REJECTED", Some(id))
        }
        Err(DepositCreditError::OutcomeUnknown) => {
            reply(StatusCode::GATEWAY_TIMEOUT, "OUTCOME_UNKNOWN", Some(id))
```

async 대기는 HTTP 태스크에서만 일어난다. 기존 핸들의 소유 명령을 넘기고 enum 변형을 빠짐없이 분기한다. TypeScript의 판별 가능한 합 타입과 비교할 수 있지만 Rust는 소유권 이동과 누락된 match 분기를 컴파일 시 확인한다. 주문별 스레드·런타임·새 공유 락을 만들지 않는다.

출처: [deposit_http_tests.rs](../../src/domains/cex/engine/runtime/engine/journal_tests/deposit_http_tests.rs#L160), 현재 소스의 부분 발췌.

```rust
    blocker.execute_batch("BEGIN IMMEDIATE")?;
    let response = router(&engine).oneshot(request(uuid::Uuid::new_v4(), "1"));
    tokio::pin!(response);
    // 막힌 것은 기록 워커다. 요청의 비동기 대기 중에도 금융 출력과 성공은 아직 없어야 한다.
    tokio::select! { result=&mut response=>panic!("저널이 막힌 동안 응답했습니다: {:?}",result), _=tokio::time::sleep(std::time::Duration::from_millis(100))=>{} }
```

시험의 별도 SQLite 연결만 쓰기를 막는다. 코어/HTTP를 가짜 성공으로 바꾸지 않고 실제 기록 워커가 기다리는 동안 응답·출력이 없는지 확인한 뒤 잠금을 풀어 성공을 확인한다. 이 시험 락은 제품 체결 코어에 추가한 락이 아니다.

## 작성한 테스트

[HTTP 경계10개](../../src/routes/tests/deposit.rs)는 실제 라우터와 단일 벤치 워커를 사용한다. 이10개 자체는 내구성 증거가 아니다.

| 함수 | 시나리오·기대 결과 |
| --- | --- |
| exact_string_amount_is_forwarded_once_without_rounding | 정확 금액/동일 원본 두 호출, 잔액 한 번 |
| equivalent_decimal_text_has_the_same_financial_identity | 같은 수치의 표현 차이는 중복 반입 아님 |
| changed_content_is_rejected_without_increasing_the_balance | 같은 ID의 금액/고객/자산 변경409 |
| malformed_or_unknown_fields_cannot_enter_the_worker | 누락/손상/중복 필드·추가 잔액 필드400, 잔액0 |
| numeric_json_and_non_decimal_strings_are_rejected | 실수 JSON/지수/공백/부호/밑줄/NaN 거부 |
| zero_excess_precision_and_overflow_never_get_rounded | 0·작은 정밀도 초과·큰 정수 범위 초과400 |
| invalid_identity_and_asset_are_rejected_before_dispatch | nil/0/고객 범위·잘못된 자산400 |
| only_the_platform_service_identity_can_send_credit | 고객 JWT/잘못된 서비스 키401·잔액0 |
| oversized_body_is_rejected_without_echoing_its_content | 큰 본문413·고정 코드·잔액0 |
| closed_worker_is_this_attempt_not_accepted | 정지 핸들503·NOT_ACCEPTED |

[실제 저널/워커5개](../../src/domains/cex/engine/runtime/engine/journal_tests/deposit_http_tests.rs)는 기존 실제 단일 코어·기록/출력 워커 조립과 임시 SQLite를 사용한다. 외부 PostgreSQL에는 lazy 대역만 제공하며 조회가 실행되지 않는1번 포트다. Kafka 대신 기존 출력 함수의 이벤트 수신 대역을 사용한다.

| 함수 | 시나리오·기대 결과 |
| --- | --- |
| deposit_http_commits_once_and_restart_preserves_original | HTTP200 전 저널 확정, 동일 입금 이벤트 한 번·재시작 재확인 후 잔액/이벤트 중복 없음 |
| deposit_http_changed_amount_is_a_durable_conflict | 충돌409도 저널에 확정, 원래 금액 복구 |
| deposit_http_failed_recording_is_unknown_and_never_published | 실제 기록 실패504·비공개 사유 비노출·이벤트0·복구 잔액 없음 |
| deposit_http_cannot_acknowledge_while_the_journal_is_blocked | 실제 저장 차단 동안 응답/이벤트 없음, 해제 뒤200/이벤트 하나 |
| deposit_http_calculation_rejection_survives_restart_without_reason_leak | 실제 범위 초과409·원인 비노출·재시작 후 동일 거절/원잔액 보존 |

## 실행 명령과 결과

- `cargo check --locked --offline`: 통과.
- `cargo test --locked --offline --lib routes::tests::deposit -- --nocapture`: 신규10개 통과.
- `cargo test --locked --offline --lib deposit_http -- --nocapture`: 신규5개 통과.
- `python3 scripts/verify_order_safety.py`: 신규15개를 포함한 **고유173개/207회** 통과, 실패·제외0. 선택 묶음의 중복을 이름으로 제거했다. 위 개별 실행15개를 이 수에 추가하지 않는다. 전체 시험 컴파일·개발 빌드도 통과했다.

검증 실행기는 소유 PostgreSQL16 서버 전체를 격리하고1번 포트의 Kafka 대역을 사용한다. 소유 컨테이너는 정리했고 금융/감사 원본과 기존 서비스 DB를 바꾸지 않았다. 결과 JSON/로그는 `.local/cycle123-proof`에 보존한다. 시험 초기 컴파일에서 JSON 중복 필드 문자열 표기와 Engine 트레이트 import를 수정했다. 신규 불필요한 mut 경고도 정리한 뒤 전체 회귀를 실행했다.

기존 unused/dead_code 경고와 SQLx0.6의 향후 Rust 호환 경고는 남는다. 실제 브로커·메인넷·플랫폼의 최종 금융 연결·장기 부하 검증 결과가 아니다. 기존 처리량 수치를 새 API의 처리량 보장으로 사용하지 않는다.

## 남은 범위

플랫폼이 현재 고객 자격/출처/위험·보류 원장·금융 승인에서 안정된 입금 반입 의도를 만들고 이 API를 호출해야 한다. 결과 불명 재확인과 확정 Kafka 수신을 플랫폼 영속 수신 키/원장·잔액에 연결하는 후속 작업이 남는다. 브로커 확인과 금융 반영은 다르며 재발행은 동일 이벤트 ID로 처리해야 한다.

현재 API는 같은 서비스 신원 경계의 내부 기능이다. 프로세스를 직접 공개하지 않는 네트워크/리버스 프록시·비밀 관리와 실행 권한은 운영 확인 대상이다. 호스트·디스크 손실의 외부 복제/백업은 기존 사용자 지시대로 조건을 문서에만 남기며 구현/배포하지 않았다.

## 관련 커밋과 증적

- [구현 0ba8c4e](https://github.com/GiHoon1123/Cex-Engine/commit/0ba8c4ead2e43a05477ac8798192b6883362a030)
- [격리 회귀 증적](../reference/021-internal-deposit-credit-http-verification-2026-10-25.json)
