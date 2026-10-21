# 주문 접수·시작·조회에서 금액을 정확하게 읽기

2026-10-21에 지금까지 변경한 인증 책임 분리, 시작 검증, 주문 멱등 접수, 영속 접수와 캐시 정산 경로를 다시 검토했다. 이 문서는 검토에서 재현한 **금액 타입 경계 오류**와 수정·테스트를 설명한다. 전체 서비스 보안 감사나 거래 확정 설계의 완료 보고서는 아니다.

## 발견한 문제와 재현

| 중요도 | 문제 | 재현·영향 | 수정 |
|---|---|---|---|
| 높음 | NUMERIC과 Decimal의 범위 차이 | DB에 저장된 `999999999999999999999.123456789`를 SQLx 0.6.3이 Decimal로 해독하다 panic. 시작 검증·신규 접수 각각 재현 | DB 금액을 text로 읽어 `from_str_exact`로 변환하고 오류 전파 |
| 높음 | 요청 금액의 묵시적 반올림 | `2.00000000000000000000000000001` 같은 문자열이 Decimal로 읽힐 때 다른 값이 될 수 있음. 원본 대조 전에 의미 손실 | 주문 생성 DTO에서 문자열만 받고 정확한 변환 실패 시 거부 |
| 높음 | Java 검증이 DB 범위에만 의존 | BigDecimal은 DB에 저장돼도 엔진이 표현하지 못하는 금액을 허용. 예약을 쪼갠 결과도 표현 범위를 벗어날 수 있음 | [플랫폼 결정](../../../Cex-Platform-Service/docs/decisions/005-common-exact-amount-contract.md)에서 공통 계약·예약 결과 검증 |
| 보통 | 활성 주문 조회의 반환 컬럼 누락 | `get_active_orders` 두 분기가 `filled_quote_amount`를 읽지만 SELECT에서 누락. 기존 `row.get`은 해독 실패 시 panic | 컬럼 추가, 주문 행 변환 전체를 Result로 변경 |
| 검증 신뢰성 | SQLx 테스트 DB 이름 충돌 | 관리자 DB만 UUID로 분리해도 실패한 `_sqlx_test_2`, `_sqlx_test_6`가 서버에 남아 후속 실행과 충돌 | 실행마다 PostgreSQL 컨테이너 자체 격리 |

첫 DB 경계 재현 필터 `_panic`은 8건 중 6통과·2실패였다. 이번 신규 4건 중 2건이 위 panic을 재현했고, 나머지는 정확한 큰 값의 보존을 검사하는 정상 대조군이다. 이후 요청 변환 재현은 3건 중 1통과·2실패였다. 실패를 보인 뒤 구현을 수정했다.

## 공식 구현과 선택 근거

- [PostgreSQL 16 NUMERIC](https://www.postgresql.org/docs/16/datatype-numeric.html): 지정한 scale보다 세밀한 값은 저장 시 반올림한다. DB가 값을 받아 줬다는 사실만으로 업무 금액 보존을 증명하지 못한다.
- [SQLx 0.6.3 Decimal 변환](https://github.com/launchbadge/sqlx/blob/v0.6.3/sqlx-core/src/postgres/types/decimal.rs): 설치 소스의 NUMERIC 바이너리 해독이 `Decimal::from_i128_with_scale`을 호출하는 것을 확인했다.
- [rust_decimal 1.40.0](https://github.com/paupino/rust-decimal/blob/1.40.0/src/decimal.rs): Decimal은 96비트 계수와 scale을 사용한다. 검사 없는 생성자는 범위 밖에서 panic하며 `from_str_exact`는 표현할 수 없는 값을 오류로 반환한다. docs.rs를 도구로 열지 못해 설치 소스·공식 GitHub 구현을 확인했다.

새 파서를 직접 만들지 않고 설치된 라이브러리의 정확한 변환 API를 사용했다. SQLx 전체 업그레이드는 별도 API·런타임 변경 검증을 요구한다. 현재 변경 경로에서 바이너리 해독을 제거하고 `NUMERIC::text → from_str_exact → Result`를 사용한다.

## 구현과 트레이드오프

- [exact_decimal.rs](../../src/shared/database/exact_decimal.rs): SQL text 컬럼을 정확하게 변환한다. 표시용 소수 끝의 0만 제거하고, 실패는 컬럼명 있는 `ColumnDecode`로 반환한다. NULL을 0으로 바꾸지 않는다.
- [startup.rs](../../src/domains/cex/engine/runtime/startup.rs): 시작 잔액은 정확한 해독 뒤 기존 비음수·예약 검증과 현재 NUMERIC(30,9) 범위 검증을 거친다. 값 보정·DB 수정은 없다.
- [order_receipt.rs](../../src/domains/cex/services/order_receipt.rs): 원본·기존 접수·INSERT RETURNING 모두 같은 정확한 변환을 사용한다. SQLx `FromRow`를 명시적으로 구현해 오류를 전파한다.
- [order_repository.rs](../../src/shared/database/repositories/cex/order_repository.rs): 주문 조회·상태 변경 반환값도 `Result<Order>`로 해독한다. 활성 주문의 누락 컬럼을 추가했다. `exact_price` 별칭을 사용해 `ORDER BY price`가 여전히 NUMERIC 정렬이 되게 했다.
- [order.rs](../../src/domains/cex/models/order.rs): 생성 명령의 price·amount·quote_amount에만 정확한 문자열 변환을 적용한다. 누락·null은 None이고 JSON 숫자는 거부한다. 기존 숫자 입력 폴백은 없다. Java는 이미 문자열을 보낸다. 잘못된 DTO의 Axum JSON 추출 오류는 서비스 진입 전에 발생한다.

**트레이드오프:** text 조회는 문자열 할당·파싱 비용이 들고 명시적인 반환 컬럼이 필요하다. 대신 금액 변환 실패가 프로세스 panic이나 반올림으로 숨겨지지 않는다. SQL 형식과 실제 PostgreSQL 테스트를 함께 유지한다. 성능 개선·저하 수치는 측정하지 않았다.

이번 적용은 시작·접수·주문 저장소 경계다. 기존 balance/trade 집계 저장소 등 모든 Decimal 사용처를 교체한 것은 아니다. 해당 경로도 후속 검토 대상이며 전역적인 panic 제거를 주장하지 않는다.

## Rust를 이해하는 데 필요한 부분

`FromRow<'r, PgRow>`는 DB 행을 StoredCommand로 바꾸는 규칙이다. `'r`은 참조를 사용할 수 있는 기간을 나타내며 새 스레드나 복제를 의미하지 않는다. `try_get`은 해독 실패를 Result로 돌려준다. 기존 `get`은 실패 시 panic할 수 있다.

`Option<Result<T,E>>.transpose()`는 `Result<Option<T>,E>`로 바꾼다. 값이 없으면 None을 유지하고 값이 잘못되면 오류를 반환한다. TypeScript라면 `null` 검사 뒤 실패할 수 있는 변환 함수를 호출하는 흐름과 비슷하다. 다만 Rust는 실패와 값 부재를 타입으로 분리한다.

`collect::<Result<Vec<_>>>()`는 목록의 한 행이라도 잘못되면 목록 전체를 오류로 반환한다. 잘못된 행을 조용히 건너뛰어 정상 주문 목록처럼 제공하지 않는다.

## 작성한 테스트

[startup_tests.rs](../../src/domains/cex/engine/runtime/engine/startup_tests.rs):

| 신규 함수 | 시나리오·기대 결과 |
|---|---|
| startup_preserves_large_exact_fraction_without_decoder_panic | 정확히 표현되는 큰 소수 복원 |
| startup_rejects_unrepresentable_decimal_without_panic | DB에는 들어가지만 Decimal 표현 불가, 오류·워커 미시작 |
| startup_rejects_available_outside_current_storage_contract | 정수 22자리·소수 10자리 잔액 차단 |

[접수 테스트](../../src/domains/cex/services/order_receipt/tests.rs):

| 신규 함수 | 시나리오·기대 결과 |
|---|---|
| receipt_preserves_large_exact_amount_without_decoder_panic | 큰 정확한 원본 신규 접수·중복 확인 성공 |
| receipt_rejects_unrepresentable_original_without_panic | 원본 해독 오류·접수 기록 없음 |

[요청 변환 테스트](../../src/domains/cex/models/order/decimal_tests.rs):

| 신규 함수 | 시나리오·기대 결과 |
|---|---|
| request_decimals_reject_precision_loss_in_every_amount_field | 세 금액 필드의 초과 정밀도·작은 값 소실·범위 초과 거부 |
| request_decimals_preserve_exact_strings_and_optional_absence | 최대 계수·끝의 0·null·누락 보존 |
| request_decimals_require_strings_instead_of_json_floating_point | JSON 숫자 거부 |

[주문 저장소 테스트](../../src/shared/database/repositories/cex/order_repository/exact_amount_tests.rs):

| 신규 함수 | 시나리오·기대 결과 |
|---|---|
| order_reads_preserve_exact_amount_and_numeric_price_priority | 생성·조회·상태 변경·취소의 정확한 값, 활성 조회 양 분기, 매수 100이 9보다 우선 |
| order_read_rejects_unrepresentable_database_amount_without_panic | 저장된 표현 불가 금액 조회가 오류 반환 |

## 실행과 결과

```bash
python3 scripts/verify_order_safety.py
JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home \
PATH=/usr/local/bin:$PATH python3 scripts/verify_durable_acceptance.py
```

[검증 실행기](../../scripts/verify_order_safety.py)는 postgres:16을 임의 루프백 포트에 띄우고 SQLx 테스트를 직렬 실행한다. 실패한 하위 DB도 컨테이너와 함께 제거한다. 기존 5432·고객 DB를 사용하지 않는다.

- Rust **70건 통과**, 실패·ignored 0: 요청 3, 주문 저장소 2, 캐시 정산·호출부 23, 접수 18, 시작 18, 예약 1, 라우터 5. 이전 60건에 신규 10건을 더했다. 필터 밖 테스트는 실행하지 않았다.
- 전체 테스트 컴파일 `cargo test --locked --no-run`, 바이너리 빌드 `cargo build --locked` 성공. 기존 unused/dead-code 및 Solana/SQLx future incompatibility 경고가 남는다.
- Java **90건 통과**, 실제 PostgreSQL·Java·HTTP 대역 검증 **10건 통과**. 세부 내용은 플랫폼 문서에 기록했다.
- 실제 Java↔Rust↔PostgreSQL 접수 회귀는 응답 유실·정상 접수·미복구 재시작 차단의 **3개 시나리오**다. 실제 체결의 DB 정산·Kafka 재전달까지 검증하는 테스트는 아니다.
- 최초 회귀 실행에서 DB 이름 충돌로 2건이 테스트 준비 중 실패했다. 제품 회귀로 숨기지 않고 서버 격리 후 전체 선택 테스트를 재실행했다. 이전 관리자 DB만 분리한 실패 실행은 SQLx 하위 DB를 남길 수 있으므로, 재현할 때 새 실행기를 사용한다. 기존 공유 서버의 식별 불가능한 DB를 임의로 삭제하지 않았다.
- 증거는 공통 루트 `.local/review-{decoder,request}-before.log`, `review-rust-*.log`, `review-rust-results.json`, `review-durable-process.log`다. 기존 개발 서버는 교체하지 않았고 검증 봇은 0이다.

## 남은 핵심 과제

이 검토로 주문장·캐시·DB·이벤트의 단일 확정은 완성되지 않았다. Matcher가 주문장을 먼저 바꾸고, 여러 체결 중 앞선 성공은 뒤 실패에서 복구되지 않으며, 캐시 성공 뒤 DB 전달 실패도 별도다. 영속 접수의 실행 순서·재생과 단일 처리자 보장도 미완료다. [이전 결정의 한계](004-atomic-trade-cache-settlement.md#보장-범위와-남은-과제)를 유지한다.

관련 커밋: `8aaca62`(정확한 DB 해독·누락 컬럼·7개 테스트), `8f862f1`(정확한 요청·3개 테스트), `4fdb026`(격리 실행기). 플랫폼 관련 커밋은 연결 문서에 기록한다.

후속: [신규 주문 전체의 메모리 확정](006-atomic-order-memory-commit.md). 주문장·여러 체결·예약 반환의 한계를 메모리 경계에서 해결했으며 DB·Kafka·복구 한계는 유지한다.
