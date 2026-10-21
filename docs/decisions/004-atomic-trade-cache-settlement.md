# 한 체결의 두 자산을 메모리에 함께 반영하기

매수자의 돈만 이동하고 매도자의 코인은 이동하지 않는 부분 반영을 막는다. 한 체결의 캐시 변경을 먼저 계산·검증한 뒤 함께 적용하고, 실패한 체결의 성공 후속 처리를 중단한다. 주문장·DB까지 포함하는 거래 확정은 후속 과제다.

## 문제와 기존 동작

`Executor.execute_trade`는 매수자 예약 USDT를 매도자에게 보낸 다음, 매도자 예약 SOL을 매수자에게 보냈다. 두 번째 예약이 부족하면 첫 번째 이동이 남았다. 없는 자산을 조회하면서 0 잔액 행도 생성했다. Decimal 곱셈은 범위를 넘으면 panic하거나 정밀도에 따라 반올림할 수 있었다.

예를 들어 2 SOL × 100 USDT 체결에서 매도자 예약이 1 SOL이면, 기존 구현은 매수자 예약 200 USDT를 먼저 차감하고 매도자에게 적립한 뒤 오류를 반환했다. 최초 회귀 테스트 10건 중 7건이 실패했다. 잔액 손상·범위·정밀도 오류를 실제로 재현한 결과다.

호출부도 문제였다. 시장가는 실패 체결을 건너뛰었고, 지정가는 오류를 로그만 남기고 정상 처리를 계속했다. 이제 두 경로 모두 오류를 반환해 그 뒤의 상태 갱신·예약 해제·이벤트 발행을 진행하지 않는다.

## 기존 제품과 공식 자료의 접근

- [TigerBeetle linked events](https://docs.tigerbeetle.com/coding/linked-events/)는 연결한 금융 변경을 모두 성공하거나 모두 실패하게 처리한다. 이번에는 그 원칙을 한 체결의 메모리 잔액에 적용한다. TigerBeetle 도입이나 동일한 영속성 보장을 의미하지 않는다.
- [PostgreSQL 트랜잭션](https://www.postgresql.org/docs/16/tutorial-transactions.html)도 여러 변경의 전부 반영 또는 전부 취소를 제공한다. 단, DB 트랜잭션이 Rust 메모리까지 되돌려 주지는 않는다.
- [rust_decimal 1.40.0 곱셈 구현](https://github.com/paupino/rust-decimal/blob/1.40.0/src/ops/mul.rs)을 확인했다. `checked_mul`만으로 정확한 금액 계산을 보장할 수 없다. 범위 검사와 반올림 금지를 별도로 다룬다.
- [Binance 자기 체결 방지](https://developers.binance.com/en/docs/products/spot/faqs/stp_faq)는 별도 정책이다. 동일 고객의 키를 합산하는 이번 구현은 자기 체결 방지를 구현한 것이 아니다.

## 선택과 트레이드오프

| 선택 | 이점 | 비용·제약 |
|---|---|---|
| 최대 네 잔액을 임시 계산한 뒤 적용 | 실패 시 원본과 행 수 보존, 전체 캐시 복제 불필요 | 체결마다 작은 HashMap·문자열·Vec 할당 |
| 현재 NUMERIC(30,9)의 고정 단위 정수 | DB 정밀도에 맞춰 검증, 반올림 금지 | 9자리를 넘는 정확한 값도 현재 계약에서는 거부 |
| 이미 잠금 파일에 있는 num-bigint 0.4.6을 직접 의존성으로 사용 | 최대 60자리 중간 곱을 정확히 계산 | BigInt 연산·할당 비용, 직접 의존성 관리 |
| 최종 Decimal 변환도 반영 전에 완료 | 마지막 수신 계정의 표현 불가도 부분 반영 없이 거부 | PostgreSQL에 표현돼도 Decimal에 정확히 표현되지 않는 값은 거부 |
| 호출부에서 오류 즉시 반환 | 실패 뒤 거짓 성공 처리를 차단 | 이미 바뀐 주문장·앞선 체결의 복구 문제는 남음 |

단순 `checked_mul`은 반올림을 놓친다. i128만 사용하면 최종 값이 유효해도 중간 곱이 넘을 수 있다. 캐시 전체 복제는 고객·자산 수만큼 비용이 증가한다. 사후 역이체는 복구 중 또 실패할 경로를 늘린다. 따라서 네 키만 검증하고 적용한다. 성능 개선 수치는 측정하지 않았다.

## 구현과 관련 코드

- [settlement.rs](../../src/domains/cex/engine/balance_cache/settlement.rs): 입력·정밀도·범위 확인, 정확한 quote 계산, 잔액 준비와 적용.
- [balance_cache.rs](../../src/domains/cex/engine/balance_cache.rs): 기존 순차 `transfer` 삭제. 실패 조회가 원본에 행을 추가하지 않는다.
- [executor.rs](../../src/domains/cex/engine/executor.rs): `settle_trade` 성공 뒤에만 기존 DB 명령 전달 경로 실행. 실행되지 않던 WAL·DB 코드 주석과 잘못된 복구 보장 설명 삭제.
- [threads.rs](../../src/domains/cex/engine/runtime/threads.rs): 지정가·시장가 정산 오류를 호출자에게 반환.

흐름은 **입력 검증 → 9자리 단위 변환 → 정확한 대금 계산 → 네 키의 새 잔액 계산 → 모든 Decimal 변환 → 원본 반영**이다. buyer quote 예약 차감, seller quote 사용 가능 증가, seller base 예약 차감, buyer base 사용 가능 증가를 처리한다. 같은 `(고객, 자산)` 키는 합친다. 수수료는 이번 범위에 없다.

가격·수량은 양수이고, 자산은 서로 다르며, 사용자·주문 ID는 양수여야 한다. 동일 주문 ID 두 개는 거부한다. 관련 잔액의 음수·지원 범위 초과·정밀도 오류도 거부한다. 최종 값은 DB와 Decimal 양쪽에 정확하게 표현돼야 한다.

## 이해에 필요한 Rust 문법과 원리

- `&mut self`: 이 호출 동안 캐시를 독점해서 수정한다. TypeScript의 객체 참조와 달리 Rust가 중복 접근을 컴파일 단계에서 제한한다. 실제 호출은 Executor의 mutex도 잡는다. DB나 다른 프로세스의 상태까지 잠그지는 않는다.
- `Result<()>`와 `?`: 성공은 `Ok(())`, 실패는 `Err`다. `?`는 오류일 때 즉시 반환한다. TypeScript의 예외 전파와 비슷하지만 반환 타입에 실패가 드러난다.
- 임시 `HashMap`은 새 값의 작업 공간이다. 원본을 수정하지 않으므로 오류가 나면 버리면 된다. TypeScript에서 별도 Map으로 다음 상태를 계산한 후 교체하는 방식과 비슷하다.
- `checked_add`는 정수 넘침을 `Option`으로 반환하고 `context`가 설명 있는 오류로 바꾼다. 돈 계산에 넘침·반올림을 숨기지 않는다.
- `collect::<Result<Vec<_>>>()?`는 모든 잔액 변환이 성공해야 Vec를 만든다. 마지막 변환이 실패해도 앞의 원본 잔액은 바뀌지 않는다.

이 보장은 정상적인 Result 오류에 대한 것이다. 프로세스 강제 종료·메모리 부족·호스트 손실에서 영속성을 제공하지 않는다.

## 작성한 테스트

[settlement_tests.rs](../../src/domains/cex/engine/balance_cache/settlement_tests.rs)는 실제 Executor를 호출한다. 실패 검사는 전체 잔액 Map·새 행 생성 여부·DB 명령 없음과 panic 없음까지 비교한다. DB·Kafka 서버를 사용하지 않는다.

| 테스트 함수 | 시나리오와 기대 결과 |
|---|---|
| second_asset_shortage_does_not_leave_first_transfer_applied | 두 번째 자산 부족, 첫 이동도 없음 |
| missing_seller_asset_does_not_create_balances_or_move_quote | 매도 자산 없음, 0 행·대금 이동 없음 |
| first_asset_shortage_keeps_all_balances_unchanged | 매수 예약 부족, 원본 보존 |
| negative_and_zero_trade_values_are_rejected_before_mutation | 가격·수량 0/음수 거부 |
| quote_multiplication_overflow_returns_error_without_panic | 곱셈 범위 초과, 오류로 반환 |
| second_recipient_overflow_does_not_apply_either_asset | 수신 잔액 범위 초과, 모두 보존 |
| quote_finer_than_database_precision_is_not_rounded | 9자리 미만 단위 대금, 반올림 없이 거부 |
| invalid_asset_pair_is_rejected_before_mutation | 같은 자산 쌍 거부 |
| successful_trade_moves_both_assets_and_emits_one_trade_command | 정상 두 자산 이동, DB 체결 명령 정확히 한 건 |
| successful_market_execution_keeps_db_publication_with_the_caller | 시장가의 DB 전달 책임 유지 |
| missing_buyer_asset_does_not_create_a_zero_row | 매수 지불 자산 없음, 새 행 없음 |
| valid_database_balance_cannot_overflow_on_either_recipient | DB 최대 잔액에 추가 적립 거부 |
| decimal_precision_loss_on_last_recipient_rejects_all_changes | 마지막 수신 값 Decimal 표현 불가, 모두 보존 |
| price_and_quantity_must_fit_current_database_precision | 입력 가격·수량 DB 계약 위반 거부 |
| smallest_supported_quote_is_transferred_exactly | 최소 0.000000001 대금 정확히 반영 |
| same_customer_keys_are_coalesced_without_losing_balances | 같은 고객의 키 합산, 자산 보존 |
| existing_balances_and_unrelated_assets_are_preserved | 기존 수신 잔액 누적, 무관 자산 유지·자산별 합계 보존 |
| corrupt_negative_balance_is_not_hidden_or_normalized | 손상된 음수 잔액 거부 |
| invalid_user_order_and_asset_identifiers_are_rejected | 사용자·주문·자산 식별자 오류 거부 |
| enormous_product_of_valid_operands_fails_without_panic | 유효 입력끼리 곱해도 최종 대금 초과면 거부 |
| exact_valid_product_beyond_i128_intermediate_is_supported | 중간 곱이 i128 초과여도 최종 유효 대금 정확히 처리 |

[settlement_pipeline_tests.rs](../../src/domains/cex/engine/runtime/engine/settlement_pipeline_tests.rs)는 실제 Matcher → Executor → 호출부를 검증한다.

| 테스트 함수 | 시나리오와 기대 결과 |
|---|---|
| real_matching_pipeline_settles_both_assets_for_limit_and_market | 지정가·시장가 각각 실제 체결 후 두 자산 잔액 확인 |
| settlement_failure_aborts_success_followups_for_limit_and_market | 주문장 수량과 예약금 불일치 주입, 두 경로 모두 오류·후속 DB 명령 없음·접수 예약 유지 |

## 실행 명령과 결과

PostgreSQL 16 로컬 전용 5433에 UUID를 붙인 새 관리자 DB를 만들고 SQLx 테스트마다 격리 DB를 사용한다. 실행 후 관리자 DB를 제거한다. 다른 프로젝트의 5432는 사용하지 않는다.

```bash
# DATABASE_URL은 이 실행에서 새로 만든 격리 관리자 DB를 지정한다.
cargo test --locked --lib settlement_
cargo test --locked --lib order_receipt::tests -- --test-threads=1
cargo test --locked --lib startup_tests -- --test-threads=1
cargo test --locked --lib reservation_tests
cargo test --locked --lib routes::tests
cargo test --locked --no-run
cargo build --locked
JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home \
PATH=/usr/local/bin:$PATH python3 scripts/verify_durable_acceptance.py
```

- 신규 테스트 **23건 통과**: 캐시·Executor 21건, 실제 매칭 파이프라인 2건. 파이프라인 각 테스트에서 지정가·시장가를 각각 검사한다.
- 기존 회귀 **37건 통과**: 접수 16건, 시작 상태 15건, 예약 1건, 라우터 5건. 합계 **60건 통과, 실패 0, ignored 0**. 필터 밖 테스트는 실행하지 않았다.
- 전체 테스트 컴파일·엔진 바이너리 빌드 성공. 기존 unused/dead-code 경고와 Solana/SQLx future incompatibility 안내는 남는다. 공유 DB 삭제 헬퍼가 있는 기존 통합 테스트는 실행하지 않았다.
- 새 Java·Rust·임시 PostgreSQL 프로세스 검증 **3개 시나리오 통과**: 실제 응답 유실 후 고객 202·재요청 200·다른 원본 409·실행 명령 1회, 다음 정상 접수 고객 201, 미복구 접수 재시작 exit 101·HTTP 시작 차단.
- 이 프로세스 검증은 상대 주문 없는 주문 접수 회귀다. 실제 체결 원자성은 위 인메모리 파이프라인 테스트가 검사하며, DB 정산까지 검증했다고 해석하지 않는다.
- 격리 관리자 DB `cex_settlement_1f31a814df7c`, 별도 프로세스 검증 컨테이너 `cex-receipt-check-b246d80b6d85`와 하위 프로세스를 정리했다. 봇 수는 0이다. 컨테이너 준비 확인 중 최초 DB 연결 오류 1회 뒤 정상 준비돼 검증을 완료했다.
- 로컬 증거: 공통 루트 `.local/settlement-before.log`(수정 전 3통과·7실패), `settlement-{settlement,receipt,startup,reservation,routes,compile,build}.log`, `settlement-regression.json`, `settlement-process.log`, `durable-acceptance-process.json`. `.local`은 Git 문서의 재현 절차를 보충하는 로컬 기록이다.

## 보장 범위와 남은 과제

- 한 체결의 메모리 잔액은 검증 실패 시 바뀌지 않는다. 동일 체결 재실행 방지·영속 복구는 제공하지 않는다.
- Matcher가 주문장을 먼저 변경한다. 정산 실패로 이미 변경된 주문장이나 앞서 성공한 다른 체결을 되돌리지 않는다. 즉 한 주문의 여러 체결 전체가 원자적이지 않다.
- 오류 반환은 해당 명령의 후속 성공 처리를 막지만 엔진 전체를 정지·격리하지 않는다. 주문장 보존과 실패 후 실행 차단을 함께 설계해야 한다.
- 캐시 성공 후 DB 채널 전달은 별도다. 기존 채널 전송은 대기할 수 있고, 실패하면 로그를 남기며 캐시를 되돌리지 않는다. 채널 성공도 DB 커밋 증명이 아니다. `ExecutionResult.success`는 영속 체결 확정을 의미하지 않는다.
- 원장·체결·주문 상태·이벤트의 동일 확정, outbox, 순서 있는 재생, 자기 체결 방지, 수수료는 후속 범위다. H2 전체 완료나 금융 서비스 운영 준비 완료로 표시하지 않는다.
- 기존 개발 서버는 재시작하지 않았다. 봇은 비활성 상태를 유지한다.

## 관련 커밋과 참고 자료

선행 영속 접수: `c491dcd`, 예약 중복 반영 제거: `11abc06`. 이번 변경: `33d299d`(캐시 원자 반영·정밀 계산·21개 테스트), `5cc89d0`(실패 후속 처리 중단·파이프라인 2개 테스트).

- [num-bigint 공식 저장소](https://github.com/rust-num/num-bigint): 설치된 의존성·타입을 확인하고 0.4.6을 직접 사용했다. 새 패키지 버전을 추가하지 않았다.
- [이전 접수 결정](003-durable-order-acceptance.md): 접수 확정과 실제 실행·정산 확정의 차이.

후속 검토: [금액 타입 경계 오류 수정](005-exact-amount-boundaries-review.md). 이 글의 당시 검증 결과와 후속 변경을 구분한다.
