# 주문 당시 적용률로 수취 자산 수수료를 확정하기

엔진과 플랫폼이 서로 다른 잔고를 만들던 수수료 처리 책임을 정리한다. 단일 스레드 코어에서 체결과 수수료를 함께 계산하고, 자체 WAL로 확정한 결과만 플랫폼에 전달한다. 작성일은 2026-10-22다.

## 문제와 기존 동작

기존 엔진은 수수료 없이 잔액을 계산했다. 플랫폼은 전체 주문의 마지막 잔액 스냅샷을 개별 체결마다 덮어쓴 다음 양측 USDT 수수료를 별도로 차감했다. 그 결과 다음 체결이 이전 수수료를 지울 수 있었고, 정산 전에 플랫폼이 접수한 다른 주문의 예약금까지 덮어쓸 수 있었다. 여러 상대 주문과 체결할 때 같은 최종 잔고를 반복 적용하는 문제도 있었다.

## 기존 제품과 공식 자료의 접근

[바이낸스 현물 수수료 FAQ](https://github.com/binance/binance-spot-api-docs/blob/master/faqs/commission_faq.md)는 수취량에 적용률을 곱하는 계산을 설명한다. 매수는 기준 자산 수량, 매도는 결제 자산 대금이 기준이다. 별도 할인 자산을 사용하지 않을 때 받은 자산에서 차감한다. 확인일은 2026-10-22다. 여기서는 수취 자산 차감 패턴만 채택하며 바이낸스의 적용률·할인·세금·소수 자릿수까지 같다고 주장하지 않는다.

## 선택과 트레이드오프

| 선택 | 얻는 것 | 비용과 제약 |
| --- | --- | --- |
| 매수 BTC·매도 USDT처럼 받은 자산에서 차감 | 추가 수수료 예약 없이 대금·수량만 예약 | 수익을 자산별로 기록·집계해야 함 |
| 플랫폼이 접수 당시 적용률을 불변 원본에 보존 | 정책 변경 후에도 기존 주문·재요청의 비용 유지 | 정책 변경이 이미 접수한 주문에는 적용되지 않음 |
| 엔진이 실제 수수료와 순수취액 계산 | 실행 잔액과 전달 결과의 계산 책임 일치 | 수수료 계약 변경은 명시적인 기록 형식 변경이 필요 |
| 체결별 변화량 전달 | 플랫폼의 후속 예약과 과거 수수료 보존 | 플랫폼은 순서·중복·예약 검증을 반드시 유지 |
| 수수료는 체결마다 소수 9자리 아래 절삭 | 현재 금액 저장 계약에 맞고 초과 청구 방지 | 잘게 나눈 체결과 단일 체결의 총 수수료가 다를 수 있음 |

양측 모두 결제 자산으로 청구하면 매수자의 추가 예약·부분 체결·가격 개선·취소 환급 계약이 더 복잡해진다. 현재 요구에는 수취 자산 차감이 더 단순하다. 적용률은 소수 6자리 이내, 0 이상 1 미만이며, 현재 정책은 양측 동일률이다. 주문 접수 시점이 다르면 양측의 고정 적용률이 다를 수 있다. 메이커·테이커 차등, 리베이트, 할인 자산은 구현하지 않는다.

## 구현과 코드

- [접수 원본 검증](../../src/domains/cex/services/order_receipt.rs): 플랫폼 `order_submissions.fee_rate`를 읽고 `engine_order_receipts`에도 보존한다. 고객·HTTP 요청이 수수료율을 지정하지 않는다.
- [주문 서비스](../../src/domains/cex/services/order_service.rs): 검증한 적용률을 `OrderEntry`로 전달한다.
- [매칭](../../src/domains/cex/engine/matcher.rs): 매수·매도 원본의 적용률을 `MatchResult`에 담는다. 신규·기존 주문 모두 유효한 적용률이어야 한다.
- [정확한 금액 연산](../../src/domains/cex/engine/amount.rs): 기존 `num-bigint`로 곱셈 후 고정 최소 단위에서 절삭한다. 일반 거래 대금은 기존처럼 정확히 표현되지 않으면 거부한다. 수수료만 명시적 절삭을 허용한다.
- [후보 정산](../../src/domains/cex/engine/balance_cache/settlement.rs): 기준 자산 수취량과 결제 자산 수취액에서 각각 차감한다. 오류는 전체 신규 주문 후보 확정 전에 발생한다.
- [출력](../../src/domains/cex/engine/runtime/output.rs): 절대 잔고 스냅샷과 실행 캐시 의존성을 제거하고 체결·양측 적용률·실제 수수료를 전달한다.
- 실행되지 않던 Rust 수수료 조회 서비스·저장소·모델, 적용률을 잃는 `order_to_entry` 변환을 제거했다. 기존 금융 테이블과 이력 SQL은 삭제하지 않았다.

코어는 독점 소유한 메모리만 변경한다. 입출력·설정 조회·공유 락을 추가하지 않았다. WAL 워커가 먼저 영속 커밋하고 출력 워커가 Kafka로 공개한다. 수수료는 WAL 체결 기록에 남는다. 별도 거래소 수익 계정의 복식 원장과 독립 대사는 후속 H1/H6 항목이다.

## 기록 형식과 실행 전 조건

저널 저장·기점 버전을 **2**로 올렸다. 예전 버전 1 기록을 수수료 0으로 해석하거나 변환하지 않는다. 버전 1 디렉터리를 열면 기존 기록을 덮어쓰지 않고 기동을 거부한다. 검증은 새 격리 디비·새 저널에서 수행했다.

플랫폼 원본과 엔진 접수 테이블에 필수 `fee_rate`가 필요하다. [접수 테이블 정의](../../schema/order_receipts.sql)는 새 환경의 스키마다. 이번 작업은 기존 실행 서비스 재시작·데이터 삭제·스키마 변환을 수행하지 않는다. 이전 금융 기록을 가진 환경에 새 바이너리를 바로 배포할 수 없다. 그 환경의 전환 기준을 별도로 확정해야 하며 저널 삭제나 플랫폼 잔액으로 우회 기동해서는 안 된다.

## 이해에 필요한 Rust 문법

`OrderEntry.fee_rate`는 TypeScript 객체 필드처럼 명령의 일부지만, Rust에서는 새 필드를 모든 생성 지점에서 명시해야 한다. 기존 수수료 없는 테스트·벤치는 0을 명시하며 제품 경로는 접수 증명의 적용률을 쓴다.

`BigInt`는 TypeScript의 `bigint`와 비슷하게 큰 정수로 중간 곱을 보존한다. 최소 단위 정수끼리 곱하고 나누므로 부동소수점 오차가 없다. `Result`와 `?`는 실패를 호출자에게 돌려주고, 후보 캐시에만 계산하므로 실패가 실제 오더북·실행 잔액을 부분 변경하지 않는다.

## 작성한 테스트

| 파일 | 함수 | 검증 |
| --- | --- | --- |
| [체결 파이프라인](../../src/domains/cex/engine/runtime/engine/settlement_pipeline_tests.rs) | `settlement_fees_follow_each_order_and_preserve_prior_deductions` | 원본별 적용률·연속 부분 체결·과거 수수료 유지·자산별 보존식 |
| 같은 파일 | `settlement_fee_rounding_and_invalid_rates_are_explicit` | 최소 단위 절삭·0 수수료·음수·100%·과도한 정밀도 거부 |
| 같은 파일 | `settlement_invalid_fee_keeps_book_and_accounts_unchanged` | 잘못된 적용률의 원본 보존·추가 수수료 예약 없이 거래 |
| [저널 회귀](../../src/domains/cex/engine/runtime/engine/journal_tests.rs) | `journal_fee_results_replay_and_duplicate_without_second_charge` | 실제 코어·기록·출력 워커와 재시작 후 재차감 방지 |
| 같은 파일 | `journal_fee_amount_mismatch_blocks_recovery_even_with_valid_checksum` | 검사 합이 정상이어도 실제 수수료 재계산과 다른 결과의 복구 거부 |
| 같은 파일 | `journal_old_fee_contract_is_rejected_without_rewriting_history` | 이전 형식 기동 거부·원본 버전 보존 |
| [접수 회귀](../../src/domains/cex/services/order_receipt/tests.rs) | `receipt_fee_rate_is_frozen_and_invalid_policy_is_rejected` | 접수 증명의 적용률 고정·잘못된 정책의 접수 롤백 |

기존 생성자·시작 검사·발행 예제와 [실제 출력 검증기](../../scripts/verify_engine_output.py), [접수 통합 검증기](../../scripts/verify_durable_acceptance.py)도 새 계약으로 갱신했다.

## 실행 명령과 결과

`python3 scripts/verify_order_safety.py`: 실행 소유 PostgreSQL에서 고유 테스트 **119개 통과**, 선택 필터 중복을 포함한 실행은 121회다. 전체 테스트 컴파일·제품 빌드도 통과했다. 공유 데이터베이스를 사용하는 기존 통합 테스트 전체를 실행한 결과는 아니다.

추가 프로세스·플랫폼 결과와 남은 한계는 [플랫폼 기록](../../../Cex-Platform-Service/docs/decisions/008-fee-authority-and-balance-deltas.md)을 함께 읽는다. 순수 계산 TPS나 실제 내구성 포함 처리량은 이번에 측정하지 않았다. 기존 미사용 코드 경고와 `solana-client`·`sqlx-core`의 미래 Rust 호환 경고는 남아 있다.

## 관련 커밋

- [엔진 구현·테스트 `9f441fa`](https://github.com/GiHoon1123/Cex-Engine/commit/9f441fa7078f45cf6e34c7c5e71230cdd345a1d3)
- [플랫폼 구현·테스트 `646c553`](https://github.com/GiHoon1123/Cex-Platform-Service/commit/646c553a6416f9ae957f9eae8798d5d0cc77df77)

두 저장소의 금융 이벤트 계약이 함께 바뀐다. 개별 프로세스만 먼저 새 버전으로 배포하는 검증은 수행하지 않았다.
