# 출금 원본의 예약·해제·완료와 최초 금융 거절을 자체 WAL에 결합

주문과 출금이 같은 사용 가능 잔액을 두 번 사용하지 않도록 출금별 불변 원본과 생명주기를 단일 코어에 추가했다. 실제 체인 전송·서명·고객/운영 승인은 플랫폼 책임으로 남기고, 엔진의 정확한 예약·종료·재전송·확정 출력과 자체 복원을 검증한다. 공통 사이클137·엔진 결정023이다.

## 문제와 기존 동작

입금은 외부 UUID와 경제적 원본을 자체 저널에 결합하지만 출금에는 같은 계약이 없었다. 원본 없는 잔액 감소나 일반 잠금 명령을 재시도하면 같은 출금을 두 번 차감하거나, 응답 유실 때 잠금을 잘못 풀 수 있다. 출금의 미래 체인 승인/서명을 엔진에 넣는 방식도 엔진의 책임과 맞지 않는다.

또한 HTTP의 일반409는 최초 잔액 부족과 완료 후 다른 단계 요청을 모두 포함할 수 있다. 이를 전부 플랫폼 반환 권한으로 삼으면 이미 외부로 지급한 돈을 다시 가용 잔액으로 만들 수 있다. 최초 예약의 금융 거절은 명확한 단독 결과를 갖도록 했다.

## 기존 제품과 공식 자료의 접근

2026-10-26 KST 확인한 [Coinbase 계정 holds API](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/accounts/get-single-account-hold)는 진행 주문과 출금 요청의 hold, 주문 취소/출금 완료에 따른 해제를 설명한다. 이 공개 계약의 예약/가용 구분을 참고하며 내부 DB/스레드/승인 정책은 추측하지 않는다.

기존 엔진의 단일 상태 소유, 자체 WAL 커밋 뒤 응답·Kafka 공개, 안정적 위치 기반 이벤트 ID, 직접 재생을 그대로 사용한다. 설치된 Decimal·Serde·Crossbeam·Tokio·SQLite·Kafka와 기존 정확 금액 함수를 재사용했고 Cargo 의존성/잠금 파일을 변경하지 않았다.

## 선택과 트레이드오프

| 선택 | 이점 | 비용·제약·대안 |
| --- | --- | --- |
| 기존 available/locked + 출금별 원본/상태 | 별도 잔액 체계 없이 주문과 출금의 이중 사용 차단 | 두 종류의 예약이 같은 합계에 있으므로 각 원본 몫을 정확히 지켜야 한다. 주문 부분 체결/취소·시장가 예산/수수료·서로 다른 출금 경합을 추가 검증 |
| reserve/release/complete 세 단계 | 예약→반환 또는 외부 유출의 종료를 명확히 함 | 플랫폼은 실제 전송 결과/승인에 맞는 단계를 선택해야 한다. 엔진 complete는 블록체인 전송 증명이 아님 |
| 같은 UUID의 소유자/자산/금액 고정 | 다른 대상·금액으로 재시도하는 오류 차단 | 키를 바꿔 불명을 신규 출금으로 만들 수 없다. 플랫폼의 멱등 원본 저장과 연결 필요 |
| 최초 유효 예약의 금융 실패도 보존 | 나중에 입금됐다고 같은 거절 출금이 실행되지 않음 | 새 업무는 새 원본/승인이 필요하다. 최초 거절 이벤트에만 반환 권한을 결합해야 함 |
| 성공한 같은 단계만 멱등, 종료 뒤 재예약 금지 | 해제/완료 상태를 늦은 reserve가 되살리지 않음 | 과거 reserve 요청도 현재 종료 뒤에는409다. 과거 성공과 현재 활성 예약을 혼동하지 않음 |
| 최초 금융 거절의 독립 Kafka 결과 | 일반 HTTP409와 금융 반환 근거 구분 | 저장/출력 이벤트 한 건이 추가된다. 거절 JSON만 보고 플랫폼 예약금을 풀지 않음 |
| 전용 금융 명령을 기존 큐/워커로 전달 | 단일 코어·공유 락0 유지, 새 스레드/디스크 대기 없음 | 기존 유한 큐/출력 부하를 공유한다. 플랫폼은 입력 권한/자산별 한도/외부 전송을 통제해야 함 |
| 입출금 HTTP의 정확 문자열 검증 공유 | 같은 검증을 두 경계에 복제하지 않음 | 두 API의 입력 규칙이 함께 바뀌므로 기존 입금 회귀를 유지. 새 공용 프레임워크 대신 작은 순수 함수만 사용 |
| 실행 이력이 있는 메모리로 새 genesis 거부 | 원본·예약 메타데이터를 잃는 새 계보 생성 방지 | 최초 잔액 반입과 상태 스냅샷은 다르다. 기존 자체 WAL 복구만 허용 |

## 상태 전이와 외부 계약

| 현재 상태 | reserve | release | complete |
| --- | --- | --- | --- |
| 원본 없음 | 정확 잔액 예약 또는 최초 금융 거절 보존 | 이번 명령 거절, 새 원본/반환 없음 | 이번 명령 거절, 새 원본/유출 없음 |
| RESERVED | 중복 성공, 새 금액/이벤트 없음 | 원래 몫만 가용으로 반환→RELEASED | 원래 잠금 몫만 차감→COMPLETED |
| RELEASED | 거절 | 중복 성공 | 거절 |
| COMPLETED | 거절 | 거절 | 중복 성공 |
| 최초 예약 REJECTED | 기존 금융 거절 유지 | 거절 | 거절 |

모든 단계는 같은 withdrawal_id/user_id/asset/amount를 요구한다. 다른 원본의 충돌·잘못된 단계·형식 오류는 최초 금융 거절 이벤트를 만들지 않는다. 없는 예약의 종료 거절은 그 위치의 명령 결과일 뿐 미래 UUID를 소비한 예약/해제 묘비가 아니다. 실제 원본 생성 뒤에는 정상 상태 기계가 적용된다. 플랫폼은 순서를 저장하고 예약 확정을 확인한 뒤 종료를 요청해야 한다.

내부 `POST /api/cex/withdrawals/reservations`는 action=reserve/release/complete와 원본을 받는다. 기존 서비스 키 필터와4096바이트 한도, 미지/중복 필드 거부, 금액 문자열 정확 검증, Cache-Control:no-store를 적용한다.

- 200 + DURABLY_RESERVED / DURABLY_RELEASED / DURABLY_COMPLETED: 이번 단계의 자체 저널 확정. Kafka 수신/플랫폼 금융 반영/실제 체인 확정과 다름.
- 409 + DURABLY_REJECTED: 이번 명령 거절의 자체 기록. 기존 예약·다른 단계·과거 외부 지급이 없다는 뜻이 아님.
- 503 + NOT_ACCEPTED: 이번 큐 전송 미접수. 과거 같은 원본의 미실행 증명 아님.
- 504 + OUTCOME_UNKNOWN: 큐 진입 후 시간 초과/응답 유실/저장 실패로 이번 결과 불명. 같은 원본/동작만 재확인.
- 400/413: 입력/크기 거절. 서비스 신원은 기존401의 빈 본문 계약.

확정 결과는 기존 order-events의 한 파티션에서 withdrawal_reserved/withdrawal_released/withdrawal_completed/withdrawal_rejected로 공개한다. 최초 금융 거절의 reason_code는 고정 RESERVATION_REJECTED이고 내부 오류 문자열은 공개하지 않는다. 이벤트 식별자·시각은 원래 저널 위치에서 재현한다. 발행은 적어도 한 번이며 플랫폼 영속 수신 키로 중복 반영을 막아야 한다.

## 구현과 실제 코드·문법

체인 주소·노드·서명키·KYC/승인 상태는 새 코어 자료형에 넣지 않았다. &mut EngineState를 실제 단일 코어만 소유하며, 새 공유 Mutex/RwLock·건별 스레드·디스크/네트워크 호출이 없다. 금액은 현행 엔진의9자리 정확 단위다. BTC 온체인8자리/네트워크·수수료/수신 주소는 플랫폼이 추가 검증해야 하며 ETH18자리는 아직 지원한 것이 아니다.

출처: [withdrawal.rs](../../src/domains/cex/engine/runtime/withdrawal.rs#L95), 현재 소스의 부분 발췌.

```rust
        match (&receipt.status, command.action) {
            // 같은 단계의 성공만 멱등이다. 이미 해제/완료한 예약을 재전송으로 다시 잠그지 않는다.
            (Status::Reserved, WithdrawalAction::Reserve)
            | (Status::Released, WithdrawalAction::Release)
            | (Status::Completed, WithdrawalAction::Complete) => return Ok(None),
            (Status::Reserved, _) => {}
            _ => anyhow::bail!("Withdrawal reservation is already finalized"),
        }
    } else {
        // 주문 취소의 미접수 원본과 같다. 아직 존재하지 않는 예약을 임의로 생성/해제하지 않는다.
```

Rust의 패턴은 현재 상태와 요청 동작을 함께 검사한다. 성공한 중복은 Ok(None)으로 새 금융 출력을 만들지 않는다. TypeScript의 판별 가능한 합 타입과 비교할 수 있지만 실제 &mut 소유권/상태 독점은 Rust의 빌림 규칙과 단일 워커 구조가 함께 보장한다.

출처: [withdrawal.rs](../../src/domains/cex/engine/runtime/withdrawal.rs#L115), 현재 소스의 부분 발췌.

```rust
            if let Err(error) = cache.lock_balance(funds.user_id, &funds.asset, funds.amount) {
                let reason = error.to_string();
                state.withdrawal_receipts.insert(
                    funds.withdrawal_id,
                    WithdrawalReceipt {
                        original: funds.clone(),
                        status: Status::Rejected(reason.clone()),
                    },
                );
                // 최초 금융 거절도 별도 결과다. HTTP의 다른 단계 충돌과 구분해 플랫폼이 안전하게 반영한다.
                return Ok(Some(OutputBatch::WithdrawalRejected {
                    funds: funds.clone(),
                    reason,
```

최초 금융 거절도 계산된 업무 결과이므로 OutputBatch를 보존한다. 실패를 성공 예약으로 바꾸는 것이 아니다. 다음 응답 분기가 이를 실제 거절로 반환하며 Kafka에는 최초 거절 결과가 한 번 생성된다. 같은 원본의 이후 재시도는 상태의 거절을 유지한다.

출처: [journal.rs](../../src/domains/cex/engine/runtime/journal.rs#L94), 현재 소스의 부분 발췌.

```rust
            Reply::Withdrawal(sender) => {
                // 확정 금융 거절은 Kafka 출력도 갖지만 HTTP 성공 예약으로 바꾸지 않는다.
                let _ = sender.send(match self.outcome {
                    Outcome::Failed(reason) | Outcome::Applied(Some(OutputBatch::WithdrawalRejected { reason, .. })) => Err(anyhow::anyhow!(reason)),
                    _ => Ok(()),
                });
            }
            Reply::Unit(sender) => {
```

계산/기록 성공과 금융 승인 성공은 다르다. Applied의 금융 거절 결과도 HTTP에서는 Rejected다. finish는 기존 기록 워커가 커밋한 뒤에만 호출하므로 저장 장애 때 확정 거절을 먼저 공개하지 않는다.

출처: [withdrawal.rs](../../src/domains/cex/engine/runtime/withdrawal.rs#L138), 현재 소스의 부분 발췌.

```rust
            let next = amount::subtract(old.locked, funds.amount)?;
            // 검사 후에만 한 필드를 반영한다. 주문 몫과 다른 출금의 예약 금액을 함께 차감하지 않는다.
            cache.get_balance_mut(funds.user_id, &funds.asset).locked = next;
        }
```

?는 부족/정밀도 오류 때 즉시 반환한다. 변경은 검사 후 한 필드에만 반영한다. 전체 locked 합계에는 주문과 다른 출금도 있으므로 해당 원본 금액만 차감한다. 소유권을 다른 스레드에 공유하지 않는다.

출처: [handle.rs](../../src/domains/cex/engine/runtime/handle.rs#L34), 현재 소스의 부분 발췌.

```rust
        self.send_balance(BalanceCommand::Withdrawal {
            command,
            response: sender,
        })
        .map_err(|_| WithdrawalError::NotAccepted)?;
        timeout(Duration::from_secs(3), receiver)
            .await
            .map_err(|_| WithdrawalError::OutcomeUnknown)?
            .map_err(|_| WithdrawalError::OutcomeUnknown)?
            .map_err(|error| WithdrawalError::Rejected {
                reason: error.to_string(),
```

원본과 응답 채널을 이동하고 요청 태스크만 비동기 대기한다. try_send 포화는 즉시 미접수이며, 큐 이후 시간 초과/채널 종료는 결과 불명이다. timeout이 코어 계산이나 이후 커밋을 취소하지 않는다.

출처: [withdrawal_tests.rs](../../src/domains/cex/engine/runtime/engine/journal_tests/withdrawal_tests.rs#L622), 현재 소스의 부분 발췌.

```rust
    // 시장가의 예산 잔여 해제가 출금 예약분까지 반환하면 이 단계의 금액이 달라진다.
    withdrawal.action = Action::Release;
    assert!(matches!(
        run(&mut state, &withdrawal),
        Outcome::Applied(Some(_))
```

실제 시장가 예산과1% 수수료 처리 뒤에도 출금100USDT가 남아야 한다. 그 원본을 해제한 뒤의 가용750/잠금0도 확인해 기존 주문의 자동 잔여 해제가 다른 목적의 예약을 풀지 않았는지 검사한다.

## 작성한 테스트

신규 Rust 함수28개다. 각 함수는 하나 이상의 입력/실패 시나리오를 담고, 반복 입력을 별도 함수 수로 부풀리지 않는다.

| 파일/대표 함수 | 시나리오·기대 결과 |
| --- | --- |
| withdrawal_tests.rs의 순수 계산 | 정확 예약/해제/완료·다른 주문/출금 몫 보존·종료 재활성 금지·변경 원본 거부·유효 금융 거절의 영구 보존·없는 예약 종료·잘못된 ID/자산/정밀도·오버플로 시 부분 변경 없음 |
| order_fills_and_cancellation_preserve_the_separate_withdrawal_share | 실제 매칭·부분 체결·기존 주문 취소 뒤 출금 몫만 남고 완료 시 그 몫만 유출 |
| withdrawal_safety_share_survives_market_budget_and_fee_completion | 실제 시장가 예산/수수료/자동 종료 후 출금 예약 보존 |
| withdrawal_safety_distinct_concurrent_reservations_cannot_spend_the_same_available | 서로 다른 UUID 두 요청이 같은 잔액을 경쟁하면 한 건만 성공·자체 복원 금액 일치 |
| withdrawal_worker_concurrent_same_reservation_publishes_only_once_and_restarts | 같은 UUID 동시 요청은 한 금액 이동·한 원본 출력, 재시작 뒤 중복/해제 유지 |
| withdrawal_resolve_race_has_one_winner_and_no_negative_balance | 실제 코어에서 release/complete 경합 한 건만 성공 |
| withdrawal_journal_failure_never_confirms_or_publishes / withdrawal_rejection_with_failed_recording_is_unknown_and_has_no_financial_result | 성공 계산·금융 거절 계산 모두 실제 기록 실패면 결과 불명·외부 출력 없음·복원 전 상태 |
| withdrawal_timeout_can_commit_later_without_creating_a_second_reservation | 응답 시간 초과 뒤 커밋해도 같은 원본 재전송은 한 예약/한 고유 결과 |
| withdrawal_output_failure_replays_the_exact_original_event | 출력 실패/커서 미진행 뒤 같은 이벤트 ID/내용/시각 재발행 |
| withdrawal_http_waits_for_the_actual_journal_commit_before_success_or_output | 실제 SQLite 다른 연결로 쓰기를 막으면 HTTP 성공/금융 출력 없음, 해제 뒤 확정 |
| withdrawal_http_terminal_state_survives_recovery / withdrawal_insufficient_funds_is_durable_only_after_the_record_is_committed | 종료와 최초 거절의 실제 저널/재시작 및 커밋 전 확정 거절 금지 |
| withdrawal_closed_or_full_input_is_not_an_accepted_command | 실제 유한 채널 포화/닫힘에서 동기 대기 없이 미접수, 기존 입력 보존 |
| routes/tests/withdrawal.rs7개 | 실제 라우터·벤치 코어의 단계/중복/원본 충돌·서비스 신원·명시적 입력/중복 필드/지수·반올림/숫자 금액 거부·본문 크기 |

라우터7개는 저장 워커 없는 벤치이므로 그 성공을 영속 확정 증거로 세지 않는다. 별도 실제 저널/HTTP 시험과 아래 실제 프로세스 시험이 확정 경계를 검증한다. 상속한 기존 SOL 단위 시험 자료는 다중 자산/기존 회귀 입력이며 SOL 서비스를 다시 활성화한 것이 아니다. 실제 프로세스는 BTC를 사용한다.

전체 함수 출처: [withdrawal_tests.rs](../../src/domains/cex/engine/runtime/engine/journal_tests/withdrawal_tests.rs)

- `withdrawal_reserve_release_and_duplicates_preserve_other_holds`
- `withdrawal_complete_debits_only_the_original_hold_once`
- `withdrawal_identifier_never_changes_customer_asset_or_amount`
- `withdrawal_insufficient_reservation_is_permanent_even_after_new_funds`
- `withdrawal_resolution_cannot_create_an_unknown_reservation`
- `invalid_withdrawal_never_consumes_identity_or_creates_accounts`
- `withdrawal_locked_overflow_preserves_both_balances`
- `order_fills_and_cancellation_preserve_the_separate_withdrawal_share`
- `withdrawal_history_cannot_be_silently_reseeded_as_genesis`
- `withdrawal_worker_concurrent_same_reservation_publishes_only_once_and_restarts`
- `withdrawal_resolve_race_has_one_winner_and_no_negative_balance`
- `withdrawal_journal_failure_never_confirms_or_publishes`
- `withdrawal_output_failure_replays_the_exact_original_event`
- `withdrawal_timeout_can_commit_later_without_creating_a_second_reservation`
- `withdrawal_http_waits_for_the_actual_journal_commit_before_success_or_output`
- `withdrawal_http_terminal_state_survives_recovery`
- `withdrawal_insufficient_funds_is_durable_only_after_the_record_is_committed`
- `withdrawal_closed_or_full_input_is_not_an_accepted_command`
- `withdrawal_safety_share_survives_market_budget_and_fee_completion`
- `withdrawal_safety_distinct_concurrent_reservations_cannot_spend_the_same_available`
- `withdrawal_rejection_with_failed_recording_is_unknown_and_has_no_financial_result`

전체 함수 출처: [withdrawal.rs](../../src/routes/tests/withdrawal.rs)

- `withdrawal_http_reserve_and_release_are_exact_and_idempotent`
- `withdrawal_http_completion_cannot_be_released_or_reserved_again`
- `withdrawal_http_changed_original_never_spends_another_account`
- `withdrawal_http_rejects_malformed_unknown_and_duplicate_fields`
- `withdrawal_http_never_rounds_or_coerces_the_amount`
- `withdrawal_http_requires_the_service_identity`
- `withdrawal_http_rejects_invalid_identity_and_oversized_body`

## 실행 명령과 실제 결과

```sh
python3 scripts/verify_order_safety.py
python3 scripts/verify_withdrawal_reservations.py
cargo test --lib --locked --offline -- --list
```

macOS·설치 Rust 도구체인·소유 PostgreSQL16/Kafka3.9.1, cargo --locked 빌드. .local/cycle137-proof의 최종 실행은 **고유209개/247회**, 실패·제외0이다. 라이브러리 목록206개 전부 포함, 기존129의181개 누락0·신규28개를 이름으로 대조했다. 전체 시험 컴파일/제품 빌드와 Python 구문·현재 발췌/링크·변경 파일 형식/git diff --check를 확인했다. 과거 실행의 통과 수를 더하지 않았다.

실제 프로세스 실행기는 소유 최소 초기 DB·별도 Rust 바이너리·실제 TCP·자체 SQLite WAL·한 파티션 Kafka를 사용한다. 잘못된 서비스 키는 저널에 들어가지 않는다. 예약 뒤 SIGKILL·초기 DB pause→같은 WAL 복원→해제/완료/부족 거절→추가 입금 뒤 같은 거절 보존→다시 SIGKILL/복원과 중복 재확인을 수행했다. Kafka의 고유6개 금융 결과가 독립 읽기 전용 감사 API의 확정6개와 ID/본문/시각까지 같았다. 원시 발행 중복은 같은 ID/내용으로 검증하며 정확히 한 번 발행이라 주장하지 않는다. 플랫폼의 최초 DB 잔액은 바뀌지 않았다.

이것은 플랫폼 금융 소비자·출금 승인·실제 Bitcoin Core 서명/전송 검증이 아니다. 실제 출금이 완료된 서비스라고 표시하지 않는다. 소유 컨테이너/프로세스만 제거했고 기존5개 서비스와 봇0을 유지했다. 디스크/호스트 손실 복제·백업은 기존 지시대로 문서 조건만 유지한다.

개발 중 두 신규 fixture 오류(다른 자산에 입금한 시험, Decimal 자체에 표현되지 않는 최대값)를 수정했다. 전체 라이브러리의 직접 실행은 DATABASE_URL 미설정으로47개가 준비 단계에서 실패했고155개가 통과했다. 실제 DB에 연결되지 않았으며 공식 격리 실행기로 다시 검증했다. 첫 프로세스 실행기의401 빈 본문을 JSON으로 읽던 시험 어댑터 오류도 기존 빈401 계약에 맞췄다. 제품에 폴백을 넣지 않았고 해당 초기 로그를 최종 결과와 분리해 보존했다. 최초 금융 거절의 독립 결과를 보완한 뒤 최종 전체 선택/실제 프로세스를 다시 실행했다.

전체 저장소 cargo fmt를 통과했다고 주장하지 않는다. 기존 형식 차이를 일괄 정리하지 않고 새 모듈/실제 수정한 핸들의 형식만 검사했다. 기존 unused/dead_code/SQLx0.6 향후 호환 경고와 실제 운영 성능/장애 부하 검증은 남는다. 기존1M/s 수치를 재사용하지 않았다.

## 다음 연결과 한계

다음 플랫폼 출금 원본/영속 예약·인증/심사·출력·네 종류 금융 수신/원장·독립 대사를 연결한다. 고객 프론트는 계속 플랫폼만 호출하며 새 엔진 경로를 직접 공개하지 않는다. 현재 플랫폼은 새 출금 결과 소비를 아직 구현하지 않았으므로 고객 출금 API나 자동 전송을 활성화하지 않는다.

출금 전달 이력 표를 추가할 때 최초 기동의 현재 이력 검사에도 같은 표를 필수로 연결해야 한다. 새 경로/플랫폼 잔액만으로 자체 WAL을 대신할 수 없다. 결과 불명·전송/서명 재시도·실제 BTC의 고정 지급 원본·노드 확정/재조직·손실/정정·다른 출처 입금/반환·전체 검토·프론트·마지막 ETH는 후속이다.

## 관련 커밋과 증적

- [구현 0f55c4d](https://github.com/GiHoon1123/Cex-Engine/commit/0f55c4d12ae6aad98c16f373e6fd33141b1106ad)
- [기계 판독 증적](../reference/023-withdrawal-reservations-verification-2026-10-26.json)
- [자체 저널/복원](009-durable-journal-recovery.md)·[최초 기동](022-first-bootstrap-current-history.md)
