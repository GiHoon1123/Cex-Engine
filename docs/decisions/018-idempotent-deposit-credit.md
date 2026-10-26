# 입금 반입 원본을 한 번만 실행하고 WAL에서 중복 방지 상태 복원

플랫폼이 같은 입금 원본을 재시도해도 실행 잔액과 금융 결과를 한 번만 만든다. 고객·자산·금액이 바뀐 같은 식별자는 거부하고, 자체 저널 재생으로 이 판단을 복원한다. 공통 사이클075·엔진 결정018이다. 이번 단계는 내부 명령/실제 워커 검증이며 HTTP·체인 감지·플랫폼 원장은 후속이다.

후속 [021 내부 HTTP](021-internal-deposit-credit-http.md)에서 기존 명령 핸들을 서비스 신원 아래 연결했다. 아래 미공개 설명은 당시 범위다. 플랫폼 금융 허가·AVAILABLE 반영은 계속 후속 작업이다.

## 문제와 기존 동작

기존 Balance 명령은 잔액 변화량만 받는다. 응답 유실 후 같은 증감 요청을 다시 보내면 입금을 다시 더할 수 있다. [플랫폼074 고객 주소](../../../Cex-Platform-Service/docs/decisions/074-customer-bitcoin-deposit-address.md)를 입금 처리와 연결하기 전에 영속 입금 원본의 식별자와 금융 내용이 결합된 별도 명령이 필요하다.

기존 Balance 명령의 저널 기록과 금융 이력을 삭제하거나 입금 식별자를 추측해 덧붙이지 않았다. 입금과 관련 없는 기존 기록의 의미는 그대로 유지한다. 플랫폼의 사용하지 않는 동기화 helper와 구형 HTTP 증감 경로 정리는 새 실제 반입 경로를 연결하면서 별도로 확인한다. 구형 경로를 신규 입금의 폴백으로 사용하지 않는다.

## 기존 제품과 공식 자료의 접근

2026-10-25 [Stripe 멱등 요청](https://docs.stripe.com/api/idempotent_requests)은 안정적인 요청 키와 원래 파라미터 대조로 응답 유실 재시도를 처리한다. 이번에는 이 원칙을 적용하지만 Stripe API와 같은 보존기한/응답 캐시를 구현했다고 주장하지 않는다. 금융 반입 식별자는 임의로24시간 후 버리지 않는다.

[Kafka4.1 메시지 전달 보장](https://kafka.apache.org/41/design/design/#message-delivery-semantics)은 브로커 내 전달과 외부 저장 반영의 경계를 구분한다. producer의 중복 방지 설정만으로 엔진의 메모리 잔액이나 플랫폼 DB의 정확히 한 번 반영이 완성되지 않는다. 기존 [009 WAL 확정](009-durable-journal-recovery.md)의 명령·결과·출력 순서를 확장한다. 이미 설치된 uuid·Decimal·serde·HashMap·crossbeam·oneshot을 사용했고 의존성은 추가하지 않았다.

## 선택과 트레이드오프

| 선택 | 얻는 점 | 비용·대안·제약 |
| --- | --- | --- |
| DepositCredit의 원본 UUID+고객+자산+금액 결합 | 같은 입금의 재시도와 식별자 오용을 구분 | 플랫폼이 같은 체인 입금에 안정적인 원본 UUID 하나를 영속 부여해야 함. 노드 txid/vout 검증·재조직 판단은 이 명령이 하지 않음 |
| 코어 소유 HashMap과 WAL 재생 | 코어의 DB 조회·공유 락 없이 중복 여부 판단 | 입금 수에 비례하는 메모리·시작 재생 비용. 지금은 안전성을 위해 식별자를 임의 파기하지 않으며 향후 측정 없는 별도 캐시/만료 정책을 만들지 않음 |
| 정상 형식 명령의 계산 실패도 내용/사유 보존 | 실패 중복을 성공으로 바꿔 답하거나 다른 금액으로 재사용하지 않음 | 범위 초과 후 잔액이 달라져도 같은 원본은 계속 실패. 정당한 재처리는 별도 원본·승인 근거가 필요하며 자동으로 ID를 갈아 재시도하지 않음 |
| 중복 성공은 새 금융 출력 없음 | 같은 입금이 Kafka/플랫폼에서 또 반영될 원인을 줄임 | 중복 요청 자체는 저널에 남음. 원래 결과의 브로커 수신 불명은 원래 저널 위치의 출력 재발행이 처리 |
| 별도 BalanceCommand 분기 | 기존 단일 코어와 기록/출력 워커 수명 유지 | 우선순위는 기존 잔액 큐 규칙을 따름. 네트워크 도착 시각의 전역 순서가 아니라 실제 코어 실행 순서가 저널의 기준 |

## 구현과 보장 경계

DepositCredit은 deposit_id, user_id, asset, amount를 받는다. 코어는 체인·KYC·승인·주소·확정 수를 모른다. UUID nil/고객0/빈 또는 공백 경계 자산/128바이트 초과 자산/0·음수/기존9자리 정밀도 범위 밖 금액은 원본 상태를 만들지 않고 거부한다. BTC의8자리나 네트워크 규칙은 플랫폼이 검증해야 한다. 대소문자를 자동 보정하지 않는다.

정상 형식 명령은 현재 available에 정확히 더할 수 있는지 먼저 검사한다. 검증이 끝난 뒤 available만 변경하며 locked는 보존한다. 결과는 DepositCredited로 기록 워커에 넘긴다. 정상 형식의 계산 실패도 원본/사유를 코어 인덱스에 남겨 같은 실패를 재현한다. 반면 형식/정밀도 검증에서 거부된 입력은 인덱스를 차지하지 않으며 저널에는 실패 명령 자체가 기록된다.

기록 워커가 자체 저널에 명령/결과를 영속 확정한 뒤 성공 응답을 보낸다. 출력 워커는 확정된 결과만 읽고 같은 금융 토픽으로 deposit_credited를 보낼 수 있다. 출력의 event_id는 기존 engine_id:sequence:0이며 timestamp도 원래 기록을 사용한다. 새 공개 이벤트에는 입금 ID·고객·자산·금액을 포함하고 잔액 스냅샷을 보내지 않는다.

입력 큐 포화는 즉시 오류다. 이미 접수한 명령의3초 응답 시간 초과/응답 채널 유실은 결과 불명이므로 **같은 입금 ID와 금융 내용**으로 재조회/재시도해야 한다. 금액이나 ID를 바꿔 회피하지 않는다. 출력 포화 시에는 기존 유한 큐 계약에 따라 코어 계산을 잠시 멈추며, 디스크 작업이나 blocking send를 코어에 추가하지 않았다. 무제한 처리량/대기 없음으로 설명하지 않는다.

오더북·잔액·입금 인덱스는 동일한 엔진 워커만 소유한다. 요청자는 명령 핸들만 가지며 디스크/브로커 입출력은 기존 장수 워커가 담당한다. 새 shared Mutex/RwLock·입금별 스레드/런타임·코어 네트워크 호출은 없다. 재시작은 자체 저널을 재생해 성공/실패 원본과 중복 인덱스를 복원한다. 플랫폼 DB로 우회하지 않는다.

## 실제 코드와 필요한 문법

출처: [deposit.rs](../../src/domains/cex/engine/runtime/deposit.rs#L41), 현재 소스의 부분 발췌.

```rust
    if let Some(receipt) = state.deposit_receipts.get(&credit.deposit_id) {
        ensure!(
            &receipt.original == credit,
            "Deposit id conflicts with original financial content"
        );
        if let Some(reason) = &receipt.rejection {
            anyhow::bail!("{}", reason);
        }
        // 중복 요청 자체는 저널에 남지만 새 금융 출력과 잔액 증가는 만들지 않는다.
        return Ok(None);
    }
    ensure!(
        !credit.deposit_id.is_nil() && credit.user_id > 0,
```

&mut EngineState는 이 호출 동안 상태에 대한 독점 변경 접근이다. TypeScript 객체 참조처럼 여러 작업에 공유한 뒤 락을 잡는 구조가 아니다. Option의 Some이면 이전 원본을 대조하고, 성공 중복은 Ok(None)으로 새 출력을 만들지 않는다. 이전 실패는 같은 사유로 다시 실패한다.

출처: [deposit.rs](../../src/domains/cex/engine/runtime/deposit.rs#L73), 현재 소스의 부분 발췌.

```rust
        let next = amount::add(old, credit.amount)?;
        // 범위 검증이 끝난 뒤에만 원본 상태를 바꾼다. 잠금 잔액은 건드리지 않는다.
        cache
            .get_balance_mut(credit.user_id, &credit.asset)
            .available = next;
        Ok(Some(OutputBatch::DepositCredited(credit.clone())))
    })();
```

?는 계산 실패를 즉시 반환한다. 정확한 다음 잔액을 구하기 전에 get_balance_mut으로 계정을 만들거나 기존 값을 수정하지 않는다. clone은 입금 원본을 소유하는 결과/인덱스에 필요한 값 복사이며 코어 상태를 외부로 공유하는 것이 아니다.

출처: [handle.rs](../../src/domains/cex/engine/runtime/handle.rs#L49), 현재 소스의 부분 발췌.

```rust
    pub async fn credit_deposit(
        &self,
        credit: super::DepositCredit,
    ) -> std::result::Result<(), super::DepositCreditError> {
        use super::DepositCreditError;
        let (sender, receiver) = oneshot::channel();
        self.send_balance(BalanceCommand::CreditDeposit {
            credit,
            response: sender,
        })
        .map_err(|_| DepositCreditError::NotAccepted)?;
        // 큐 진입 뒤에는 응답 유실/시간 초과를 거절로 바꾸지 않는다. 메모리 계산이나
        // 저널 커밋이 이미 진행됐을 수 있으므로 금융 원본을 유지한 재확인만 허용한다.
        timeout(Duration::from_secs(3), receiver)
```

요청의 oneshot 송신 권한을 명령과 함께 넘긴다. async 요청 작업이 영속 응답을 기다리지만 코어 스레드는 기록 워커로 응답 권한을 이전한 뒤 다음 계산을 수행한다. 시간 초과는 이미 반영된 명령을 취소하거나 되돌리지 않는다.

## 작성한 테스트

[engine/journal_tests.rs](../../src/domains/cex/engine/runtime/engine/journal_tests.rs)에8개를 추가했다. 별도 저장소/네트워크를 생략한 계산 시험과 실제 코어·저널·출력 워커 시험을 구분한다. 시험 sink는 실제 Kafka 브로커가 아니며 별도의 기존 시장 연결 시험에서 브로커를 사용했다.

| 함수 | 검증 시나리오·기대 결과 |
| --- | --- |
| deposit_once_preserves_locked_balance_and_duplicate_has_no_financial_output | 새 입금은 available만1회 증가, locked 보존, 중복은 새 출력 없음 |
| deposit_identifier_cannot_change_user_asset_or_amount | 같은 UUID의 고객/자산/금액 변경 실패, 타 계정/자산 미생성 |
| invalid_deposit_never_creates_account_or_consumes_identifier | 잘못된 ID/고객/자산·음수/0/정밀도 초과 거부, 계정/인덱스 미생성 |
| valid_deposit_overflow_keeps_original_and_repeated_failure | 범위 초과 시 원본 잔액 보존, 후속 잔액 변화 뒤에도 같은 실패, 내용 변경 거부 |
| deposit_real_worker_concurrent_retries_and_restart_credit_once | 실제 워커의 동시2요청에서 잔액·출력1회, 자체 WAL 재시작 뒤 중복/내용 변경에도 증가 없음 |
| deposit_output_failure_republishes_same_event_after_recovery | 출력 수신 확인 유실을 주입하고 자체 재시작. 원래 event_id/시각/원문 그대로1회 재발행 |
| deposit_journal_failure_never_acknowledges_or_publishes | 기록 INSERT 실패 시 성공 응답/출력0건, WAL 순번0·복구 잔액 미생성 |
| deposit_failed_receipt_replays_as_failure_and_keeps_original_payload | 정상 형식의 계산 실패를 WAL에 보존·복원한 뒤 같은 실패와 내용 변경 거부 유지 |

## 실행 명령과 결과

```bash
/Users/dustin/.cargo/bin/cargo test --bin cex-engine deposit --no-default-features
python3 scripts/verify_order_safety.py
```

다른 저장소의 실제 연결 회귀는 Cex-Platform-Service에서 python3 scripts/verify_market_path.py로 실행했다. 상위 실행은 개인 CDD/EDD/Didit/메일/BITCOIN 환경을 제거했고 실행기가 소유 PG·Kafka·자체 저널·엔진 프로세스와 bot0 설정을 사용했다.

최초 deposit 필터 컴파일 실행은 테스트 추가 전이므로0개였다. 동작 검증으로 집계하지 않는다. 신규7개 시험 통과 뒤 실패 원본 재복구1개를 보강했고 최종 신규8개를 포함한 안전성 실행기를 돌렸다. **Rust 고유147개 통과/실행160회, 실패·제외0개**다. journal/deposit 등 겹치는 선택 실행을 고유 개수에 중복 합산하지 않았다. 전체 테스트 컴파일과 제품 build도 통과했다. 별도 실제 시장 연결은 **1개 통과**, PG/Kafka 제거 확인이다. 신규 입금 이벤트를 플랫폼 원장에 소비한 시험과 혼동하지 않는다.

제품 bin 빌드 경고59개(중복8개 포함), SQLx0.6.3 미래 호환 경고가 있다. 신규 내부 입금 핸들/변형은 아직 HTTP 호출자가 없어 미사용 경고에 포함된다. 경고 숨김 설정을 추가하지 않았다. 과거1M TPS 수치를 새 경로에 재사용하지 않으며 새 처리량/지연 벤치를 실행하지 않았다.

## 다음 연결과 남은 한계

새 명령은 내부 런타임에서 완결 검증했지만 현재 HTTP 반입 경로와 플랫폼의 deposit_credited 소비/원장 반영은 아직 없다. 체인 관찰·확정/재조직·보류·입금 원본 UUID 부여도 후속이다. 이 기능만으로 실제 BTC 입금·고객 거래 가능 잔액 반영이 완성됐다고 표시하지 않는다.

현재 SQLite 스키마/저널 버전4와 과거 명령 변형은 유지하며 새 명령을 추가했다. 과거 기록을 바꾸거나 호환 변환기를 만들지 않는다. 새 명령을 모르는 과거 바이너리는 그 기록을 읽을 수 없으므로 운영에서 임의로 구버전으로 되돌리지 않는다. 외부 복제/백업은 기존 지시에 따라 필요 조건만 문서화하며 실제 인프라는 배포하지 않았다.

## 커밋과 증적

- [구현 404e5a9](https://github.com/GiHoon1123/Cex-Engine/commit/404e5a9c23049b3213ede919dfba8d58760a5bee)
- [격리 실행/개별 테스트 증적](../reference/018-idempotent-deposit-credit-verification-2026-10-25.json)

현행092 연결: [020 명시적 입금 오류](020-typed-deposit-outcomes.md)에서 핸들 반환 타입을 갱신했다. 위 발췌는 현행 코드이며075 당시 실행과 구분한다.075의 같은 줄 기반 집계147/160은 보존 로그 재집계에서 고유151/실행164로 정정됐다. 과거 JSON 원본은 유지한다.
