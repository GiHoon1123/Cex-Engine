# 입금의 이번 미접수·영속 거절·결과 불명을 타입으로 구분하기

요청 핸들이 큐 전송 실패, 저널에 확정한 거절, 응답 유실/시간 초과를 서로 다른 오류 타입으로 반환한다. 문자열을 해석해 금융 보상 여부를 결정하지 않고 같은 입금 원본으로 재확인할 경계를 설명한다. 공통 사이클092·엔진 결정020이다.

후속 [021 내부 HTTP](021-internal-deposit-credit-http.md)에서 기존 명령 핸들을 서비스 신원 아래 연결했다. 아래 미공개 설명은 당시 범위다. 플랫폼 금융 허가·AVAILABLE 반영은 계속 후속 작업이다.

## 문제와 기존 동작

018의 입금은 같은 ID/고객/자산/금액을 한 번만 반영하고 저널 뒤 응답한다. 하지만 반환 타입은 모든 실패가 anyhow::Error라 호출자가 확정 거절과 결과 불명을 구분하려면 문구에 의존할 수 있었다. 특히 계산은 거절됐어도 그 거절을 기록하는 디스크 작업이 실패하면 요청자는 영속한 거절을 받은 것이 아니다.

이번 변경은 요청 핸들의 반환 계약이다. 코어 계산·독점 상태·단일 스레드·채널·저널 명령/결과 직렬화·기록/출력 워커를 바꾸지 않는다. 새 HTTP 공개/플랫폼 원장 해제는 아직 연결하지 않았다.

## 기존 제품과 공식 자료의 접근

AWS의 [안전한 멱등 재시도](https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/)는 호출자 식별자와 의도를 보존하며 응답 유실 재시도를 다룬다.018의 안정 원본을 그대로 사용하고, 이 변경에서 재시도 때 ID를 새로 만드는 경로를 추가하지 않는다. 동일 API의 보존 기간이나 내부 구현을 복제했다는 의미는 아니다.

설치된 Cargo.lock의 thiserror1.0.69와 [해당 버전 문서](https://docs.rs/thiserror/1.0.69/thiserror/)를 확인했다. derive로 표준 Error/Display를 구현하고 enum 변형으로 분기한다. 이미 사용하는 anyhow/oneshot/timeout을 활용하며 새 의존성은 없다.

## 선택과 트레이드오프

| 선택 | 이점 | 비용·제약 |
| --- | --- | --- |
|3개 오류 변형 | 문구 파싱 없이 전송/확정 거절/결과 불명 구분 | 반환 타입을 사용하는 Rust 호출자는 새 Result를 다뤄야 함. 구형 반환 helper/호환 분기를 두지 않음 |
|확정 거절의 설명 문자열 유지 | 원래 WAL 계산 결과/재생 의미를 바꾸지 않고 학습/원인 확인 가능 | 사유 문자열은 기계 판단 코드가 아님. 외부 API에 원문을 무조건 노출해서는 안 됨 |
|3초 뒤 결과 불명 | 느린 저장/응답 유실을 잘못된 실패 보상으로 바꾸지 않음 | 호출자는 원본을 보존하고 재확인해야 하며 대기/재처리 업무가 필요 |
|핸들만 변경 | 코어의 추가 락/계산·분기·명령별 워커 비용을 만들지 않음 | HTTP·플랫폼의 응답 계약/수신은 후속 연결 필요 |
|오류 세부 분류를 추측해 늘리지 않음 | 현재 필요한 금융 확정 경계를 가장 작은 타입으로 표현 | 거절 종류별 자동 조치는 지금 제공하지 않음. 영속 사실 없이 문자열로 추론하지 않음 |

사용하지 않는 코드의 호환 유지와 금융 저널의 의미 보존은 구분한다. 과거 금융 기록을 지우거나 새 거절 스키마로 변환하지 않았다. 오류 분류를 위해 코어 결과를 다시 계산하거나 공유 상태를 추가하지도 않았다.

## 오류 계약

| 변형 | 확정할 수 있는 사실 | 호출자의 처리 |
| --- | --- | --- |
|NotAccepted | 이번 try_send가 큐에 들어가지 않음. 닫힘/포화/연결 종료 포함 | 과거 같은 입금이 실행되지 않았다는 뜻은 아니다. 안정 원본을 유지 |
|Rejected { reason } | 요청한 명령의 거절을 자체 저널에 확정한 뒤 응답받음 | 현재 명령의 확정 거절로 다룸. 다른 금액/ID로 자동 회피하지 않음 |
|OutcomeUnknown | 큐 진입 뒤 응답 유실/3초 시간 초과 | 반영/거절 모두 가능. 원본을 보존해 같은 ID/내용으로 재확인 |

성공도 기존처럼 영속 커밋 뒤에만 반환한다. 순수 벤치 모드는 원래부터 저널을 생략하는 시험 경로이므로 이 타입만 보고 내구성 증명으로 사용할 수 없다. 기록 워커는 append/commit 성공 뒤 Pending.finish를 호출하며, 계산 거절 후 기록 실패면 송신자가 사라져 OutcomeUnknown이 된다.

## 구현과 이해에 필요한 코드

출처: [deposit.rs](../../src/domains/cex/engine/runtime/deposit.rs#L20), 현재 소스의 부분 발췌.

```rust
#[derive(Debug, thiserror::Error)]
pub enum DepositCreditError {
    #[error("이번 입금 명령 전송이 접수되지 않았습니다")]
    NotAccepted,
    #[error("입금 결과를 확정할 수 없습니다. 같은 식별자와 금융 내용으로 재확인하세요")]
    OutcomeUnknown,
    /// 이 변형은 저널 커밋 이후 받은 거절에만 사용한다. 이유는 설명용이며 분기에 사용하지 않는다.
    #[error("자체 저널에 확정된 입금 거절: {reason}")]
    Rejected { reason: String },
}

```

Rust enum은 서로 배타적인 변형을 정의한다. TypeScript의 {kind: "notAccepted"}|{kind:"outcomeUnknown"}|{kind:"rejected",reason:string} 같은 판별 가능한 합 타입과 비교할 수 있다. thiserror는 오류 표시 구현을 생성하며 새로운 런타임/스레드를 만들지 않는다.

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
            .await
            .map_err(|_| DepositCreditError::OutcomeUnknown)?
            .map_err(|_| DepositCreditError::OutcomeUnknown)?
            // 기록 워커의 finish는 커밋 뒤에만 호출된다. 계산 거절도 저장에 실패하면
            // 이 분기에 도달하지 않고 위의 응답 유실로 처리된다.
            .map_err(|error| DepositCreditError::Rejected {
                reason: error.to_string(),
            })
    }
    /// 닫힘은 정상 정지 또는 출력 실패로 더 이상 명령을 실행하지 않음을 뜻한다.
    /// 브로커 연결이나 프로세스 장애 복구 가능성 전체를 검증하는 상태는 아니다.
```

표준 Result<(),DepositCreditError>는 성공 시 값이 없는 ()와 명시적 오류 중 하나다. map_err는 실패 변형만 바꾸며 ?는 그 실패를 즉시 반환한다. timeout이나 수신 채널 오류는 동일한 결과 불명이고, 수신된 금융 거절만 Rejected로 분류한다. async 요청의 대기는 코어 스레드의 디스크 대기가 아니다.

출처: [store.rs](../../src/domains/cex/engine/runtime/journal/store.rs#L328), 현재 소스의 부분 발췌.

```rust
        store.append(&group, &mut sequence).map_err(|error| {
            eprintln!("journal_commit_failed reason={error:#}");
            error
        })?;
        finish.progress.durable.store(sequence, Ordering::Release);
        for item in group {
            item.finish();
        }
    }
    finish.successful = true;
```

기록 성공 뒤에만 item.finish가 호출된다. 이번에 이 구현을 변경하지 않았으며 신규 실제 워커 시험으로 타입 분류가 이 순서를 따르는지 확인했다. 호출자 시간 초과가 이미 진행 중인 커밋을 취소하지 않는다.

## 작성한 테스트

[handle.rs](../../src/domains/cex/engine/runtime/handle.rs)와 [journal_tests.rs](../../src/domains/cex/engine/runtime/engine/journal_tests.rs)에 신규6개를 작성했다.

| 함수 | 시나리오·기대 결과 |
| --- | --- |
|deposit_full_input_queue_is_not_accepted_without_consuming_existing_command | 포화 입력 큐는100ms 외부 제한 안에 NotAccepted, 기존 대기 명령 유지 |
|deposit_closed_or_disconnected_input_is_not_accepted | 닫힘 표시/수신자 종료는 NotAccepted·큐 미삽입 |
|deposit_lost_response_after_queue_admission_is_outcome_unknown | 명령 소비 뒤 응답 소유자 유실은 OutcomeUnknown |
|deposit_typed_rejection_waits_for_commit_and_survives_restart | 실제 단일 코어가 범위 초과를 계산해도 응답 보류. 실제 저널 append 뒤 Rejected·재시작 뒤 같은 사유/원본 잔액 |
|deposit_timeout_can_commit_later_and_retry_still_credits_once |3초 응답 시간 초과→그 뒤 실제 커밋→자체 재시작/같은 ID 재시도. 잔액/금융 출력1회 |
|deposit_calculation_rejection_with_failed_journal_is_outcome_unknown | 계산 거절과 동시에 기록 장애. 확정 거절을 반환하지 않고 OutcomeUnknown·저널 순번0/입금 원본0 |

기록 순서 시험은 실제 코어가 만든 Pending을 시험에서 보류한 뒤 실제 Store.append/finish를 호출한다. 제품 기록/출력 워커도 재시작에서 사용한다. 새 sink는 시험 채널이며 실제 Kafka 브로커의 수신 보장을 증명하지 않는다.

## 시험 집계 오류 정정

기존 로컬 집계가 이름과 같은 줄의 ok만 찾아 시작 시험4개를 누락했다. 실제 Cargo 요약은18개 통과인데 Kafka의 예상된 격리 주소 연결 오류가 이름/ok 사이에 끼어14개만 인식했다. 제품 실패가 아니라 결과 집계의 누락이다.

[scripts/rust_test_results.py](../../scripts/rust_test_results.py)는 시험 이름을 별도로 읽고 Cargo 요약의 통과 수/실패0/제외0과 대조한다. 이름 중복/누락·빈 선택·실패/제외 요약이면 실행기를 실패시킨다. [verify_order_safety.py](../../scripts/verify_order_safety.py)가 이제 이 결과를 선택별/고유/실행 합계로 직접 남기며 수작업 같은 줄 파싱을 사용하지 않는다.

출처: [rust_test_results.py](../../scripts/rust_test_results.py#L7), 현재 소스의 부분 발췌.

```python
    names = re.findall(r"^test (\S+) \.\.\.", text, re.MULTILINE)
    summaries = re.findall(
        r"^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;", text, re.MULTILINE
    )
    if not summaries:
        raise ValueError("시험 요약이 없습니다")
    passed = sum(int(row[1]) for row in summaries)
    if passed == 0 or any(row[0] != "ok" or int(row[2]) or int(row[3]) for row in summaries):
        raise ValueError("실패·제외·빈 선택은 완료로 집계할 수 없습니다")
    if len(names) != passed or len(set(names)) != len(names):
        raise ValueError("시험 이름과 통과 개수가 일치하지 않습니다")
    return {"tests": passed, "failed": 0, "ignored": 0, "cases": names}
```

외부 로그의 FAIL이라는 단어를 시험 실패로 해석하지 않는다. 실제 시험 요약을 기준으로 하고 이름 개수와 요약이 맞지 않으면 결과를 추측하지 않는다. 이 파서는 현재 실행기의 통제된 선택 로그를 위한 것으로 임의의 모든 Cargo 출력 형식을 지원한다고 주장하지 않는다.

[test_rust_test_results.py](../../scripts/test_rust_test_results.py)의5개는 정상 성공, 외부 오류 로그의 줄 끼어들기, 이름 누락/중복, 실제 실패/제외 요약,0개 선택/요약 누락을 각각 검증한다. 통과 수를 늘리려고 제외를 성공으로 취급하지 않는다.

보존된075/076 원본 로그를 새 집계기로 재검사했다. 과거 JSON 증적 자체는 덮어쓰지 않고 이번 증적에 로그 SHA-256/선택별 이름/요약과 정정 수치를 별도로 남겼다.

| 원본 실행 | 당시 집계 | 보존 로그 재집계 |
| --- | --- | --- |
|075/엔진018 | 고유147/실행160 | 고유151/실행164 |
|076/엔진019 | 고유148/실행161 | 고유152/실행165 |

076의 Java179+Rust152+시장1은 고유332개다. 당시328개라는 합계의 차이도 같은4개이며 새 제품 테스트를 과거 시점에 추가한 것이 아니다. 현재 실행은 새 Rust6개를 포함해 고유158개다. 과거 계획/증적의 카운트는 이 정정과 시점을 함께 읽는다.

## 실행 명령과 결과

```bash
/Users/dustin/.cargo/bin/cargo test --locked --lib deposit -- --test-threads=1
python3 -m unittest discover -s scripts -p test_rust_test_results.py -v
python3 scripts/verify_order_safety.py
```

macOS arm64/기존 Cargo.lock/소유 PG16·임시 자체 저널. 입금 선택14개 통과 후 안전성 전체 선택을 통과했다. 집계기 보강 뒤 전체 실행기를 다시 실행해 **Rust 고유158개/선택 실행177회**, 실패/제외0개를 이름 수와 Cargo 요약으로 확인했다. Python 집계5개도 통과해 언어를 합치면 고유163개/실행182회다. 최초 입금 선택 실행은 최종 회귀 합계에 다시 더하지 않는다.

전체 테스트 소스 컴파일/제품 build 통과, 소유 PG 제거를 확인했다. lib9개/lib test10개/bin60개(중복7 포함) 경고와 SQLx0.6.3 미래 호환 경고가 남는다. 제품 bin은 내부 입금 진입점을 아직 HTTP에서 사용하지 않아 새 공개 오류 타입의 미사용 경고1개가 추가됐다. 경고를 숨기는 allow나 임시 호출을 넣지 않았다. 기존 Kafka 없는 시작 시험의 격리 주소 연결 거절 로그는 브로커 검증으로 해석하지 않는다.

새 실제 시장/Kafka 연결·플랫폼 금융 회귀·메인넷·성능 측정·OSV 감사는 이번에 다시 실행하지 않았다. 코어 공유 락/수명·기록/출력 포화·저널 재생·금액/주문 회귀는 위 안전성 실행기의 실제 선택 범위다.

## 남은 연결과 관련 커밋

플랫폼의 현재 입금/고객/이전거래 위험 근거와 금융 승인을 불변 반입 원본에 결합한 뒤, 서비스 인증 HTTP와 확정 Kafka의 영속 수신/원장 전이를 연결해야 한다. 타입 변경만으로 신규 입금을 고객의 사용 가능 잔액으로 반영하지 않는다.

- [입금 반환 계약 3e890cf](https://github.com/GiHoon1123/Cex-Engine/commit/3e890cf8c9eba604c4bef95fe5bd8c79fed4d10b)
- [집계 회귀 324d8be](https://github.com/GiHoon1123/Cex-Engine/commit/324d8be)
- [실행·정정 증적](../reference/020-typed-deposit-outcomes-verification-2026-10-25.json)
