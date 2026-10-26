# 현재 금융 이력을 잃는 새 저널 초기화를 막기

새 데이터 경로를 최초 기동으로 오인해 플랫폼의 현재 잔액만 가져오는 경로를 보완한다. 현재 엔진 결과 수신·입금 전달 이력이 있으면 초기화를 거부한다. 원래 자체 저널로 재시작할 때는 플랫폼 DB를 읽지 않는다. 공통 사이클129·엔진 결정022다.

## 문제와 기존 동작

[009 자체 저널](009-durable-journal-recovery.md)은 단일 스레드 코어의 상태·명령 원본을 복구 근거로 삼는다. 기존 최초 기동 검증은 과거 engine_order_receipts, 미종결 주문, 잠긴 잔액, 금액 오류 등을 검사했다. 이미 소유한 디렉터리의 저널 유실도 거부한다.

하지만 아예 다른 빈 경로를 지정하면 현재 플랫폼의 engine_event_receipts와 bitcoin_credit_dispatch_attempts를 검사하지 않았다. 종료된 주문과 사용 가능 잔액만 남은 경우 새 저널을 만들 수 있었다. 현재 잔액에는 원본 입금의 중복 방지 식별자와 이벤트 순서가 없어 같은 명령의 재확인을 신규 반영으로 오인할 위험이 있다. 이 문제는 체결 알고리즘이 아니라 최초 시작점 검증의 누락이다.

## 기존 제품과 공식 자료의 접근

2026-10-26 KST 확인한 [Microsoft의 이벤트 소싱 패턴](https://learn.microsoft.com/en-us/azure/architecture/patterns/event-sourcing)은 변경 이력을 권위로 보존하고 조회 투영을 분리하는 접근을 설명한다. 조회 화면에 권한 안내가 있었지만 패턴 본문은 확인했다. 기존 자체 저널과 같은 원칙을 적용하며 새 프레임워크나 별도 이벤트 저장 계층을 도입하지 않는다.

[PostgreSQL16 격리 수준](https://www.postgresql.org/docs/16/transaction-iso.html)의 REPEATABLE READ를 사용해 기존 읽기 전용 최초 검증과 같은 스냅샷에서 검사한다. 이는 이후 플랫폼 쓰기나 두 엔진의 동시 실행을 막는 리더 펜싱이 아니다.

## 선택과 트레이드오프

| 선택 | 이점 | 비용·제약·대안 |
| --- | --- | --- |
| 현재 수신 기록이 하나라도 있으면 새 초기화 거부 | 과거 엔진의 순서·원본을 잃는 시작 차단 | 공개 호가 수신만 있어도 자동 초기화를 거부한다. 금융 이벤트만 문자열로 골라 허용하는 대안은 손상/미지 식별자를 빠뜨리므로 사용하지 않는다 |
| 입금 전송 시도 자체를 확인 | 결과 불명·HTTP 결과 누락을 미실행으로 오인하지 않음 | 이번 미접수/거절 기록도 자동 예외로 허용하지 않는다. 원래 권위 저널과 실제 전달 상태를 확인해야 한다 |
| 같은 읽기 전용 트랜잭션 | 검사 시점과 쓰기 권한 경계 유지 | 스키마 초기화가 먼저 필요하다. 표 누락·권한 오류는 실패이며 빈 표 폴백이 없다 |
| 기존 저널은 그대로 재생 | DB 장애/투영 지연과 독립적으로 기존 금융 상태 복구 | WAL의 가용성과 무결성이 필요하다. 잔액만 가져와 재시작하는 우회는 제공하지 않는다 |

사용자 정상 거래에 인증 단계나 입력을 추가하지 않는다. 운영자가 잘못된 데이터 경로로 시작했을 때 자동 시작 대신 원본 복구를 요구하는 운영 부담을 선택했다. 실제 최초 기동은 플랫폼 스키마를 준비한 뒤 금융 요청을 받기 전에 검증된 잔액으로 수행한다. 이후에는 동일한 저널 경로를 유지한다. 금융 기록을 지워서 검사를 통과시키면 안 된다.

## 구현과 실제 코드·문법

코어의 오더북·잔액 소유권·매칭 루프·기록/출력 워커는 변경하지 않았다. 최초 검증 함수와 그 회귀 시험만 수정했다.

출처: [startup.rs](../../src/domains/cex/engine/runtime/startup.rs#L43), 현재 소스의 부분 발췌.

```rust
    let received: Option<String> =
        sqlx::query_scalar("SELECT event_id FROM engine_event_receipts ORDER BY event_id LIMIT 1")
            .fetch_optional(&mut *transaction)
            .await
            .context("Failed to inspect current engine event history")?;
    ensure!(
        received.is_none(),
        "Engine startup blocked: current engine event history exists; restore the authoritative journal"
    );

```

Option<String>은 행이 없으면 None, 있으면 Some(이벤트 식별자)다. fetch_optional은 조회 실패를 None으로 바꾸지 않는다. await 뒤 ?는 조회 오류를 호출자에게 전파하고, ensure!은 이력이 있으면 시작을 거부한다. TypeScript의 nullable 값과 구분된 예외 처리를 함께 표현하지만 Rust는 Option과 Result를 별개 타입으로 검사한다.

출처: [startup.rs](../../src/domains/cex/engine/runtime/startup.rs#L56), 현재 소스의 부분 발췌.

```rust
    let attempted: Option<String> = sqlx::query_scalar(
        "SELECT id::text FROM bitcoin_credit_dispatch_attempts ORDER BY id LIMIT 1",
    )
    .fetch_optional(&mut *transaction)
    .await
    .context("Failed to inspect native deposit dispatch history")?;
    ensure!(
        attempted.is_none(),
        "Engine startup blocked: native deposit dispatch history exists; restore the authoritative journal"
    );

```

두 번째 검사도 &mut *transaction으로 같은 트랜잭션 연결을 빌린다. 새 풀 연결로 조회하지 않는다. 식별자와 결과를 재계산하지 않고 기록의 존재만 확인하며 최초 시작점의 적격성만 판단한다.

출처: [startup_tests.rs](../../src/domains/cex/engine/runtime/engine/startup_tests.rs#L546), 현재 소스의 부분 발췌.

```rust
    pool.close().await;
    let mut recovered =
        HighPerformanceEngine::new(Database::noop(pool)).with_data_dir(directory.path());
    recovered.start().await?;
    let balance = recovered.handle().get_balance(101, "USDT").await?;
    finish_engine(&mut recovered).await?;
    assert_eq!(balance, (Decimal::from(90), Decimal::from(10)));
    Ok(())
}
```

시험에서는 실제 PostgreSQL 풀을 닫고 같은 저널 디렉터리로 표준 엔진을 다시 시작한다. move되는 풀 핸들로는 DB 조회가 성공할 수 없다. 999로 바꾼 DB 투영이 아니라 저널의 사용 가능90·잠금10을 복원해야 통과한다. 이 시험에서만 엔진 명령으로 잠금을 만든 것이며 제품 코어에 새 공유 락을 추가하지 않았다.

## 작성한 테스트

파일: [startup_tests.rs](../../src/domains/cex/engine/runtime/engine/startup_tests.rs). 아래 신규8개는 SQLx0.6이 격리 PostgreSQL 서버 안에 함수별 DB를 만들어 실행한다. 시험 스키마는 읽기 계약만 구성하며 손상 조건을 만들기 위해 일부 제약을 느슨하게 둔다. 플랫폼 전체 금융 서비스가 이 시험에 포함됐다는 뜻은 아니다.

| 함수 | 시나리오·기대 결과 |
| --- | --- |
| startup_rejects_current_financial_receipt_with_terminal_orders | 종료 주문·정상 잔액·과거 접수 없음이어도 현재 수신 이력으로 시작 거부, 원본 보존 |
| startup_rejects_public_receipt_as_prior_engine_activity | 공개 호가 식별자도 과거 엔진 실행 증거이므로 거부 |
| startup_does_not_treat_unknown_receipt_identity_as_empty_history | 손상·알 수 없는 식별자를 빈 이력으로 분류하지 않음 |
| startup_rejects_deposit_dispatch_without_any_received_result | 수신 결과 없는 전송 시도만으로 거부, 시도 원본 보존 |
| startup_refuses_missing_current_receipt_table | 수신 표 누락 시 오류 전파, 워커/캐시 미공개 |
| startup_refuses_missing_deposit_dispatch_table | 전달 표 누락 시 오류 전파, 워커/캐시 미공개 |
| current_history_probes_use_the_same_readonly_repeatable_snapshot | 잘못된 격리/쓰기 모드에서만 행을 내는 시험 뷰로 두 조회가 실제 같은 읽기 전용 RR 연결인지 확인 |
| existing_journal_recovers_while_new_path_refuses_current_history | 새 경로 거부 후 DB 풀을 닫아도 원래 WAL로90/10 복원 |

거부 도우미는 코어·출력에 더해 기록 워커도 시작하지 않았는지 확인한다. 검사 실패가 유효한 앞선 행을 캐시에 부분 공개하지 않는 기존 회귀도 유지했다.

## 실행 명령과 결과

환경: 로컬 macOS·설치 Rust 도구체인·PostgreSQL16 소유 컨테이너. 기존5432/5433 공유 DB를 사용하지 않았다.

- `caffeinate -i python3 scripts/verify_order_safety.py`: **고유181개/218회**, 실패·제외0. 19개 선택/컴파일/빌드 단계 모두 종료0. 이름이 중복 선택된 실행을 고유 수에서 제거했다. 전체 시험 컴파일과 개발 빌드도 통과했다.
- `/Users/dustin/.cargo/bin/rustfmt --edition 2021 --check src/domains/cex/engine/runtime/startup.rs src/domains/cex/engine/runtime/engine/startup_tests.rs`: 통과.
- `git diff --check`: 통과.

저장소 전체 cargo fmt --check는 기존 benches/tps_benchmark.rs와 balance_cache.rs 등의 형식 차이로 실패했다. 이번 두 파일만 rustfmt를 적용하고 해당 파일 검사를 통과시켰다. 무관한 기존 파일을 재정렬하지 않았다. 기존 unused/dead_code 및 SQLx0.6 향후 호환 경고는 남는다.

원시 로그/집계는 .local/cycle129-proof에 보존했고 소유 시험 컨테이너는 정리했다. 공유 금융 데이터·기존 서비스·봇 설정은 변경하지 않았다. 실제 Kafka/플랫폼 전체 연결·장기 부하·운영 이중 기동 시험을 실행한 결과가 아니다. 과거 순수 계산 성능 수치를 재사용하지 않는다.

## 한계와 운영 조건

이 검사는 플랫폼에 남은 이력을 이용한 보수적인 최초 기동 방어다. 모든 이력이 비어 있다고 언제나 안전한 시작점이라는 증명은 아니다. 검증 중 다른 프로세스가 금융 요청을 받지 않는 초기화 순서, 단일 활성 엔진, 올바른 데이터 볼륨과 권한이 필요하다. DB 이력 자체가 훼손되거나 다른 엔진이 동시에 시작되는 위험을 해결한 것으로 주장하지 않는다.

이미 초기화된 원래 WAL의 검증·재생은 계속 복구 권위다. 플랫폼 DB 폴백을 추가하지 않았다. 디스크/호스트 손실의 복제·외부 백업은 사용자 지시대로 문서에 조건만 남기며 구현·배포하지 않았다. 플랫폼 자동 처리의 실제 Rust HTTP→WAL→Kafka→금융 반영 연결 검증은 후속 사이클이다.

## 관련 커밋과 증적

- [구현 232e983](https://github.com/GiHoon1123/Cex-Engine/commit/232e983d016d9a9a3dc79bb1dbcb24ef86ec51bc)
- [기계 판독 검증 결과](../reference/022-first-bootstrap-current-history-verification-2026-10-26.json)
- [플랫폼 자동 처리128](../../../Cex-Platform-Service/docs/decisions/128-automatic-bitcoin-credit-processing.md)
