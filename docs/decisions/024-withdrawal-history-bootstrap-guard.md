# 출금 이력이 있으면 플랫폼 잔액으로 새 엔진을 시작하지 않는다

고객 출금 요청이 플랫폼에 저장된 뒤 자체 WAL을 잃은 엔진이 새 초기 상태를 만들지 못하게 했다. 이 문서는 최초 잔액 반입과 자체 저널 복원의 차이, 추가한 기동 검사와 실제 검증을 설명한다. 공통 사이클141의 엔진 변경이다.

## 문제와 기존 동작

출금 예약은 플랫폼의 불변 원본과 엔진의 같은 UUID를 연결한다. 플랫폼 잔액에 잠금이 없더라도 과거 출금이 거절되거나 반환됐을 수 있다. 잔액만 다시 읽으면 과거 UUID의 거절·종료 상태와 이벤트 위치를 복원할 수 없다. 같은 명령이 새 출금으로 처리될 위험이 있으므로 잔액이 정상이라는 이유로 새 계보를 만들면 안 된다.

## 기존 접근과 선택

기존 [자체 WAL 복원 계약](009-durable-journal-recovery.md)과 [출금 생명주기](023-withdrawal-reservations.md)를 확장한다. 자체 저널이 없을 때만 수행하는 읽기 전용 Repeatable Read 초기 검사에서 플랫폼의 `bitcoin_withdrawals`와 `bitcoin_withdrawal_dispatch_attempts`를 함께 읽는다. 하나라도 이력이 있거나 표를 읽을 수 없으면 기동을 거부한다. 정상 재시작은 자체 저널을 검증·재생한다.

장점은 잔액 스냅샷만으로 소실된 출금 생명주기를 추측하지 않는다는 점이다. 비용은 최초 기동 전에 플랫폼 스키마가 준비돼 있어야 하고, 저널을 잃으면 단순 DB 반입으로 재개할 수 없다는 점이다. 플랫폼 DB를 복구의 대체 권위로 삼는 대안은 UUID·확정 위치를 보존하지 못해 선택하지 않았다. 디스크·호스트 손실 보호에는 별도 백업·복제가 필요하며 이번 변경은 해당 인프라를 구현하지 않는다.

## 구현과 이해에 필요한 코드

출처: [startup.rs](../../src/domains/cex/engine/runtime/startup.rs#L70), 현재 소스의 부분 발췌.

```rust
    let withdrawal: Option<String> =
        sqlx::query_scalar("SELECT id::text FROM bitcoin_withdrawals ORDER BY id LIMIT 1")
            .fetch_optional(&mut *transaction)
            .await
            .context("Failed to inspect native withdrawal history")?;
    ensure!(withdrawal.is_none(),
        "Engine startup blocked: native withdrawal history exists; restore the authoritative journal");
```

`Option<String>`은 이력이 없으면 `None`, 있으면 `Some(id)`다. `fetch_optional`의 조회 실패는 `?`로 상위 기동 실패에 전달한다. 조회 오류를 `None`으로 바꾸지 않는다. `&mut *transaction`은 동일한 초기 검사 트랜잭션을 잠시 빌린다. TypeScript의 선택 값과 비교할 수 있지만 Rust의 빌림은 컴파일 시점의 접근 제약도 갖는다. 이 코드는 매칭 루프가 아닌 초기화 경로에 있고, 거래 코어에 공유 락·네트워크 대기·새 스레드를 추가하지 않는다.

## 작성한 테스트

[startup_tests.rs](../../src/domains/cex/engine/runtime/engine/startup_tests.rs)의 신규 SQLx 시험이다. 실패한 SQLx 시험이 하위 DB를 남길 수 있어 시험 실행 전체를 전용 PostgreSQL 컨테이너로 격리했다.

| 함수 | 시나리오와 기대 결과 |
| --- | --- |
| `startup_blocks_current_withdrawal_history_without_journal` | 출금 원본만 있어도 새 초기 반입 거부 |
| `startup_blocks_unconfirmed_withdrawal_dispatch_without_journal` | 금융 결과가 아직 없어도 출금 전달 시도가 있으면 거부 |
| `startup_refuses_missing_withdrawal_history_tables` | 출금 원본 표가 없으면 이력 없음으로 추측하지 않고 실패 |
| `startup_refuses_missing_withdrawal_dispatch_table` | 전달 시도 표가 없어도 실패 |

[실제 실행기](../../scripts/verify_withdrawal_reservations.py)의 최소 플랫폼 스키마도 두 표를 포함하도록 갱신했다. 실제 Rust 프로세스·TCP·WAL·Kafka의 예약/해제/완료/거절과 강제 종료 뒤 복원을 다시 확인한다.

## 실행 명령과 결과

- `cargo test --locked --lib startup_tests -- --test-threads=1`: 30통과, 실패0·제외0. 다른 라이브러리 시험180개는 이 선택 실행의 대상이 아니다.
- `cargo build --locked`: 통과.
- `python3 scripts/verify_withdrawal_reservations.py`: 통과. 확정 결과6개와 Kafka 원시7개 메시지를 비교했고, 중복을 제외한 내용이 일치했다. 엔진이 플랫폼 잔액을 쓰지 않음도 확인했다.
- 두 수정 Rust 파일의 `rustfmt --edition 2021 --check`: 통과. 저장소 전체 형식 검사는 기존 다른 파일의 차이를 출력했으며 전체 형식이 통과했다고 주장하지 않는다.

증적은 [실행 결과](../reference/024-withdrawal-bootstrap-verification.json)와 공통 로컬 `.local/cycle141-proof/rust.json`, `rust-startup.log`, `rust-build.log`, `.local/cycle141-engine-wire.log`다. 각 시험이 생성한 PostgreSQL·Kafka 컨테이너는 제거했다. 기존 로컬 서비스와 고객 데이터는 사용하지 않았다.

## 관련 커밋과 구현 연결

- [구현 a70242e](https://github.com/GiHoon1123/Cex-Engine/commit/a70242e)
- 플랫폼의 고객 서명·불변 출금 원본·원장 예약·확정 수신은 [플랫폼141 기록](../../../Cex-Platform-Service/docs/decisions/141-bitcoin-signed-withdrawal-reservations.md)으로 연결한다.
- 실제 지급 승인·체인 서명·전송·완료는 이 변경의 검증 범위가 아니다.
