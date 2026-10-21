# 실행 전에 주문 접수를 영속화하고 같은 주문을 다시 실행하지 않기

> 후속 변경: [009 WAL 확정과 복구](009-durable-journal-recovery.md)에서 자체 저널 재생을 연결했다. 아래 내용은 해당 결정 당시의 상태다. DB 시작 차단은 이제 새 저널의 최초 기동에 적용하고, 기존 저널이 있으면 자체 기록으로 복원한다.

플랫폼의 원본 주문에 연결된 접수 기록을 PostgreSQL에 먼저 커밋한다. 같은 주문 ID의 재전송은 기존 접수만 반환하며 실행 큐로 다시 보내지 않는다. 이 문서는 접수와 체결의 차이, 장애 경계, 예약 중복 반영 수정과 검증 결과를 설명한다.

- 작성·검증일: 2026-10-21.
- 상태: HTTP 영속 접수·중복 전달 차단 구현, 격리 DB·실제 Java/Rust 프로세스 연결 검증 완료.
- 선행: [엔진 초기 상태 차단](002-block-incomplete-startup-state.md), [플랫폼 원본·고객 키 보존](../../../Cex-Platform-Service/docs/decisions/003-idempotent-order-submission.md).
- 자동 복구·체결 결과의 원자적 확정·전역 실행 순서는 아직 미완료다.

## 기존 동작과 위험

기존 `OrderService.create_order`는 `tokio::spawn`으로 제출 작업을 분리하고 곧바로 `pending` 주문을 만들어 201을 반환했다. 응답 시점에는 실행 큐 전달조차 성공했는지 알 수 없었고, 생성 WAL 전송도 주석 처리돼 있었다. 같은 주문 ID를 다시 보내도 새 실행을 막는 영속 기록이 없었다.

플랫폼은 이미 DB에서 주문 금액을 예약했는데 엔진의 주문 제출 경로도 같은 예약 delta를 DB Writer로 보냈다. 1,000 중 200을 예약하면 플랫폼에서 `800/200`이 된 잔액을 엔진이 다시 `600/400`으로 만들 수 있었다.

이번에는 접수 기록을 먼저 확정하고 접수의 최초 삽입자만 큐 전달을 시도한다. 실행용 캐시는 예약하되 플랫폼의 DB 예약은 반복하지 않는다.

## 기존 제품과 공식 자료

TigerBeetle은 같은 업무에 같은 식별자를 사용하고 서버가 중복 효과를 방지하도록 설계한다. 여기서는 플랫폼이 부여한 주문 ID를 엔진 접수의 키로 사용한다. 고객 UUID와의 연결은 플랫폼 원본 행으로 추적한다. [신뢰할 수 있는 거래 제출](https://docs.tigerbeetle.com/coding/reliable-transaction-submission/)

PostgreSQL의 유일 제약과 `INSERT ... ON CONFLICT DO NOTHING RETURNING`은 동시 삽입에서 최초 행을 구분하는 데 사용한다. 기존 행을 UPDATE하거나 접수 시각을 덮어쓰지 않는다. [PostgreSQL 16 INSERT](https://www.postgresql.org/docs/16/sql-insert.html)

동기 커밋은 응답 전에 로컬 WAL 반영을 기다리는 데 필요하다. `synchronous_commit=on`만으로 복제·백업·호스트 손실 복구가 완성되지는 않는다. DB의 `fsync` 등 정상적인 내구성 설정과 저장장치가 전제다. [PostgreSQL 비동기 커밋](https://www.postgresql.org/docs/16/wal-async-commit.html)

## 선택과 트레이드오프

| 선택 | 이점 | 비용·한계 |
|---|---|---|
| DB 접수 커밋 후 응답·큐 전달 | 성공 응답과 추적 가능한 접수를 연결 | 접수마다 DB 트랜잭션과 동기 WAL 지연 |
| 최초 삽입자만 큐 전달 시도 | 응답 유실·동시 요청에서 같은 주문 재실행 방지 | 커밋 후 전달 전 종료되면 실행되지 않은 채 남을 수 있음 |
| 기존 접수는 그대로 반환 | 바뀐 잔액·주문 상태에 영향받지 않는 중복 확인 | 처리 여부·체결 결과를 알려주는 응답은 아님 |
| 플랫폼 원본과 실제 주문을 조회·잠금 | 임의 주문 ID·다른 고객·다른 명령의 접수 차단 | 공유 DB 스키마 의존성과 짧은 행 락 |
| 접수 존재 시 재시작 차단 | 처리 위치를 추측해 재실행하지 않음 | 종료 주문이어도 현재는 자동 재개 불가 |
| 큐 `try_send` | 큐가 가득 차도 async 작업을 blocking하지 않음 | 큐 전달 실패 시 접수는 남지만 자동 재전달 없음 |

새 브로커·분산 락·범용 워커·추측한 재생 로직은 추가하지 않았다. 이미 설치된 SQLx·PostgreSQL·Serde·thiserror를 사용한다. 접수와 실행 큐 사이에 원자적 트랜잭션이 있다고 주장하지 않는다.

## 영속 접수의 데이터와 처리 순서

정의: [schema/order_receipts.sql](../../schema/order_receipts.sql).

`engine_order_receipts`는 주문 ID, 고객 ID, 원본 유형·방식·자산·가격·수량·금액, 접수 시각을 저장한다. 주문 ID가 PK이며 플랫폼 `order_submissions(order_id)`에 삭제 제한 외래키를 둔다. 접수 시각은 실행 순서 번호가 아니다. 접수 행을 수정·삭제하는 제품 경로는 없다.

```mermaid
sequenceDiagram
    participant P as Java 플랫폼
    participant H as Rust HTTP
    participant D as PostgreSQL
    participant Q as 실행 큐
    P->>H: 플랫폼 주문 ID와 원본 명령
    H->>D: 기존 접수 조회
    alt 기존 접수
        D-->>H: 저장한 원본·접수 시각
        H-->>P: 200 기존 접수 / 내용 충돌 시 409
    else 최초 접수
        H->>D: 플랫폼 원본·주문 확인 및 접수 삽입
        H->>D: COMMIT (동기 WAL)
        H->>Q: 최초 삽입자만 전달 시도
        H-->>P: 202 영속 접수
    end
```

[order_receipt.rs](../../src/domains/cex/services/order_receipt.rs)는 READ COMMITTED와 동기 커밋을 명시한다. 플랫폼 원본과 주문 소유자가 일치하고, 새 접수 시 주문이 `pending`·미체결이어야 한다. 원본은 `FOR SHARE`로 읽어 접수 중 변경과 경합한다. 이미 접수한 주문은 현재 주문 상태가 바뀌어도 기존 접수를 반환한다.

첫 접수 조회 직후 경쟁 요청이 접수하고 주문을 종료할 수도 있다. 원본 조회가 비면 접수를 다시 읽어 이를 확인한다. 새 SELECT에서 승자의 커밋을 읽어야 하므로 세션의 기본 격리 수준에 맡기지 않는다.

[OrderService](../../src/domains/cex/services/order_service.rs)는 커밋 반환 뒤 최초 명령만 전달한다. 별도 spawn과 비원자적인 사전 잔액 조회는 제거했다. 플랫폼은 예약을 먼저 커밋하고, 엔진 실행 스레드는 기존 잔액 잠금 검사로 실행용 캐시를 확인한다. 이 접수 응답은 잔액 검사·매칭 성공을 보장하지 않는다.

큐가 닫혔거나 가득 찼어도 접수를 삭제하지 않는다. DB 커밋 응답을 잃었다면 HTTP가 실패해도 접수가 남을 수 있다. 재전송은 그 접수를 확인하며 실행을 추측해 반복하지 않는다.

## HTTP 계약과 플랫폼 검증

```json
{
  "order_id": "123",
  "user_id": "101",
  "receipt_status": "accepted",
  "accepted_at": "2026-10-21T00:00:00Z"
}
```

| 상태 | 의미 |
|---|---|
| 202 | 새 접수 커밋. 실행·체결 결과 미확정 |
| 200 | 같은 주문 ID·명령의 기존 접수. 큐 재전송 없음 |
| 400 | 잘못된 입력·플랫폼 원본 없음·새 접수의 주문 조건 불충족 |
| 409 | 접수 또는 플랫폼 원본과 고객·명령 내용 충돌 |
| 503 | 영속 접수 저장소 오류. SQL·연결 정보는 응답에 노출하지 않음 |

이전의 엔진 201·가상 `pending` 주문 DTO 응답은 제거했다. ID는 응답에서 문자열로 보존한다. Java는 200/202뿐 아니라 주문 ID·고객 ID·접수 상태·유효한 시각을 검증한다. 단순 2xx, 다른 ID, 오래된 DTO는 성공으로 간주하지 않는다. [플랫폼 검증 기록](../../../Cex-Platform-Service/docs/decisions/004-verify-durable-engine-receipt.md)

고객 인증과 내부 호출자 신원 보장은 별도 통제다. 플랫폼 원본 비교만으로 엔진 호출자를 인증한 것은 아니다.

## 예약 중복 반영 제거

[threads.rs](../../src/domains/cex/engine/runtime/threads.rs)의 최초 주문 예약에서 DB `UpdateBalance` 전송을 삭제했다. 플랫폼은 주문·원본·예약을 한 트랜잭션에 기록한다. 엔진은 시작 때 읽은 잔액의 실행용 캐시만 예약한다.

검증한 최소 흐름은 상대 주문 없는 지정가 매수다. 실제 두 프로세스에서 첫 200 예약 후 DB `800/200`, 다음 100 예약 후 `700/300`을 확인했다. 수수료·시장가 정산·취소·부분 체결·다중 계정의 원자성 전체를 검증한 결과는 아니다.

## 시작 조건과 스키마 준비

플랫폼이 생성한 `orders`와 `order_submissions`가 먼저 필요하다. 신규 테이블 정의를 검증용 또는 준비한 DB에 적용한다. 기존 주문에 접수를 추측해 채우거나 금융 이력을 지우는 단계는 없다.

```bash
cd /Users/dustin/Desktop/common/project/Cex-Engine
psql 'postgresql://root:1234@127.0.0.1:5433/cex' -v ON_ERROR_STOP=1 -f schema/order_receipts.sql
```

이번 작업은 이 명령을 기존 로컬 고객 DB에 실행하지 않았다. 새 테스트 DB에만 적용했다. 기존의 비활성 마이그레이션 경로를 되살리지 않았고, 스키마가 없으면 시작·접수를 거부한다. 반복 적용용 `IF NOT EXISTS`나 자동 수정 폴백도 없다.

현재는 접수 1건이라도 있으면 시작을 차단한다. `orders`가 종료 상태인 것만으로 메모리·체결 확정 위치를 복원할 수 없다. 접수 행 삭제·상태 임의 변경으로 우회하지 않는다.

HTTP 기본 바인딩은 `127.0.0.1:3002`다. 격리 검증·컨테이너 연결은 `ENGINE_HTTP_ADDRESS`로 명시한다. 컨테이너에서 다른 서비스가 접근해야 하면 내부 네트워크용 `0.0.0.0:3002` 등을 지정하고 외부 노출을 제한한다. 이번에 기존 실행 프로세스는 교체하지 않았다.

## 이해에 필요한 Rust·DB 원리

- `Result<AcceptanceResult>`는 성공과 오류를 명시적으로 반환한다. `?`로 DB 오류를 전파하며 성공 접수로 바꾸지 않는다.
- DB의 `created`는 INSERT 승자를 구분한다. 여러 Tokio 작업·여러 서비스 인스턴스가 같은 주문을 받아도 프로세스 메모리 대신 PK가 중복을 판단한다. 서로 다른 주문을 여러 엔진이 처리해도 안전하다는 리더 선출 보장은 아니다.
- `transaction.commit().await`가 성공한 뒤 큐에 접근한다. 큐의 성공은 메모리에 들어갔다는 뜻이며 DB 커밋·처리 완료와 다르다.
- `Option<Decimal>`의 null은 시장가 매수에서 수량을 지정하지 않았다는 의미다. 이를 0으로 바꿔 원본을 비교하지 않는다. Decimal은 소수 scale이 달라도 수치가 같으면 동일하게 비교한다.
- `tokio::spawn` 제거로 요청과 제출 시도의 제어 흐름을 연결했다. TypeScript에서 작업을 기다리지 않는 Promise를 시작하고 성공을 먼저 반환하던 경로를 `await`로 연결한 것과 비슷하지만, 원격·메모리 작업까지 원자화되는 것은 아니다.

## 작성한 테스트

파일: [order_receipt/tests.rs](../../src/domains/cex/services/order_receipt/tests.rs). SQLx가 테스트마다 새 PostgreSQL DB를 만든다. 벤치 엔진은 워커 없이 큐 전달 수를 관찰하며 실제 매칭 결과를 주장하지 않는다.

| 함수 | 시나리오·기대 결과 |
|---|---|
| receipt_is_durable_before_dispatch_and_duplicate_keeps_timestamp | 접수 1건·큐 1건·기존 시각 유지 |
| concurrent_receipts_dispatch_exactly_one_command | 동시 12개 → 삽입·큐 전달 1회 |
| changed_command_or_owner_cannot_reuse_receipt | 고객과 원본 7개 필드 변경 충돌 |
| unknown_or_terminal_platform_order_is_not_accepted | 원본 없는 주문·새 접수의 종료 주문 거부 |
| platform_original_mismatch_is_rejected_before_receipt | 다른 원본은 저장 전 거부 |
| current_order_changes_do_not_change_existing_receipt | 종료 후에도 기존 접수 유지, 재전송 없음 |
| new_service_and_executor_do_not_dispatch_existing_receipt | 새 서비스·캐시에서도 DB 접수 확인 |
| commit_failure_does_not_dispatch_or_report_acceptance | 지연 FK로 COMMIT 실패 주입 → 기록·큐 없음 |
| closed_dispatch_channel_keeps_receipt_and_does_not_panic | 큐 종료에도 접수 보존·panic 없음 |
| market_buy_preserves_exact_budget_and_null_amount | 123.250000001·수량 null 보존 |
| invalid_identifiers_and_fields_never_dispatch | 누락·0·BIGINT 초과 ID와 잘못된 수량 거부 |
| receipt_foreign_key_protects_platform_original | 접수에 연결된 플랫폼 원본 삭제 거부 |
| http_contract_distinguishes_new_and_existing_receipt | 실제 라우터 202/200·정확한 ID·접수 응답 |
| missing_receipt_storage_returns_unavailable_without_sql_details | 저장소 없음 → 503·내부 SQL 비노출 |
| acceptance_forces_synchronous_commit_even_if_session_default_is_off | 세션 비동기 커밋·REPEATABLE READ를 접수의 동기 커밋·READ COMMITTED로 변경 |
| concurrent_completion_keeps_duplicate_acceptance_valid | 동시 접수와 주문 종료 경쟁에서도 승자 1개 |

추가·회귀:

- [startup_tests.rs](../../src/domains/cex/engine/runtime/engine/startup_tests.rs)의 신규 `startup_rejects_receipt_even_with_terminal_order`: 종료 주문도 접수 복원 근거가 없으면 시작 차단. 기존 14건도 새 스키마에서 실행했다.
- [reservation_tests.rs](../../src/domains/cex/engine/runtime/engine/reservation_tests.rs)의 신규 `platform_reservation_is_not_written_to_database_twice`: 실제 주문 처리에서 캐시 `800/200`·DB 예약 delta 전송 없음.
- 기존 실제 라우터 회귀 5건도 실행했다.

## 실행과 결과

SQLx 0.6.3의 `migrations=false` 테스트를 사용했다. 공통 로컬 전용 PostgreSQL 컨테이너에 새 관리자 DB `cex_engine_receipt_c55d2290c23e`를 만들고 그 안의 테스트 메타데이터로 함수별 DB를 생성했다. 테스트 후 관리자 DB까지 제거한다. 기존 5432·고객 DB 전체 삭제 헬퍼는 사용하지 않았다.

```bash
# DATABASE_URL은 새로 생성한 격리 테스트 관리자 DB를 가리킨다.
export PATH=/Users/dustin/.cargo/bin:$PATH
export KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:1
cargo test --locked --lib order_receipt::tests -- --test-threads=1
cargo test --locked --lib startup_tests -- --test-threads=1
cargo test --locked --lib reservation_tests
cargo test --locked --lib routes::tests
cargo test --locked --no-run
cargo build --locked
```

- Rust 선택 테스트 **37건 통과, 실패 0, 제외 0**: 신규 18건·기존 회귀 19건. 필터 밖 테스트는 실행하지 않았다.
- 전체 테스트 컴파일·바이너리 빌드 성공. 기존 라이브러리·바이너리 경고와 Solana/SQLx future incompatibility 안내가 남는다.
- 중간 `order_serialization` 필터는 0건을 선택했으며 통과 건수에 포함하지 않았다.
- Java **83건 통과**, 플랫폼 HTTP 대역 프로세스 회귀 **8건 통과**. 세부 결과는 플랫폼 기록을 따른다.

실제 연결 검증: [verify_durable_acceptance.py](../../scripts/verify_durable_acceptance.py).

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home \
PATH=/usr/local/bin:$PATH python3 scripts/verify_durable_acceptance.py
```

새 PostgreSQL 16 컨테이너·새 Java JAR·새 Rust 바이너리를 임의 루프백 포트로 실행했다. 봇 0, 실제 Kafka·시세 서버 대신 사용하지 않는 루프백 주소, 임시 JWT 키·WAL 디렉터리를 사용했다.

| 연결 시나리오 | 결과 |
|---|---|
| 실제 Rust 응답을 받은 프록시가 Java에 응답을 전달하지 않음 | 고객 202, 엔진 재전송 5회는 모두 200, 플랫폼 재요청 200, 다른 원본 409. 실제 매칭 명령 1회·접수 1건·DB 800/200 |
| 다음 새 주문의 실제 접수 응답을 Java에 전달 | 고객 201, 접수 2건, DB 700/300. Java가 Rust의 실제 응답 형식을 검증 |
| Rust 종료 후 동일 DB에서 재시작 | exit 101, durable receipt 오류, HTTP 시작 없음 |

검증 후 임시 컨테이너·프로세스·WAL을 정리했다. 실행 증거는 공통 루트 `.local/engine-receipt-final.log`, `.local/engine-reservation-test.log`, `.local/durable-acceptance-process.json`이다. 이 연결 시나리오는 상대 주문 없는 지정가 매수이며 실제 체결·정산 전체나 디스크·호스트 손실 복구 시험은 아니다.

## 남은 경계와 커밋

접수와 실행 큐 사이 종료, 처리 중 부분 변경, 체결·이벤트의 원자적 확정, 실행 순서·결정적 재생, 단일 엔진 소유권은 후속 범위다. 자동 재실행을 추가하기 전에 이 경계를 해결해야 한다. 접수 시각을 실행 순서로 사용하거나 종료 상태만 보고 완료를 추측하지 않는다.

- `06656c8`: 영속 접수·원본 연결·HTTP 계약·시작 차단.
- `27b7619`: 플랫폼 예약의 DB 중복 반영 제거와 회귀.
- `ad7f44f`: 실제 두 프로세스 연결·응답 유실 검증.
- `b325672`: 세션 기본값과 무관한 접수 트랜잭션 격리·동기 커밋 보강.
- 플랫폼 `4810e55`: 접수 증명의 식별자·상태·시각 검증.

문서는 별도 목적의 커밋으로 기록한다. 검증·문서 완료 후 작업 단위로 푸시한다.

후속 검토: [금액 타입 경계 오류 수정](005-exact-amount-boundaries-review.md). 이 글의 당시 검증 결과와 후속 변경을 구분한다.
