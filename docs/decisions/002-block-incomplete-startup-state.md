# 복구 근거가 없는 주문·예약금이 남으면 엔진 시작 차단하기

> 후속 변경: [009 WAL 확정과 복구](009-durable-journal-recovery.md)에서 자체 저널 재생을 연결했다. 아래 내용은 해당 결정 당시의 상태다. DB 시작 차단은 이제 새 저널의 최초 기동에 적용하고, 기존 저널이 있으면 자체 기록으로 복원한다.

후속 [영속 접수 기록](003-durable-order-acceptance.md)에서 신규 스키마와 접수 존재 시 시작 차단을 추가했다. 현재 준비·시작 조건은 후속 기록도 함께 따른다. 아래는 이 사이클 당시의 검증 기록이다.

엔진 시작 시 예약금을 사용 가능 잔액으로 합치던 동작을 제거했다. 같은 DB 스냅샷의 주문·잔액을 검증하고, 현재 재생할 수 없는 미종결 주문·예약금이나 잘못된 데이터가 있으면 거래를 재개하지 않는다.

- 작업일: 2026-10-21.
- 분류: 고도화 — 재시작 안전성, 자산 정합성.
- 구현·테스트 커밋: `c7d89c1` (`fix(engine): 불완전한 복구 상태의 시작 차단`).
- 검증: 신규 PostgreSQL 테스트 14개·기존 HTTP 5개·직렬화 1개 통과, 전체 테스트 컴파일·실행 파일 빌드 성공, 격리한 실제 프로세스 시작 거부 2건 확인.
- 완료 범위: DB에서 확인 가능한 불완전 상태의 시작 차단. 활성 주문 자동 복원이나 완전한 장애 복구의 완료를 뜻하지 않는다.

## 문제와 기존 동작

기존 시작 코드는 각 고객 잔액을 다음처럼 메모리에 넣었다.

```text
available = DB.available + DB.locked
locked = 0
```

그러나 활성 주문을 주문장으로 복원하지는 않았다. 예를 들어 DB available 70·locked 30을 가진 고객은 재시작 뒤 available 100·locked 0으로 취급될 수 있었다. 남아 있는 주문·예약금과 무관하게 자산을 다시 사용할 수 있게 되는 문제다.

복원 근거도 부족했다.

- [주문 처리](../../src/domains/cex/engine/runtime/threads.rs)의 WAL 전송 일부가 주석 처리돼 있다.
- [WalReader](https://github.com/GiHoon1123/Cex-Engine/blob/0da5d472d49399ccaadddaf85ad55fe8e46988f3/src/domains/cex/engine/wal.rs)는 존재하지만 시작 경로에서 상태 재생에 사용되지 않는다.
- DB의 pending 주문만으로 엔진 접수 확정 여부·접수 순서·체결 확정 위치를 구분할 수 없다.
- `tests/recovery.rs`의 기존 5개 테스트는 실제 복원 결과나 손상 복구를 검사하지 않는 뼈대였다. 일부는 전후 잔액을 읽고도 비교하지 않았다.

따라서 DB 주문을 임의 순서로 넣거나 locked를 임의 해제하면서 복구됐다고 표시할 수 없었다.

## 검증된 패턴과 선택 근거

PostgreSQL WAL은 데이터 변경을 설명하는 로그를 영속화한 뒤 그 기록으로 재실행하는 원리를 설명한다. 이 원리를 기준으로 보면 현재 엔진은 재생할 확정 기록을 먼저 마련해야 한다. PostgreSQL 자체의 WAL이 엔진의 메모리 주문장과 미전달 Kafka 이벤트까지 복구해 주지는 않는다. [PostgreSQL WAL 설명](https://www.postgresql.org/docs/current/wal-intro.html)

주문과 잔액은 한 `REPEATABLE READ` 트랜잭션에서 읽는다. PostgreSQL 기본 `READ COMMITTED`에서는 연속 SELECT가 다른 커밋 시점을 볼 수 있기 때문이다. 읽기 전용 트랜잭션으로 검사 과정의 금융 쓰기도 차단한다. [PostgreSQL 트랜잭션 격리](https://www.postgresql.org/docs/current/transaction-iso.html)

테스트 DB 격리는 새 도구 없이 설치된 SQLx 0.6.3의 `#[sqlx::test]`를 사용했다. 함수별 DB 생성·성공 시 정리와 `migrations = false` 기능을 로컬 의존성의 문서·소스에서 확인했다. [SQLx 0.6.3 테스트 문서](https://github.com/launchbadge/sqlx/blob/v0.6.3/src/macros/test.md)

## 선택과 트레이드오프

| 대안 | 이점 | 비용·문제 | 선택 |
|---|---|---|---|
| 예약금을 모두 가용 금액으로 해제 | 서버를 바로 시작할 수 있음 | 미종결 업무의 자산을 다시 사용 | 제거 |
| DB pending·partial을 바로 주문장에 추가 | 활성 주문이 보이는 상태를 만들 수 있음 | 접수 확정·순서·미반영 체결을 구분하지 못함 | 채택하지 않음 |
| locked를 유지하고 빈 주문장으로 시작 | 예약 자산의 재사용 일부 방지 | 주문은 사라진 상태로 거래를 계속하고 고객 자산이 묶임 | 채택하지 않음 |
| 검증 가능한 초기 상태만 시작 | 잘못된 재개와 암묵적 언락 차단 | 미종결 주문·예약금이 있으면 엔진 이용 불가 | 채택 |

시작 검증은 장기적으로 유지할 안전 계약이다. 앞으로 권위 있는 재생 기록을 구현하면 복원 결과와 주문·예약·원장을 검증한 뒤 시작하는 단계로 확장한다. 임시 우회 설정·자동 취소·잔액 초기화·폴백은 추가하지 않았다.

## 구현과 상태 변화

[startup.rs](../../src/domains/cex/engine/runtime/startup.rs)는 주문·잔액의 읽기와 검증을 담당하고, [engine.rs](../../src/domains/cex/engine/runtime/engine.rs)는 검증 결과 반영과 스레드 수명주기를 담당한다.

```text
새 엔진
→ 중복 시작·정지 인스턴스 재사용 검사
→ 읽기 전용 REPEATABLE READ 트랜잭션
→ 미종결 주문 검사
→ 전체 잔액을 별도 캐시에 구성·검증
→ 읽기 트랜잭션 종료
→ 검증된 캐시 반영·빈 주문장 구성
→ WAL·DB·매칭 작업 스레드 시작
```

검증 실패는 캐시 반영과 작업 스레드 시작 전에 오류로 반환한다. 현재 실행 진입점은 엔진 시작 성공 뒤 HTTP 리스너를 여므로 실패한 초기 상태에서 주문 API를 열지 않는다.

**시작 거부 조건**

- filled·cancelled·rejected 이외의 주문 상태. pending·partial·알 수 없는 상태·NULL 포함.
- 양수 locked. 현재는 연결할 주문 재생 경로가 없으므로 시작을 거부한다.
- 음수 available 또는 locked.
- 잘못된 고객 ID·자산 식별자 또는 중복 고객·자산 잔액.
- Decimal로 표현할 수 없는 NUMERIC, 조회·트랜잭션 오류.
- 이미 실행 중이거나 stop으로 명령 채널이 닫힌 엔진 인스턴스의 start 호출.

종료 주문 이력과 locked 0인 유효 잔액이 있는 DB, 빈 DB는 시작할 수 있다. 정상 금액은 가산·합산·반올림 없이 읽고 종료 주문 이력은 보존한다.

기존 시작 경로만 사용하던 `UserBalanceRepository.get_all_balances()`는 참조 확인 후 제거했다. 형식적인 기존 복구 테스트 5개도 제거하고 실제 초기 상태 검증으로 대체했다. 의존성·스키마·운영 데이터를 변경하지 않았다.

## 이해에 필요한 Rust와 DB 원리

- `Result<BalanceCache>`는 성공하면 완성된 캐시, 실패하면 오류를 반환한다. `?`는 오류를 상위 호출자에게 전달하며 0이나 빈 상태로 바꾸지 않는다.
- `ensure!`는 조건이 맞지 않으면 즉시 오류를 반환한다. `bail!`은 복구 불가 사유를 담은 오류를 만든다.
- `try_get()`은 DB 열의 타입·값 해독 실패를 오류로 전달한다. `NUMERIC NaN`이나 Decimal 범위 초과를 panic·0 대체로 처리하지 않는다.
- `&mut *transaction`은 SQLx 트랜잭션의 내부 연결을 가변 참조로 빌려 동일한 연결에서 쿼리를 실행한다.
- `let mut balances`는 검증 중인 별도 캐시다. 모든 검증이 끝난 후 `*executor.balance_cache_mut() = balances`로 소유권을 옮겨 실제 캐시를 교체한다.
- `&mut self`는 같은 객체의 동시 가변 접근을 막지만 순차적인 중복 start까지 막지는 않는다. 실행 상태·채널 상태를 별도로 검사한다.
- `#[cfg(test)]`는 테스트 모듈을 테스트 빌드에만 포함한다. 표준 엔진의 실제 start·작업 스레드를 시험하며 Bench 모드로 DB 검증을 우회하지 않는다.

## 작성한 테스트

[startup_tests.rs](../../src/domains/cex/engine/runtime/engine/startup_tests.rs)에 신규 테스트 14개를 작성했다. 각 함수는 SQLx가 만든 전용 PostgreSQL DB를 사용한다.

| 함수 | 시나리오와 기대 결과 | 결과 |
|---|---|---|
| `startup_rejects_locked_balance_without_replay` | 주문이 없어도 locked가 남으면 거부 | 통과 |
| `startup_rejects_pending_order_even_without_locked_balance` | locked가 없어도 pending 주문이 있으면 거부 | 통과 |
| `startup_rejects_negative_available_balance` | 음수 available 거부 | 통과 |
| `startup_rejects_negative_locked_balance` | 음수 locked 거부 | 통과 |
| `startup_rejects_partial_unknown_and_null_order_statuses` | partial·미정의·NULL 상태 모두 거부 | 통과 |
| `failed_validation_does_not_publish_earlier_valid_rows` | 뒤 행 검증 실패 시 정상 첫 행도 실제 캐시에 반영하지 않고 DB 유지 | 통과 |
| `startup_rejects_duplicate_balance_rows` | 같은 고객·자산의 중복 행을 덮어쓰지 않고 거부 | 통과 |
| `startup_rejects_unrepresentable_decimal_without_panicking` | NUMERIC NaN·Decimal 범위 초과를 오류로 전파 | 통과 |
| `startup_rejects_invalid_user_and_asset_identifiers` | 음수·0 고객 ID, 빈 값·앞 공백 자산 거부 | 통과 |
| `startup_propagates_database_inspection_failure` | 주문 조회 실패 시 시작 거부 | 통과 |
| `startup_preserves_exact_balances_with_terminal_order_history` | 소수점 9자리 금액·0 잔액 로드, 종료 이력 보존·빈 주문장 확인 | 통과 |
| `startup_reads_one_repeatable_read_readonly_snapshot` | 실제 조회 연결의 격리 수준·읽기 전용 여부를 DB view로 검사 | 통과 |
| `duplicate_start_does_not_reload_or_replace_running_state` | 중복 start가 실행 중 잔액을 DB 값으로 덮거나 스레드를 중복 생성하지 않음 | 통과 |
| `empty_database_starts_and_stopped_instance_cannot_restart` | 빈 DB 시작·조회·종료, 닫힌 인스턴스 재시작 거부 | 통과 |

시작 거부 테스트는 실패 전에 매칭·WAL·DB 작업 스레드가 만들어지지 않았는지, 검증 중 잔액이 실제 캐시에 반영되지 않았는지도 확인한다. 구버전에서 잘못 시작될 때도 assert 전에 스레드·임시 파일을 정리하도록 작성했다.

## 실행 명령과 결과

설치된 Cargo/Rust 경로를 사용하고 테스트 전용 관리 DB의 URL을 `CEX_STARTUP_TEST_URL`로 준비한다. SQLx는 이 관리 DB에 테스트 메타데이터를 쓰고 별도 테스트 DB를 생성하므로 실제 cex DB나 다른 프로젝트 DB를 지정하지 않는다.

```sh
export PATH="/Users/dustin/.cargo/bin:$PATH"
export DATABASE_URL="${CEX_STARTUP_TEST_URL:?테스트 전용 관리 DB URL 필요}"
export KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:1"

cargo test --locked --lib startup_tests -- --test-threads=1
cargo test --locked --lib routes::tests
cargo test --locked --lib test_binary_serialization
cargo test --locked --no-run
cargo build --locked
```

- 변경 전: locked·pending·음수 available의 신규 재현 테스트 3개 모두 시작을 허용해 실패.
- 변경 후: 신규 14개·기존 HTTP 5개·직렬화 1개 통과. 이 20개 중 실패·제외 0.
- 전체 테스트 컴파일과 실행 파일 빌드 성공. 기존 외부 DB·UDP·Kafka 테스트 전체 실행 결과는 아니다.
- 테스트 함수별 DB와 관리 DB, 임시 WAL 파일 정리 확인.
- 기존 미사용 코드·cfg 경고와 Solana·SQLx의 미래 호환 경고가 남아 있다. Kafka는 의도적으로 사용하지 않는 루프백 주소에 연결하므로 연결 거부 로그가 발생한다. 실제 Kafka 전달을 검증한 결과가 아니다.

설치된 SQLx의 자동 DB 격리를 사용하며 현재 마이그레이션은 실행하지 않았다. 기존 통합 테스트의 공유 DB `DELETE` 헬퍼를 사용하지 않았다.

## 실행 파일에서 확인한 시작 거부

공통 루트의 로컬 보조 스크립트로 별도 PostgreSQL 16 컨테이너를 만들고 다음을 확인했다.

```sh
python3 .local/engine_startup_smoke.py
```

| 입력 | 실행 결과 | 데이터·리스너 |
|---|---|---|
| available 70·locked 30, 주문 없음 | 예약금 사유로 종료 코드 101 | DB 값 유지, HTTP 시작 안 함, WAL 파일 생성 안 함 |
| available 100·locked 0, pending 주문 | 미종결 주문 사유로 종료 코드 101 | 잔액·주문 상태 유지, HTTP 시작 안 함, WAL 파일 생성 안 함 |

현재 main은 start 오류를 `expect`로 처리하므로 비정상 종료 코드 101이 나온다. 이를 성공 실행으로 집계하지 않았다. 임시 컨테이너는 제거했고 기존 3002 엔진·8080 플랫폼·Kafka와 봇 비활성화 설정을 유지했다.

로컬 산출물은 [변경 전 로그](../../../.local/engine-startup-before.log), [신규 테스트](../../../.local/engine-startup-after.log), [HTTP 회귀](../../../.local/engine-startup-routes.log), [컴파일](../../../.local/engine-startup-test-compile.log), [빌드](../../../.local/engine-startup-build.log), [프로세스 결과](../../../.local/engine-startup-smoke.json)에 있다. 보조 스크립트·로그는 작업 공간 산출물이며 저장소의 재현 가능한 테스트 코드는 위 Rust 파일에 있다.

## 운영 영향과 남은 한계

- 미종결 주문·예약금이 있는 DB에서는 새 엔진 시작을 거부한다. 시작을 통과시키기 위해 주문 이력을 삭제하거나 예약금을 0으로 만드는 동작은 추가하지 않았다.
- 이 검사는 스냅샷에 보이는 상태만 검증한다. DB에 아직 기록되지 않은 접수·체결 유실이나 이전 비정상 종료 여부까지 입증하지 않는다.
- `REPEATABLE READ`는 플랫폼의 이후 쓰기를 차단하거나 단일 엔진 리더를 보장하지 않는다. 시작 중 신규 주문·자산 변경을 제한하는 플랫폼 연동과 리더 펜싱이 별도로 필요하다.
- 활성 주문 접수 순서, 체결·잔액 변경의 확정 단위, 영속 기록과 이벤트 재전달을 구현해야 자동 복구를 완성할 수 있다.
- 중복 start 차단은 같은 객체에 대한 보호다. 여러 프로세스가 같은 DB를 사용하는 문제를 해결한 것은 아니다.
- 현재 Standard 생성자는 Kafka 클라이언트를 미리 만들 수 있다. 시작 거부 시 생성하지 않는다고 검증한 대상은 매칭·WAL·DB 작업 스레드다.
- 가용 잔액 로드가 성공해도 원장·보관 자산·전체 체결 정합성 검증을 통과했다는 뜻은 아니다.
- 기존 실행 중인 엔진은 교체하지 않았다. 실제 운영 배포·완전한 복구·고가용성 검증은 포함하지 않는다.

## 관련 작업과 다음 순서

선행 작업은 [플랫폼 시작 시 잔액 재입금 제거](../../../Cex-Platform-Service/docs/decisions/001-remove-startup-balance-replay.md)다. 다음에는 엔진·플랫폼의 접수·예약·체결 확정 책임과 영속 기록 단위를 정하고, 그 근거를 재생하는 복구 경로를 구현한다. 전체 순서는 [거래소 로드맵](../../../docs/roadmap/README.md)을 따른다.
