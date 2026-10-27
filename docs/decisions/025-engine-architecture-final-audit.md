# 단일 소유 체결 코어의 최종 구조 감사와 폐기 경로 제거

현재 실행 경로를 다시 추적해 체결 상태가 한 워커로 이동하고 입력·저널·출력만 채널로 연결되는지 검증했다. 실행하지 않는 DB 직접 잔고 서비스와 성공만 반환하던 모의 엔진을 삭제하고, 잘못된 Cargo 기능 이름을 수정했다.

## 문제와 기존 동작

실제 HTTP 주문은 `OrderService → EngineHandle → 단일 엔진 워커`로 전달된다. 하지만 소스에는 라우트가 사용하지 않는 `BalanceService`, DB 잔고 저장소·응답 모델, 모든 잔고를 무제한으로 돌려주는 `MockEngine`이 남아 있었다. 이 코드는 현재 제품 경로가 아니면서도 엔진이 DB 잔고를 직접 관리하거나 모의 구현으로 교체될 수 있다는 잘못된 설명과 선택지를 만들었다.

또한 `Database::noop` 조건은 Cargo에 선언한 `bench_mode`가 아니라 존재하지 않는 `bench` 기능을 검사했다. 시험에서는 `cfg(test)` 덕분에 숨겨졌지만 벤치 기능 빌드에서는 의도한 생성자를 열지 않는 실제 오타였다.

## 기존 제품과 공식 자료의 접근

Rust 공식 문서는 채널로 값을 보낼 때 소유권이 수신 측으로 이동해 송신 측이 같은 값을 계속 쓰지 못하는 점을 설명한다. Crossbeam의 `bounded` 채널은 정해진 용량을 갖고 포화 시 송신을 실패하거나 대기하게 할 수 있다. LMAX Disruptor도 상태 갱신에서 단일 작성자 원칙을 핵심 성능 패턴으로 제시한다.

- [Rust 메시지 전달과 소유권](https://doc.rust-lang.org/book/ch16-02-message-passing.html)
- [Rust 공유 상태 동시성](https://doc.rust-lang.org/stable/book/ch16-03-shared-state.html)
- [Crossbeam bounded 채널](https://docs.rs/crossbeam/latest/crossbeam/channel/index.html)
- [LMAX Disruptor 단일 작성자 원칙](https://lmax-exchange.github.io/disruptor/user-guide/)

이 프로젝트는 라이브러리 선택만 따라 하지 않는다. 주문장·실행 잔액·출금 예약 전체를 한 워커가 소유하고, 복제 가능한 핸들은 명령만 보낸다는 현재 계약을 정적 검사와 실패 회귀로 고정한다.

## 선택과 트레이드오프

| 선택 | 이점 | 비용·제약 |
| --- | --- | --- |
| 사용하지 않는 DB 잔고 서비스·저장소 삭제 | 상태 권위가 단일 코어라는 설명과 코드가 일치 | 엔진에서 DB 잔고 CRUD를 다시 쓰려면 새 요구와 설계 필요 |
| 성공만 반환하는 모의 엔진 삭제 | 잘못된 금융 성공을 만드는 대체 구현 제거 | 시험은 실제 코어나 명시적 경계 대역을 사용해야 함 |
| 상태 이동·스레드 수·공유 락을 정적 검사 | 구조 회귀를 빠르게 탐지 | 텍스트 검사는 동작 시험을 대체하지 못함 |
| 격리 PostgreSQL 회귀를 별도 실행 | SQLx가 남기는 하위 DB와 다른 서비스 자료를 격리 | Docker가 필요하고 순수 시험보다 오래 걸림 |

## 구현과 관련 코드

출처: [state.rs](../../src/domains/cex/services/state.rs)의 전체 현재 구조.

```rust
#[derive(Clone)]
pub struct CexState {
    pub order_service: OrderService,
}

impl CexState {
    pub fn new(db: Database, engine: EngineHandle) -> Self {
        Self {
            order_service: OrderService::new(db, engine),
        }
    }
}
```

Rust의 구조체 필드는 TypeScript 객체 속성과 비슷하지만, `EngineHandle` 복제는 거래 상태 복사가 아니다. 채널 송신 핸들만 복제되고 실제 주문장과 잔액은 엔진 워커가 소유한다. `CexState`에서 사용하지 않는 엔진·잔고 서비스 필드를 제거해 HTTP 상태가 거래 상태 참조를 보관하지 않게 했다.

출처: [connection.rs](../../src/shared/database/connection.rs)의 부분 발췌.

```rust
#[cfg(any(test, feature = "bench_mode"))]
pub fn noop(pool: PgPool) -> Self {
    Self { pool }
}
```

`cfg`는 조건이 참일 때만 항목을 컴파일한다. Cargo의 실제 기능 이름과 정확히 같아야 하며, 존재하지 않는 이름은 폴백이 아니라 비활성 코드가 된다.

## 작성한 테스트

- [audit_engine_architecture.py](../../scripts/audit_engine_architecture.py): 제품 Rust 소스의 `Mutex`/`RwLock` 선언 0, 폐기 파일·타입 참조 0, 상태 이동 1회, 코어·저널·출력 장수 워커 경계, `bench_mode` 이름 일치를 7개 검사로 확인한다.
- [verify_order_safety.py](../../scripts/verify_order_safety.py): 서비스 신원, 감사 읽기, 시장 출력, 저널 포화·손상·재시작, 입금·출금, 소유권 이동, 정밀도, 주문 접수, 시작 차단, HTTP 경계를 실행별 소유 PostgreSQL에서 검증한다.
- 기존 핵심 예: `ownership_worker_moves_state_and_matches_direct_core_results`, `journal_kafka_cannot_overtake_durable_watermark`, `journal_full_queue_preserves_input_and_does_not_confirm_uncommitted_order`, `withdrawal_http_waits_for_the_actual_journal_commit_before_success_or_output`.

## 실행 명령과 결과

- `python3 scripts/audit_engine_architecture.py`: 7개 통과, 실패 0.
- `python3 scripts/verify_order_safety.py`: 고유 213개·실행 251회 통과, 실패·제외 0, 19개 검증 묶음과 격리 PostgreSQL 정리 성공.
- `cargo test --locked --offline --no-run`: 전체 대상 컴파일 성공. 기존 SQLx 0.6.3 미래 비호환 경고와 별도 미사용 코드 경고는 남아 있다.
- 처음 `DATABASE_URL` 없이 `cargo test --all-targets`를 실행해 순수 시험 159개는 통과하고 SQLx DB 시험 51개는 환경 누락으로 실패했다. 제품 실패로 합산하지 않고 격리 실행기로 전부 다시 검증했다.

## 완료 범위와 남은 한계

현재 제품 소스에 공유 `Mutex`/`RwLock` 선언은 없고, 거래 상태는 한 코어 워커로 한 번 이동한다. 저널과 출력은 각각 별도 장수 워커다. 정적 검사는 향후 간접 의존성 내부 구현이나 운영 CPU·디스크 경쟁을 증명하지 않으므로 실제 포화·강제 종료·성능 측정은 계속 별도 검증해야 한다.

엔진에는 아직 오래된 공개 타입·조회 메서드와 영어 병기 주석, SQLx 0.6 미래 비호환 경고가 남아 있다. 이번 사이클은 사용 여부가 명백한 DB 잔고/모의 코어 경로만 삭제했으며, 다음 감사에서 실제 API와 시험 참조를 대조해 더 좁힌다.

## 관련 커밋과 참고 자료

- 구현·검증: `ad338bd`
- [검증 증적](../reference/025-engine-architecture-final-audit-verification-2026-10-27.json)
