# Cex-Engine

## 기술 문서

- [설계 결정과 개선 기록](docs/README.md)

## 실행과 복구

단일 체결 코어와 저널·Kafka 워커를 분리한다. 엔진 자체 저널이 영속 확정된 결과만 응답·발행하고, 재시작 때 전체 명령을 검증해 재생한다. 구조·테스트·운영 한계는 [WAL 확정과 복구](docs/decisions/009-durable-journal-recovery.md)를 따른다.

- 영속 디렉터리: `ENGINE_DATA_DIR` (기본 `.engine-data`). 배포에서는 고정된 절대 경로와 영속 볼륨을 사용한다.
- HTTP: 기본 `127.0.0.1:3002`, 변경은 `ENGINE_HTTP_ADDRESS`.
- 최초 기동만 검증된 플랫폼 잔액을 시작점으로 저장한다. 저널 없이 기존 접수·활성 주문·예약금이 있으면 시작을 거부한다.
- 이후 복구는 자체 저널을 기준으로 하며, 기록 손상 시 플랫폼 DB로 우회하지 않는다.
- 디스크 손실에 대한 외부 복제·백업은 미구현이며 문서에만 필요 조건을 명시한다.

## 자체 시장 데이터

공개 호가·체결은 WAL 확정 후 `market-events`로 발행한다. 브라우저는 플랫폼에 연결한다. [시장 출력 계약과 검증](docs/decisions/011-durable-market-output.md), [엔진 봇 제거](docs/decisions/010-remove-engine-bot.md)를 참고한다.

## 주문 실패·복구의 현재 계약

[전체 주문 거절의 영속 결과와 예약 반환 근거](docs/decisions/014-durable-order-rejection.md)에서 현재 구현·테스트·저장 계약과 운영 한계를 확인한다. 엔진 저널 버전은 4이며 과거 저널을 자동 변환하지 않는다.

## 원장·독립 대사

[코어와 분리한 읽기 전용 저널 감사 조회](docs/decisions/015-read-only-journal-audit.md)에서 현재 범위와 실행 조건을 확인한다. 최초 반입·운영 접근·저장 계약은 문서의 조건을 충족해야 한다.

## 실제 구현과 함께 읽기 · 2026-10-23

이 문서의 관련 동작을 확인할 현재 코드다. 메서드·설정의 일부이며 독립 실행 예제가 아니다. 계획이나 과거 검토 전체가 구현·검증됐다는 뜻은 아니다.

출처: [handle.rs](src/domains/cex/engine/runtime/handle.rs#L20) · [전체 구현 설명·트레이드오프·작성한 테스트](docs/decisions/007-single-owner-engine-state.md)

```rust
pub struct EngineHandle {
    pub(super) order_tx: Sender<OrderCommand>,
    pub(super) balance_tx: Sender<BalanceCommand>,
    pub(super) closed: Arc<AtomicBool>,
}
```

채널 Sender의 clone은 명령 제출 권한을 복제한다. `Arc<AtomicBool>`은 수명/종료 신호이며 오더북·잔액을 공유 락으로 감싼 상태가 아니다. 실제 거래 상태는 한 워커가 소유한다.

이번 보강은 문서 변경이다. 신규 제품 테스트는 0개이며 제품 테스트는 재실행하지 않았다. 소스와 발췌의 일치를 별도로 검사했다. 과거 실행 결과와 현재 소스의 동작 범위는 연결한 구현 기록에서 구분한다.
