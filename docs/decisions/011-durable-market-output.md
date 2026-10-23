# WAL에서 자체 호가와 공개 체결을 발행하기

체결 코어에 호가 조회·전송 비용을 추가하지 않고, 확정된 자체 거래 결과를 플랫폼과 화면으로 전달한다. 기존 금융 WAL 형식과 단일 스레드의 독점 상태를 유지하는 방법을 설명한다.

## 문제와 검증된 접근

기존 체결·취소 Kafka 출력에는 미체결 주문의 가격별 잔량이 없었다. 프론트가 바이낸스 호가를 표시하면 자체 주문·체결·취소가 화면과 연결되지 않는다. 남아 있던 UDP 호가 서비스는 실행에 연결되지 않았고 SOL 거래쌍을 고정해 조회했다.

[LMAX 소비자 의존성](https://lmax-exchange.github.io/disruptor/user-guide/)처럼 외부 공개가 저널 확정 순번을 추월하지 않게 한다. [Coinbase 공개 호가 피드](https://help.coinbase.com/en/developer-platform/websocket-feeds/exchange)의 스냅샷과 후속 갱신 역할을 참고했다. 현재는 델타 병합 대신 매번 상위 20개 전체 스냅샷을 제공한다.

## 선택과 트레이드오프

| 선택 | 얻는 이점 | 비용과 제약 |
| --- | --- | --- |
| 출력 워커의 별도 공개용 집계 | 코어 복사·공유 락·외부 조회가 없음 | 미체결 주문 인덱스·가격별 합계를 출력 워커에도 보관 |
| 기존 WAL 명령·확정 결과에서 집계 | 금융 WAL 형식과 과거 기록을 바꾸지 않음 | 재시작 때 출력 전달 커서 이전 이력도 다시 읽음 |
| 전체 상위 20개 호가 | 최초 접속·누락·재연결의 화면 동기화가 단순함 | 델타보다 큰 메시지, 공개 깊이 20개 |
| 금융 결과 뒤 시장 데이터 전달, 둘 다 확인 후 커서 저장 | 중간 실패를 같은 저널 위치로 재시도 | 두 토픽의 원자적 커밋은 아님. 시장 토픽 장애도 신규 거래 차단으로 이어짐 |
| 기존 정수 라이브러리로 호가 수량 합산 | 여러 계정 합이 개별 잔액 상한을 넘어도 정확함 | 출력 워커에서 큰 정수 연산·문자열 변환 수행 |

Kafka의 브로커 확인은 플랫폼 정산 완료가 아니다. 금융·시장 소비자가 각각 늦을 수 있다. 새 성능 수치는 측정하지 않았으며 과거 순수 코어 벤치를 전체 경로 성능으로 사용하지 않는다.

## 구현과 계약

[공개 집계](../../src/domains/cex/engine/runtime/market.rs)는 성공한 최초 주문·체결·취소만 반영한다. 거부·중복 주문·중복 취소·잔액 명령은 호가를 바꾸지 않는다. 체결 가격·수량은 확정된 결과를 사용하며 매칭·정산을 다시 실행하지 않는다.

[출력 워커](../../src/domains/cex/engine/runtime/output.rs)는 원자적으로 공개된 영속 순번 이하만 읽는다. 금융 이벤트를 전달한 뒤 `market-events`에 시장 스냅샷을 보내고, 두 전달의 확인 이후에만 저널 전달 커서를 저장한다. 재시작에서는 전달 커서까지 공개 집계를 복원해 각 거래쌍의 최신 기준점을 다시 발행한다. 실패 후 같은 식별자·시각·본문을 재현한다.

| 필드 | 의미 |
| --- | --- |
| `event_id` | `엔진 UUID:저널 순번:market` |
| `engine_id` | 자체 저널의 엔진 UUID |
| `sequence` | 이번 호가 변경의 전역 저널 순번, 문자열 |
| `previous_sequence` | 같은 거래쌍의 이전 호가 변경 순번, 문자열 |
| `base_mint`·`quote_mint` | 거래쌍, 현재 화면 기본값 BTC/USDT |
| `bids`·`asks` | 가격·잔량 문자열 쌍, 각각 내림차순·오름차순 상위 20개 |
| `trades` | 이번 주문의 공개 체결, 안정적인 체결 ID·가격·수량·유입 방향 |

잔액 명령과 중복 요청이 사이에 있으면 전역 순번은 건너뛸 수 있다. 수신자는 단순히 `sequence+1`을 기대하지 않고 `previous_sequence`를 확인한다. 사용자 ID·잔액·미체결 주문 ID는 공개하지 않는다. 두 토픽은 초기 구성에서 각각 파티션 1개이며 같은 엔진 UUID를 키로 사용한다.

활성 HTTP 경로에서 브라우저용 CORS와 사용하지 않는 엔진 WebSocket 기능·UDP 호가 서비스 및 직접 의존성을 삭제했다. 엔진 기본 바인딩은 내부 루프백이다. 컨테이너에서는 내부 네트워크에만 노출해야 한다.

## 취소의 권위

취소 때 플랫폼 주문 행을 다시 조회하던 경로를 제거했다. 실제 두 서버 검증에서 플랫폼의 시간 컬럼과 Rust 조회 모델 차이가 드러났고, 비동기 반영 전의 주문 상태를 엔진의 현재 상태보다 우선하는 문제도 있었다.

플랫폼은 인증한 `user_id`와 원본 `base_mint`·`quote_mint`를 내부 DELETE 쿼리로 보낸다. 코어가 소유자·거래쌍·현재 잔량을 검증하고 자체 취소 이력으로 중복을 처리한다. 취소 성공 응답도 WAL 확정 뒤에 공개한다. 플랫폼의 금융 상태 변경 책임은 옮기지 않는다.

## 문법과 원리

- `BTreeMap<Decimal, BigInt>`는 가격순으로 정렬된 잔량 합계다. 1e-9 단위의 정수로 합산해 개별 계정의 `NUMERIC(30,9)` 범위를 전체 호가 상한으로 잘못 적용하지 않는다.
- 출력용 `HashMap`은 출력 워커가 독점 소유한다. TypeScript의 별도 워커에 공개용 상태를 두는 것과 비슷하지만 Rust에서는 코어 상태 참조를 전달하지 않아 컴파일 단계의 접근 경계도 유지한다.
- `FnMut` 콜백은 외부 전달을 검증용 함수로 바꿀 수 있게 한다. 제품에서는 같은 전용 워커의 Kafka 전달을 호출한다. 코어에 테스트 분기나 디비 접근을 넣지 않는다.

### 현재 코드로 연결해 읽기

공개 호가는 코어 상태를 직접 빌리는 대신 확정 기록의 투영으로 갱신한다. 아래는 2026-10-23 현재 [실제 소스](../../src/domains/cex/engine/runtime/market.rs#L160)의 연속 행 발췌다. 메서드 일부일 수 있으므로 독립 실행 예제로 사용하지 않고 앞뒤 검증과 함께 읽는다. 과거 사이클의 코드/측정 시점은 본문의 기록을 유지한다.

```rust
let event = MarketSnapshot {
    event_type: "market_snapshot",
    event_id: format!("{engine_id}:{}:market", record.sequence),
    engine_id: engine_id.into(),
    sequence: record.sequence.to_string(),
    previous_sequence: book.sequence.to_string(),
    base_mint: pair.base_mint.clone(),
    quote_mint: pair.quote_mint.clone(),
    timestamp: record.timestamp,
    // 상위 20개 전체 스냅샷을 택해 누락 뒤에도 최신 호가로 바로 재동기화한다.
    // 델타보다 전송량이 크지만 코어의 복사 비용과 클라이언트의 델타 병합 상태가 없다.
    bids: book
        .bids
        .iter()
        .rev()
        .take(20)
        .map(|(p, q)| [p.to_string(), quantity_text(q)])
        .collect(),
    asks: book
        .asks
        .iter()
        .take(20)
        .map(|(p, q)| [p.to_string(), quantity_text(q)])
        .collect(),
    trades,
};
```

`.iter().rev().take(20)`은 최선 가격부터 최대 20개 가격 수준을 뽑는다. 이 메서드의 `&mut self`는 출력 투영의 로컬 상태를 갱신한다. 코어의 오더북과 별도 소유 상태이며, 순번과 중복/거절 처리 규칙을 따라 공개용 스냅샷을 만든다. 시장 데이터도 저널 확정 범위에서 나온다.

## 작성·수정한 테스트

[시장 집계 테스트](../../src/domains/cex/engine/runtime/market.rs), 5개:

- `market_projection_tracks_real_partial_fill_cancel_and_replay`: 제품 명령 계산으로 부분 체결·취소·재생을 확인.
- `market_projection_skips_duplicates_and_rejections_and_preserves_other_pairs`: 중복·거부의 미반영과 거래쌍 분리.
- `market_projection_market_buy_never_leaves_resting_budget`: 시장가 IOC 잔액을 호가로 남기지 않음.
- `market_projection_depth_is_sorted_bounded_and_promotes_hidden_levels`: 상위 20개 정렬과 취소 후 다음 가격 승격.
- `market_projection_aggregates_beyond_one_account_limit_without_rounding`: 두 계정의 큰 잔량 합과 차감을 정확히 유지.

[저널 회귀](../../src/domains/cex/engine/runtime/engine/journal_tests.rs): `journal_market_failure_keeps_cursor_and_replays_identical_snapshot`을 추가했다. 시장 전달 실패 시 커서 보존과 동일 본문 재발행을 검증한다. 기존 영속 순번 추월 검사에는 시장 출력도 포함했다. DB 비의존 복구 테스트는 외부 출력 없는 예약 명령으로 검증하고, 실제 주문 복구는 격리 Kafka와 함께 검증한다.

[취소 경계](../../src/domains/cex/services/order_service.rs): `cancellation_uses_owned_core_without_platform_database`는 접속 불가능한 디비를 주입해 본인·거래쌍 검증, 성공·중복 취소와 잔액 복원을 확인한다. 라우터의 누락 쿼리 검증도 새 계약에 맞췄다.

## 실행과 한계

2026-10-21, Rust 1.91.0, PostgreSQL 16·Kafka 3.9.1의 실행 소유 컨테이너로 검증했다.

```sh
python3 scripts/verify_order_safety.py
python3 scripts/verify_engine_output.py
python3 scripts/verify_durable_acceptance.py
cargo check --locked --benches --features bench_mode
```

선택 회귀 **112개 고유 테스트, 중복 선택 포함 114회 통과**, 전체 테스트 컴파일·제품 빌드·벤치 컴파일 통과. 외부 DB가 필요한 전체 통합 테스트를 무조건 실행한 결과는 아니다. 출력 프로세스 검증 6개 시나리오, 실제 두 서버 접수·응답 유실·재시작 검증 3개 시나리오를 통과했다. 기존 미사용 코드 경고와 Solana·SQLx의 향후 Rust 호환 경고가 남는다.

수탁망 전환·기존 SOL 기록 변경·대량 성능 재측정·저널 압축·아주 큰 단일 체결 묶음의 Kafka 메시지 크기 검증은 수행하지 않았다. 브로커 최대 메시지 크기를 넘는 출력은 성공으로 건너뛰지 않고 저널을 보존한 채 차단한다. 디스크 손실 복제·백업은 기존 지시에 따라 운영 필요 조건만 문서화한다.

플랫폼의 수신·공개 API·봇 제한과 실제 전체 경로는 [플랫폼 기록](../../../Cex-Platform-Service/docs/decisions/007-platform-market-gateway.md), 화면 검증은 [프론트 기록](../../../Cex-Frontend/docs/decisions/001-platform-market-and-btc.md)을 따른다.

## 관련 구현 커밋

- [시장 출력·취소 경계·검증](https://github.com/GiHoon1123/Cex-Engine/commit/92561a3)
- [엔진 봇 제거](https://github.com/GiHoon1123/Cex-Engine/commit/e527400)
- [플랫폼 연결](https://github.com/GiHoon1123/Cex-Platform-Service/commit/d6bbe17)
