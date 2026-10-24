# 시장가 체결 뒤 종료와 미사용 예약 반환을 확정하기

시장가 주문이 일부만 체결되거나 체결이 없어도 플랫폼이 최종 상태와 예약금을 확정하도록 종료 결과를 전달한다. 코어의 단일 스레드·공유 락 제거와 WAL 확정 후 공개 원칙은 유지한다. 작성일은 2026-10-22다.

## 문제와 기존 동작

엔진은 시장가 주문을 오더북에 남기지 않고 사용하지 않은 예산·수량을 메모리에 반환했다. 그러나 출력에는 개별 체결만 있었다. 플랫폼은 잔량 종료를 알 수 없었고, 체결이 없으면 반환할 근거 자체를 받지 못했다.

플랫폼의 예산 기반 매수는 원본 수량이 미정이다. 상태 행의 `amount=0`을 체결 수량 상한으로 검사하면 모든 정상 체결을 거부한다. 실제 지출 예산, 체결 수량, 미사용 예약을 함께 정의해야 한다.

## 기존 제품과 공식 자료

[바이낸스 시장가 FAQ](https://github.com/binance/binance-spot-api-docs/blob/master/faqs/market_orders_faq.md)는 시장가가 이용 가능한 유동성 안에서 체결된다고 설명한다. [주문 상태 정의](https://github.com/binance/binance-spot-api-docs/blob/master/enums.md)는 주문 규칙에 따른 미체결 종료를 `EXPIRED`로 구분한다. 확인일은 2026-10-22다.

이 프로젝트는 현재 시장가를 즉시 체결 가능한 만큼 처리하고 잔여분을 종료하는 IOC 방식으로 유지한다. IOC는 즉시 체결 후 남은 부분을 취소하는 주문 실행 규칙이다. 내부 워커 구성이나 이벤트 분리는 공개 문서에서 추측하지 않고 자체 책임 경계에 맞게 설계했다. 별도 지정가 IOC·FOK·가격 보호 기능을 추가한 작업은 아니다.

## 선택과 트레이드오프

| 선택 | 이점 | 비용·제약 |
| --- | --- | --- |
| 개별 체결 뒤 시장가 종료 이벤트 추가 | 체결 0건도 상태·예약을 확정할 근거가 있음 | 시장가당 이벤트 1건 증가 |
| 체결과 종료를 같은 엔진 키·금융 토픽으로 발행 | 체결 반영 뒤 반환을 검증할 수 있음 | 순차 소비와 중복 검증이 필수 |
| 원본 예약·체결 합계·반환·건수·상태를 WAL 결과에 저장 | 복구가 현행 디비 잔고나 추측에 의존하지 않음 | 영속 결과 형식이 바뀜 |
| 남은 예산·수량이 있으면 `expired` | 부분 체결과 종료를 구분하고 체결 기록 보존 | 최소 단위 때문에 못 쓴 작은 예산도 만료로 표시 |
| 플랫폼은 마지막 종료 결과에서 잔여 예약 반환 | 후속 주문의 예약을 건드리지 않음 | 체결 반영과 종료 사이에는 잠시 `partial`로 보일 수 있음 |

`filled`는 원본 매수 예산 또는 매도 수량이 모두 사용된 경우다. `expired`는 미사용분이 반환된 종료이며 거부·사용자 취소와 다르다. 수수료는 이전 결정대로 받은 자산에서 차감하므로 예약 반환과 중복 차감하지 않는다.

## 구현과 관련 코드

- [후보 정산](../../src/domains/cex/engine/runtime/submission.rs): 기존의 예약·전체 체결·반환 계산에서 `MarketCompletion`을 구성한다. 결과에는 주문·소유자·거래쌍·방향·체결 수량·체결 대금·원본 예약·반환액·체결 건수·최종 상태가 들어간다. 오류 가능한 금액 계산은 오더북과 실행 잔고 확정 전에 끝난다.
- [출력](../../src/domains/cex/engine/runtime/output.rs): `Submitted` 결과에 종료 정보를 보존한다. 지정가는 종료 정보가 없으며 시장가만 개별 체결 뒤 `market_order_completed`를 보낸다. 코어는 디비·네트워크·디스크를 호출하지 않는다.
- [이벤트](../../src/shared/kafka/events.rs): 영속 종료 결과를 빌려 직렬화한다. 출력 워커가 수량·반환액을 다시 계산하지 않는다.
- [Kafka](../../src/shared/kafka/producer.rs): 체결과 같은 `order-events`, 같은 엔진 식별자 키를 사용한다. 금융 토픽은 초기 파티션 1개다.
- [최초 기동 검사](../../src/domains/cex/engine/runtime/startup.rs): `expired`도 종료 이력으로 구분한다. 기존 접수 증명이나 예약이 남아 있으면 여전히 기동을 막는다. 자체 WAL이 있는 재시작은 이 경로를 사용하지 않는다.

종료 식별자는 `엔진ID:저널순번:체결건수`다. 개별 체결 인덱스는 0부터 체결건수 직전까지여서 서로 충돌하지 않는다. 체결 0건의 종료 인덱스는 0이다. 기존 플랫폼 수신 키 형식을 그대로 사용하며 다른 토픽·새 큐를 만들지 않는다.

출력 워커는 체결·종료·공개 시세 모두 브로커 확인을 받은 뒤 전달 커서를 저장한다. 종료 전달에 실패하면 커서를 진행시키지 않고 새 입력을 닫는다. 재시작은 동일 체결·종료를 같은 식별자로 재발행하며 플랫폼 수신 키가 재차감·이중 반환을 막는다. 브로커 확인과 플랫폼 정산 완료는 서로 다른 경계다.

## 이해에 필요한 Rust 문법

`Option<MarketCompletion>`은 TypeScript의 `MarketCompletion | null`과 비슷한 결과 구분이다. `Some`은 종료 정보가 있고 `None`은 없다는 뜻이다. 적용률을 추측하는 기본값이나 복구 폴백으로 쓰지 않는다.

`then(|| ...)`은 시장가일 때만 결과를 구성한다. 결과는 기존 명령의 정산 합계에서 만든다. `&MarketCompletion`은 출력 워커가 이미 확정한 값을 빌려 직렬화한다는 뜻이며 실행 캐시 참조를 다른 스레드와 공유하지 않는다. `#[serde(flatten)]`은 이 결과 필드를 Kafka 이벤트의 최상위 필드로 직렬화한다.

### 현재 코드로 연결해 읽기

체결 목록 뒤에 시장가 종료 이벤트를 같은 확정 명령의 순서로 만든다. 아래는 2026-10-23 현재 [실제 소스](../../src/domains/cex/engine/runtime/output.rs#L158)의 연속 행 발췌다. 메서드 일부일 수 있으므로 독립 실행 예제로 사용하지 않고 앞뒤 검증과 함께 읽는다. 과거 사이클의 코드/측정 시점은 본문의 기록을 유지한다.

```rust
if let Some(completion) = completion {
    // 체결 뒤에 같은 파티션으로 전송한다. 마지막 이벤트 전 실패하면
    // 전달 커서를 남겨 재시작에서 체결·종료 모두 같은 식별자로 재전달한다.
    let event = MarketOrderCompletedEvent {
        event_id: format!(
            "{}:{}:{}",
            position.engine_id,
            position.sequence,
            trades.len()
        ),
        engine_id: position.engine_id.into(),
        event_type: "market_order_completed",
        timestamp: position.timestamp,
        result: completion,
    };
    send(trades.len(), encode(&event, trades.len())?)?;
}
```

`if let Some`은 종료 결과가 있을 때만 처리한다. 이벤트 ID의 순번과 체결 개수는 재전달 시에도 같아야 한다. 실제 미사용 예약 반환은 플랫폼이 이 종료 근거를 검사해 수행한다.

## 작성한 테스트

| 파일 | 함수 | 시나리오·기대 결과 |
| --- | --- | --- |
| [실제 후보 정산](../../src/domains/cex/engine/runtime/engine/settlement_pipeline_tests.rs) | `settlement_ioc_completes_full_partial_and_empty_buy_and_sell` | 매수·매도 각각 체결 없음·부분·전량, 최종 상태·반환·예약 0·오더북 잔존 없음 |
| 같은 파일 | `settlement_ioc_refunds_unspendable_budget_dust` | 예산 1, 가격 3에서 0.333333333 체결, 0.999999999 사용, 0.000000001 반환 |
| 같은 파일 | `settlement_ioc_self_only_liquidity_returns_entire_reservation` | 자기 주문만 있으면 자전거래 없이 전액 반환하고 기존 호가 보존 |
| [실제 워커·저널](../../src/domains/cex/engine/runtime/engine/journal_tests.rs) | `journal_ioc_zero_fill_and_partial_completion_replay_without_second_refund` | 체결 0·부분 결과의 WAL 보존·실제 워커 재시작·중복 명령의 이중 반환 방지 |
| 같은 파일 | `journal_ioc_completion_failure_keeps_cursor_and_identical_retry` | 출력 실패 주입 후 커서 보존·동일 결과 재전달 |
| 같은 파일 | `journal_ioc_contract_rejects_previous_fee_version_without_mutation` | 이전 계약 기동 거부·저장 버전 원본 보존 |
| [최초 기동 회귀](../../src/domains/cex/engine/runtime/engine/startup_tests.rs) | `startup_preserves_exact_balances_with_terminal_order_history` | 접수·예약 없는 종료 이력에 `expired` 포함 |

## 실행 명령과 결과

`python3 scripts/verify_order_safety.py`: 실행 소유 PostgreSQL에서 고유 Rust **129개 통과**, 필터 중복을 포함하면 **131회**다. 전체 테스트 컴파일·제품 빌드도 통과했다. 공유 외부 의존성에 연결하는 기존 테스트 전체를 실행한 결과는 아니다.

`cargo check --locked --benches --features bench_mode`: 벤치 컴파일 확인이며 처리량 측정이 아니다. 실제 제품 프로세스·플랫폼 결과는 [플랫폼 기록](../../../Cex-Platform-Service/docs/decisions/009-market-ioc-settlement.md)을 참고한다. 기존 미사용 코드·향후 Rust 버전 호환 경고는 남아 있다.

## 저장 계약과 운영 범위

자체 저널 형식은 **버전 3**이다. 시장가 종료 정보가 없는 버전 1·2를 추측하거나 변환하지 않고 기동을 거부한다. 과거 저널과 실제 디비를 수정·삭제하거나 기존 서비스를 재시작하지 않았다. 새 바이너리를 이전 저널에 바로 배포할 수 없으며, 금융 이력을 가진 환경의 전환은 별도 결정이 필요하다.

플랫폼은 누적 체결 대금을 `NUMERIC(30,9)`로 저장하고 상태 제약에 `expired`를 허용해야 한다. 이번 검증은 새 격리 스키마다. 기존 환경의 제약이나 소수 8자리 컬럼을 자동 변경하는 변환을 추가하지 않았다. 디스크·호스트 손실의 외부 복제·백업은 기존 합의대로 문서상의 필요 조건이다.

시장가 엔진 거절·플랫폼 응답 불명 자동 해소, 가격 보호·최소 주문 금액, 자산 원장·독립 대사는 후속 범위다.

## 관련 커밋

- [엔진 구현·테스트 `722bfa6`](https://github.com/GiHoon1123/Cex-Engine/commit/722bfa6f69facc3bfc0ab2517987d5e4543fd0ba)
- [플랫폼 구현·테스트 `7eed751`](https://github.com/GiHoon1123/Cex-Platform-Service/commit/7eed7510be774a7d3f493100d958f858f04f37cf)

두 저장소의 종료 이벤트 계약과 저장 정밀도가 함께 바뀐다. 개별 프로세스만 먼저 배포하는 검증은 수행하지 않았다.

## 후속 상태 갱신

거절 결과·응답 불명 자동 복구는 [다음 사이클](014-durable-order-rejection.md)에서 구현·검증했다. 현재 저널 버전은 4다. 이 문서의 버전 3은 당시 저장 계약이다. Rust 집계는 진행 로그로 나뉜 시작 테스트 4개 누락을 정정해 고유 129개/131회로 표시했다.
