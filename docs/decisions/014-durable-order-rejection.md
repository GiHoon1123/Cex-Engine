# 전체 주문 거절을 WAL 결과로 보존하고 플랫폼에 전달하기

체결 코어가 주문 전체를 거절했을 때 플랫폼이 예약을 안전하게 반환할 근거를 추가했다. 명령의 영속 접수와 거래 성공을 구분하고, 재시작·중복 명령·재발행에도 같은 거절을 재현한다. 작성일은 2026-10-22다.

## 문제와 기존 동작

코어는 전체 후보 정산이 실패하면 금융 상태를 바꾸지 않고 `Rejected`를 저널에 기록했다. HTTP 응답은 명령과 결과가 WAL에 확정됐다는 접수 확인이었다. 그러나 출력 워커는 거절을 발행하지 않았다. 플랫폼은 접수 확인을 받아도 예약을 반환할 근거가 없었고, 응답마저 잃으면 미전송과 실행 거절을 구분할 수 없었다.

HTTP 4xx·5xx·시간 초과를 거절로 간주하면 이미 처리한 주문의 자산을 재사용하게 된다. 따라서 기존의 실패 시 예약 보존을 유지하면서, 전체 거절을 금융 결과로 전달해야 한다.

## 기존 제품과 공식 자료

[AWS의 transactional outbox 설명](https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/transactional-outbox.html)은 업무 변경과 전송 기록을 같은 트랜잭션에 저장하고, 중복 전달을 받는 쪽에서 방지하는 패턴을 설명한다. [Stripe의 멱등 요청](https://docs.stripe.com/api/idempotent_requests)은 연결 실패 후 같은 키와 원본으로 재요청하는 계약을 제공한다. 확인일은 2026-10-22다.

여기서는 기존 WAL·출력 커서·플랫폼 수신 키를 활용한다. Stripe의 키 만료 기간이나 응답 캐시 구현을 복제하지 않는다. 플랫폼의 영속 재전송은 [플랫폼 결정](../../../Cex-Platform-Service/docs/decisions/010-durable-order-resolution.md)에 기록했다.

## 선택과 트레이드오프

| 선택 | 이점 | 비용·제약 |
| --- | --- | --- |
| 전체 거절을 금융 토픽으로 발행 | 예약 반환의 명시적인 영속 근거 | 거절당 이벤트 1건·소비 검증 추가 |
| 거절에 전체 원본·고정 수수료율 포함 | 다른 주문·변조된 결과의 반환 차단 | 저널과 메시지 크기 증가 |
| 내부 오류 문자열 대신 고정 사유 코드 공개 | 내부 정보 노출과 문자열 의존 제거 | 고객 메시지만으로 상세 원인을 알 수 없음 |
| 기존 WAL·출력 커서 재사용 | 별도 디비·큐·재전송 체계가 불필요 | 새 저널 버전이 필요하고 과거 버전은 기동 거부 |

매칭·실행 잔액·거절 판단은 단일 코어가 독점한다. 출력 워커는 공유 거래 상태를 읽지 않고 확정된 소유 값만 직렬화한다. 공유 락·코어의 네트워크/디스크 호출·주문별 스레드를 추가하지 않았다.

## 구현과 이벤트 계약

- [명령 적용](../../src/domains/cex/engine/runtime/journal.rs): 거절 결과에 `OrderEntry` 원본을 저장한다. 후보 전체 적용이 실패했으므로 오더북·잔액은 변경되지 않는다. 중복 인덱스는 거절 주문도 기억한다.
- [출력](../../src/domains/cex/engine/runtime/output.rs): `Rejected`를 `order_rejected`로 전달한다. 식별자는 `엔진ID:저널순번:0`, 시간은 해당 WAL 기록 시각이다.
- [이벤트](../../src/shared/kafka/events.rs): 주문 ID·사용자·방향·주문 방식·거래쌍·가격·수량·예산·수수료율·`ENGINE_EXECUTION_REJECTED`를 보낸다. 금융 소수는 문자열이며 없는 값은 명시적 `null`이다. 예산 기반 시장가 매수의 수량은 `null`이다.
- [발행](../../src/shared/kafka/producer.rs): 체결·취소·IOC 종료와 같은 `order-events`, 같은 엔진 키를 사용한다. 초기 파티션은 1개다.
- [저장](../../src/domains/cex/engine/runtime/journal/store.rs): 형식과 최초 상태 버전을 **4**로 올렸다. 이전 버전은 변환하지 않는다.

기록 워커의 영속 커밋 확인 전에는 접수 응답·Kafka 공개가 불가능하다. 거절 발행에 실패하면 출력 커서를 진행하지 않고 새 입력을 닫는다. 재시작은 자체 저널에서 같은 원본과 결과를 복원하고 같은 ID로 재발행한다. 플랫폼 수신 키가 이중 반환을 막는다.

같은 주문 ID·원본의 재전송은 거절을 다시 계산하지 않는다. 새 결과도 만들지 않는다. 최초 결과의 미발행·재발행은 출력 커서가 담당한다. 브로커 확인은 플랫폼의 금융 반영 완료를 의미하지 않는다.

## 이해에 필요한 Rust 문법

`Rejected { order: OrderEntry, reason: String }`은 하나의 enum 분기가 원본을 소유한다는 뜻이다. 출력 워커로 오더북 참조를 넘기지 않는다. `OrderRejectedEvent<'a>`의 `&'a str`은 그 결과에 있는 문자열을 잠시 빌려 직렬화한다. TypeScript 객체 참조와 달리 빌림이 원본보다 오래 살아남지 못하도록 컴파일러가 검사한다.

`Option<Decimal>`은 값이 없음을 명시한다. 시장가 매수의 미정 수량을 0으로 보내 원본과 혼동하지 않는다. `map_err`는 브로커 오류를 출력 실패로 변환하며, 여기서 실패를 성공으로 숨기지 않는다.

### 현재 코드로 연결해 읽기

거절도 원본 주문과 연결된 영속 결과 이벤트로 공개한다. 아래는 2026-10-23 현재 [실제 소스](../../src/domains/cex/engine/runtime/output.rs#L95)의 연속 행 발췌다. 메서드 일부일 수 있으므로 독립 실행 예제로 사용하지 않고 앞뒤 검증과 함께 읽는다. 과거 사이클의 코드/측정 시점은 본문의 기록을 유지한다.

```rust
OutputBatch::Rejected { order, .. } => {
    // 거절도 같은 명령의 영속 결과다. 오류 문자열이나 HTTP 상태를 반환 근거로
    // 사용하지 않고 원본과 연결된 고유 결과를 체결·취소와 같은 순서로 공개한다.
    let budget_buy = order.order_side == "market" && order.order_type == "buy";
    let event = OrderRejectedEvent {
        event_id: format!("{}:{}:0", position.engine_id, position.sequence),
        engine_id: position.engine_id.into(),
        event_type: "order_rejected",
        order_id: order.id,
        user_id: order.user_id,
        order_type: &order.order_type,
        order_side: &order.order_side,
        base_mint: &order.base_mint,
        quote_mint: &order.quote_mint,
        price: order.price,
        amount: if budget_buy { None } else { Some(order.amount) },
        quote_amount: order.quote_amount,
        fee_rate: order.fee_rate,
        reason_code: "ENGINE_EXECUTION_REJECTED",
        timestamp: position.timestamp,
    };
    send(0, encode(&event, 0)?)?;
}
```

`match`는 확정 결과 종류를 분기한다. `{ order, .. }`는 필요한 주문 필드만 꺼내고 나머지를 생략한다. HTTP 오류를 미실행 증거로 쓰지 않고 안정적인 이벤트 식별자로 플랫폼에 전달한다.

## 작성한 테스트

[실제 워커·저널 테스트](../../src/domains/cex/engine/runtime/engine/journal_tests.rs)에 다음을 추가했다.

| 함수 | 검증 |
| --- | --- |
| `journal_rejection_preserves_original_and_restart_duplicate_has_no_second_effect` | 잔액 부족 전체 거절·원본 보존·금융 상태 불변·실제 워커 재시작·동일 명령 재실행 방지 |
| `journal_rejection_output_failure_keeps_cursor_and_identical_original` | 거절 전달 실패·커서 보존·동일 순번/시각/원본 재전달 |
| `journal_rejection_contract_blocks_version_three_without_rewriting` | 버전 3 기동 거부·기존 파일 버전 보존 |

[실제 접수 검증기](../../scripts/verify_durable_acceptance.py)는 기존 3개에 플랫폼 재시작 2개 시나리오를 추가했다. 실제 WAL 접수 후 HTTP 응답만 유실된 주문과 엔진에 도달하지 않은 주문을 저장 기록으로 자동 전달한다. 전자는 원본 메시지가 같고 중복 실행이 없으며, 후자는 새로 한 번 실행된다. 예약을 다시 잡지 않는다.

실제 엔진의 소수 최소 단위 정산 실패 → 거절 Kafka → 플랫폼 전액 반환 → 원본 재전달·WAL 전량 재발행 검증은 [플랫폼 통합 테스트](../../../Cex-Platform-Service/src/test/java/dustin/cex/integration/MarketPathTest.java)에 있다.

## 실행 명령과 결과

| 명령 | 결과 |
| --- | --- |
| `python3 scripts/verify_order_safety.py` | Rust 고유 **132개**, 필터 중복 포함 **135회** 통과, 실패·제외 0, 전체 테스트 컴파일·제품 빌드 포함 |
| `python3 scripts/verify_engine_output.py` | 실제 Kafka·강제 종료·복구·출력 장애 **6개 시나리오** 통과 |
| `python3 scripts/verify_durable_acceptance.py` | 실제 Java/Rust·응답 유실·양쪽 재시작·자동 복구 **5개 시나리오** 통과 |
| `cargo check --locked --benches --features bench_mode` | 벤치 컴파일 통과. 성능 측정은 아님 |

macOS·Rust 1.91·Java 21·PostgreSQL 16·Kafka 3.9.1에서 실행 소유 자원만 사용하고 정리했다. 기존 미사용 코드·의존성의 향후 Rust 버전 호환 경고는 남아 있다. 전체 외부 의존성 테스트를 무조건 실행한 결과가 아니다.

이전 IOC 기록의 Rust 125/127 집계는 Kafka 진행 로그가 테스트 출력 줄 사이에 끼어든 시작 테스트 4개를 빠뜨렸다. 실제 이전 결과는 고유 129개/131회였고 해당 문서도 정정했다. 이번에는 테스트 선언과 통과 합계를 대조해 집계했다.

## 저장 계약과 남은 범위

버전 4 저널과 새 플랫폼 소비 계약을 함께 사용해야 한다. 기존 서버·디비·저널을 수정·재시작하거나 금융 이력을 삭제하지 않았다. 이전 저널의 자동 변환·플랫폼 잔액을 통한 우회 복구는 없다. 금융 이력을 가진 환경의 전환은 별도 결정이 필요하다.

지속되는 내부 정합성 오류는 자동 반환하지 않는다. 플랫폼 소비를 중단하고 운영 조사가 필요하다. 자산 원장·독립 대사·운영자 승인과 감사는 다음 범위다. 디스크·호스트 손실의 별도 복제·백업은 합의대로 문서상 요구사항으로 남긴다.

## 관련 커밋

- [엔진 구현 `9bc56a8`](https://github.com/GiHoon1123/Cex-Engine/commit/9bc56a8f31e9e0595df926ed394b23662a2961ce)
- [플랫폼 구현 `9bf1f13`](https://github.com/GiHoon1123/Cex-Platform-Service/commit/9bf1f13c481261cb441ec22af2d43c22edc1b8a2)
- [실제 프로세스 복구 검증 `8220a2f`](https://github.com/GiHoon1123/Cex-Engine/commit/8220a2fb0a89c93b45234b23b2050972192e4841)
