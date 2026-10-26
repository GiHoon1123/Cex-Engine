# 거래 식별자 없는 잔액 증감 HTTP 경로 제거

현재 호출자가 없는 POST /api/cex/balances/sync를 삭제했다. 신규 입금은 [018 멱등 반입](018-idempotent-deposit-credit.md)의 식별자/금융 내용 계약으로 연결하며, 원래 잔액 명령의 금융 저널 재생은 보존한다. 공통 사이클076이다.

## 문제와 선택

플랫폼의 미사용 도우미만 이 HTTP를 호출했다. 입금 식별자가 없는 변화량은 응답 유실 재시도 때 중복 반영될 수 있다. 서비스 키가 있어도 이 구형 경로를 사용할 수 없도록 핸들러/등록을 삭제했다. 호환 엔드포인트·폴백·DB/금융 기록 삭제는 추가하지 않았다. 직접 수동 운영 도구가 사용할 수도 없게 되는 제약은 실제 참조 확인 후 감수한다.

주문/취소와 상위 서비스 신원 검증·헬스·독립 감사 조회는 유지한다. 활성화되지 않았던 라우트 주석 목록도 정리했다. 제품/공식 멱등 패턴, Java 삭제/시험 수정과 실제 전체 거래 회귀는 [플랫폼076](../../../Cex-Platform-Service/docs/decisions/076-remove-retired-balance-sync.md)이 주 기록이다. 같은 설명을 중복 복사하지 않는다.

## 실제 코드와 원리

출처: [routes.rs](../../src/domains/cex/routes.rs#L15), 현재 활성 경로의 부분 발췌.

```rust
        .route("/orders", post(handlers::create_order))
        .route("/orders/:order_id", delete(handlers::cancel_order))
```

Router<AppState>는 상위에서 상태를 공급받을 라우터다. 발췌는 주문 생성·취소 등록이다. 현재는 원본 식별 입금/출금 명령도 있지만, 원본 없는 잔액 증감의 삭제 경로는 계속404다. 아래 실행 수치는019 당시 결과다. 서비스 신원은 상위 route_layer가 활성 경로에 적용한다. route를 남겨 내부 오류를 반환하는 대안 대신 등록 자체를 없앴다.

## 테스트와 실행

[src/routes/tests.rs](../../src/routes/tests.rs)의 신규 retired_balance_delta_route_is_absent_for_all_service_identities는 실제 라우터에 유효 서비스 키/무키로 올바른 증감 JSON을 보내도404인지 검사한다. trading_routes_still_validate_request_bodies는 남은 주문 요청의 빈 JSON422를 검증한다. 기존 취소·헬스·인증/경로 경계도 안전성 실행기에 포함한다.

Cex-Engine에서 python3 scripts/verify_order_safety.py를 실행했다. 소유 PostgreSQL·자체 임시 저널에서 **고유148개 통과, 선택 실행161회**, 실패/제외0개이며 전체 컴파일/제품 build가 통과했다. Java 회귀179개와 실제 시장 연결1개는 플랫폼076의 명령·증적을 따른다. Rust bin59개(중복7개 포함)·SQLx 미래 호환 경고는 남는다. 새 처리량 측정/신규 입금의 플랫폼 원장 반영 시험은 아니다.

## 관련 커밋

- [엔진 구현417613a](https://github.com/GiHoon1123/Cex-Engine/commit/417613a)
- [플랫폼 구현0e937b5](https://github.com/GiHoon1123/Cex-Platform-Service/commit/0e937b5)
- [전체 실패/수정/최종 증적](../../../Cex-Platform-Service/docs/reference/076-remove-retired-balance-sync-verification-2026-10-25.json)

092 집계 정정: 당시 고유148/실행161은 외부 로그 끼어들기로 시작 시험4개가 빠진 값이다. 보존 로그의 실제 고유152/실행165 통과를 [020 정정 기록](020-typed-deposit-outcomes.md)에서 확인한다.076 제품 변경이나 과거 증적 JSON을 바꾸지 않았다.
