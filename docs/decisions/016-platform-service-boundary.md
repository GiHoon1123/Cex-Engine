# 체결 코어와 분리한 플랫폼 서비스 신원 검증

엔진의 주문·취소·잔액 명령은 플랫폼의 별도 서비스 키를 검증한 뒤 기존 HTTP 핸들러로 전달한다. 고객 로그인·역할·승인 정책은 플랫폼에 두고, 단일 스레드 코어의 상태·명령·입출력 계약은 바꾸지 않았다.

## 문제와 기존 동작

이전 엔진은 고객 인증 도메인을 제거했지만 명령 호출자의 플랫폼 신원을 검증하지 않았다. 루프백 기본 바인딩은 외부 노출을 줄여도 같은 호스트의 임의 호출자를 인증하지는 않는다. 요청 본문의 `user_id`만으로 고객 인증을 대체할 수 없다.

## 기존 제품·공식 자료의 접근

외부 API의 권한 분리는 [업비트 인증 문서](https://docs.upbit.com/kr/reference/auth), 서비스 경계와 사용자 정책의 분리는 [플랫폼 근거 검토](../../../Cex-Platform-Service/docs/reference/access-control-compliance.md)를 참고했다. API 키 제품의 고객 기능 전체를 엔진에 복사하지 않고, 내부 플랫폼 호출 신원만 검증한다.

## 선택과 트레이드오프

현재 하나의 플랫폼만 명령을 전달하므로 별도 공유 키와 내부 네트워크 경계를 선택했다. 인증 서버·고객 권한 DB를 엔진에 추가하지 않는다. HTTP 경계에서 헤더 해시 계산 비용이 생기지만 코어의 계산·소유권·비동기 워커는 유지한다. 공유 키가 유출되면 해당 플랫폼 신원으로 호출할 수 있어 비밀 저장·교체·접근 제한이 필요하다. 여러 서비스별 신원이나 원격 배포가 필요해지면 검증된 mTLS 등과 운영 비용을 비교해야 한다.

## 구현과 관련 코드

- [라우터](../../src/routes/mod.rs)의 `create_router(service_token)`은 거래 명령 라우트에만 미들웨어를 붙인다. `x-engine-service-token`을 확인하며 사용자 JWT와 감사 키는 대체 수단이 아니다. 이미 설치한 Solana SDK의 SHA-256 기능을 사용해 원본 키의 접두사를 직접 비교하지 않는다. 해시 비교만으로 모든 부채널 방어를 검증했다고 주장하지 않는다.
- [시작 코드](../../src/main.rs)는 `ENGINE_SERVICE_TOKEN`의 존재와 32~1024바이트 길이를 워커 시작 전에 검사한다. 키 누락 시 시작하지 않으며 이전의 무인증 경로로 우회하지 않는다.
- 헬스체크는 기존 공개 상태를 유지하고, `/internal/audit/results`는 별도의 `ENGINE_AUDIT_TOKEN`을 사용한다. 감사 키에는 거래 명령 권한을 주지 않는다.
- 플랫폼의 [EngineHttpClient](../../../Cex-Platform-Service/src/main/java/dustin/cex/shared/http/EngineHttpClient.java)가 주문·취소·잔액 요청에 별도 키를 넣는다. `user_id`의 소유자·고객 정책 검증은 플랫폼 책임이다.

새 환경의 엔진과 플랫폼에 **동일한 무작위 서비스 키**를 각각 `ENGINE_SERVICE_TOKEN`으로 공급한다. 플랫폼의 `JWT_SECRET`, 엔진 감사용 `ENGINE_AUDIT_TOKEN`과 분리한다. 키를 명령행·Git·일반 로그에 기록하지 않는다. 엔진은 기본 루프백 바인딩을 유지한다. 키 교체는 현재 두 프로세스의 설정 변경·재시작이 필요하고, 기존 미확인 주문은 영속 전송 기록이 이어받는다. 이 설명은 실제 키 교체 훈련을 완료했다는 뜻이 아니다.

고객·운영자 권한·승인 상태·감사 기록을 코어에 추가하지 않았다. `engine/runtime`의 계산·저널·출력 파일도 이번 변경에서 수정하지 않았다.

## Rust 문법과 경계

`route_layer`는 등록된 거래 명령의 HTTP 경계에만 적용한다. `move` 클로저는 검증에 필요한 해시 값을 소유해 사용하고, 코어의 상태 참조나 공유 락을 캡처하지 않는다. `Next.run`은 인증 성공 뒤 기존 핸들러로 요청을 전달한다. 이는 TypeScript의 내부 API 인증 미들웨어와 유사하며 매칭 알고리즘의 일부가 아니다.

### 현재 코드로 연결해 읽기

플랫폼 서비스 신원은 엔진 HTTP 경계에서 검사한다. 아래는 2026-10-23 현재 [실제 소스](../../src/routes/mod.rs#L11)의 연속 행 발췌다. 메서드 일부일 수 있으므로 독립 실행 예제로 사용하지 않고 앞뒤 검증과 함께 읽는다. 과거 사이클의 코드/측정 시점은 본문의 기록을 유지한다.

```rust
pub fn create_router(service_token: &str) -> Router<AppState> {
    assert!(service_token.len() >= 32 && service_token.len() <= 1024, "서비스 키는 32~1024바이트여야 합니다");
    let expected = solana_sdk::hash::hash(service_token.as_bytes());
    let commands = create_cex_router().route_layer(middleware::from_fn(move |request: Request, next: Next| async move {
        let provided = request.headers().get("x-engine-service-token").and_then(|value| value.to_str().ok());
        if provided.is_none_or(|key| key.len() > 1024 || solana_sdk::hash::hash(key.as_bytes()) != expected) {
            return Err(StatusCode::UNAUTHORIZED);
        }
        Ok(next.run(request).await)
    }));
    Router::new()
        .route("/api/health", get(health_check))
        .route("/internal/audit/results", get(crate::domains::cex::engine::runtime::audit::page))
        .nest("/api/cex", commands)
}
```

미들웨어의 async closure가 헤더를 검사한 뒤 `next.run`을 호출한다. 이 경계는 플랫폼 내부 호출자의 인증이며 고객 로그인/운영 역할은 플랫폼이 담당한다. 체결 코어에 인증 상태를 공유하지 않는다.

## 작성·수정한 테스트

| 파일·함수 | 검증과 기대 결과 |
|---|---|
| [routes/tests.rs](../../src/routes/tests.rs) `command_boundary_requires_platform_identity` | 키 누락·잘못된 키·고객 JWT·감사 키로 명령이 통과하지 않음. 본문 파싱보다 먼저 401 |
| 같은 파일 `trading_routes_still_validate_request_bodies` | 올바른 서비스 신원 뒤에는 기존 422 입력 검증 유지 |
| 같은 파일 `cancellation_still_requires_user_id` | 올바른 서비스 신원도 사용자·거래쌍 검증을 생략하지 않음 |
| 같은 파일 `health_check_remains_available`, `removed_auth_routes_return_not_found`, `retired_wallet_routes_return_not_found`, `output_failure_closed_handle_reports_unavailable_health` | 공개 헬스·삭제 경로 404·장애 상태 유지 |
| [order_receipt/tests.rs](../../src/domains/cex/services/order_receipt/tests.rs) `http_contract_distinguishes_new_and_existing_receipt` | 새 서비스 키를 사용해 실제 접수 중복 방지·202/200 계약 유지 |
| [verify_durable_acceptance.py](../../scripts/verify_durable_acceptance.py) | 실제 플랫폼 세션·JWT·별도 엔진 키로 응답 유실·동일 원본 재시도·재시작 5개 시나리오 |
| [verify_engine_output.py](../../scripts/verify_engine_output.py) | 새 키로 실제 주문·취소·WAL 복구·Kafka 장애·종료 6개 시나리오 |

## 실행 명령과 결과

- `/Users/dustin/.cargo/bin/cargo test routes::tests --bin cex-engine`: 라우터 7개 통과.
- `python3 scripts/verify_order_safety.py`: 실행 소유 PostgreSQL에서 기존 선택 회귀와 전체 컴파일·바이너리 빌드 통과. 최종 개수는 공통 사이클 기록에 집계한다.
- 실제 프로세스 접수 5개·출력 6개·플랫폼 전체 금융/시세/대사 연결 1개 통과. 기존 PostgreSQL·Kafka에는 접근하지 않았다.

기존 Rust 경고 113개와 Solana/SQLx 향후 비호환 알림은 남아 있다. 지연·처리량 벤치를 새로 측정하지 않았으며 예전 성능 수치로 이번 HTTP 인증 비용을 설명하지 않는다. TLS·서비스별 키·컨테이너 격리·호스트/디스크 손실 복제·백업은 이번 구현에 포함하지 않는다.

## 관련 기록

[플랫폼 접근 통제 결정](../../../Cex-Platform-Service/docs/decisions/012-platform-access-approval-audit.md) · [공통 사이클 결과](../../../docs/roadmap/access-approval-audit.md). 기능·문서 커밋의 실제 해시는 공통 사이클 기록에 남긴다.

- 엔진 기능·회귀: [`b84f1dc`](https://github.com/GiHoon1123/Cex-Engine/commit/b84f1dc37985e0f0a57297451a8139274e165420)
- 플랫폼 기능·회귀: [`892b23f`](https://github.com/GiHoon1123/Cex-Platform-Service/commit/892b23f8e99b100fa8504731870ce9f15b27f9f7)
