# Cex-Engine

## 기술 문서

- [설계 결정과 개선 기록](docs/README.md)

## 시작 조건

미종결 주문·예약금이 있으면 현재 엔진은 자동 복원 없이 거래를 재개하지 않고 시작을 거부한다. 시작 조건과 검증 범위는 [초기 상태 검증 기록](docs/decisions/002-block-incomplete-startup-state.md)에서 확인한다.

영속 접수 기록이 있어도 현재는 재생 없이 시작을 차단한다. 신규 접수 스키마 준비와 202/200 응답 의미는 [영속 접수 기록](docs/decisions/003-durable-order-acceptance.md)을 따른다. 기본 HTTP 주소는 `127.0.0.1:3002`, 명시적 바인딩은 `ENGINE_HTTP_ADDRESS`로 지정한다.
