# 엔진 기술 문서

엔진의 설계 결정과 개선 근거를 기록한다. 각 글에는 관련 코드, 선택의 트레이드오프, 작성한 테스트와 검증 결과를 함께 남긴다.

## 설계 결정

- [엔진에서 사용자 인증 제거하기](decisions/001-remove-engine-auth.md): 인증 책임을 Java 플랫폼으로 모으고, 비활성 코드와 의존성을 제거한 이유 및 HTTP 회귀 테스트.
- [복구 근거가 없는 주문·예약금의 시작 차단](decisions/002-block-incomplete-startup-state.md): 초기 DB 스냅샷 검증, 임의 예약 해제 제거, PostgreSQL 테스트와 실제 시작 거부 확인.
- [실행 전 영속 접수·같은 주문의 중복 전달 차단](decisions/003-durable-order-acceptance.md): 플랫폼 원본 연결, 접수 증명, 예약 중복 반영 제거와 실제 두 프로세스 검증.
