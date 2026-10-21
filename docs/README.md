# 엔진 기술 문서

엔진의 설계 결정과 개선 근거를 기록한다. 각 글에는 관련 코드, 선택의 트레이드오프, 작성한 테스트와 검증 결과를 함께 남긴다.

## 설계 결정

- [엔진에서 사용자 인증 제거하기](decisions/001-remove-engine-auth.md): 인증 책임을 Java 플랫폼으로 모으고, 비활성 코드와 의존성을 제거한 이유 및 HTTP 회귀 테스트.
- [복구 근거가 없는 주문·예약금의 시작 차단](decisions/002-block-incomplete-startup-state.md): 초기 DB 스냅샷 검증, 임의 예약 해제 제거, PostgreSQL 테스트와 실제 시작 거부 확인.
- [실행 전 영속 접수·같은 주문의 중복 전달 차단](decisions/003-durable-order-acceptance.md): 플랫폼 원본 연결, 접수 증명, 예약 중복 반영 제거와 실제 두 프로세스 검증.
- [한 체결의 두 자산을 메모리에 함께 반영하기](decisions/004-atomic-trade-cache-settlement.md): 부분 이체 제거, 정확한 금액 계산, 정산 실패 후 성공 처리 중단과 테스트.
- [금액 타입 경계 재검토와 정확한 해독](decisions/005-exact-amount-boundaries-review.md): 입력·저장·조회 경계 오류 재현, 수정과 회귀 검증.
- [한 신규 주문의 매칭과 잔액을 메모리에 함께 확정하기](decisions/006-atomic-order-memory-commit.md): 후보 매칭, 전체 정산·반환 검증, 주문장·캐시 확정과 실패 회귀.
- [엔진 상태를 단일 워커가 독점 소유하도록 바꾸기](decisions/007-single-owner-engine-state.md): 주문장·실행 잔액의 공유 락 제거, 요청 핸들 분리, 실제 워커와 큐 포화 회귀.
