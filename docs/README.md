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
- [체결 결과를 하나의 출력 워커로 분리하기](decisions/008-ordered-output-worker.md): 체결별 스레드·핵심 경로 I/O 제거, 출력 포화·오류·종료 처리와 실제 Kafka 검증.

- [WAL 확정 후 공개와 자체 저널 복구](decisions/009-durable-journal-recovery.md): 기록·Kafka 워커 분리, 커밋 후 응답·발행, 전체 명령 재생과 강제 종료 검증.

- [엔진의 비활성 봇 제거](decisions/010-remove-engine-bot.md): 플랫폼으로 봇 책임을 모으고 남은 파일·직접 의존성을 제거한 근거와 회귀 검사.

- [WAL 기반 자체 호가·공개 체결 발행](decisions/011-durable-market-output.md): 코어 비용 분리, 시장 토픽·순번·재시작 기준점과 취소의 자체 상태 검증.

- [주문 당시 적용률과 수취 자산 수수료](decisions/012-received-asset-fees.md): 코어 차감·WAL 보존·변화량 발행·복구·비영 수수료 검증.

- [시장가 IOC 종료와 미사용 예약 반환](decisions/013-market-ioc-completion.md): 체결 0건·부분·전량 결과, 영속 종료 이벤트, 순서·재생 검증.

- [전체 주문 거절의 영속 결과와 예약 반환 근거](decisions/014-durable-order-rejection.md): 원본 검증·재시작·중복 처리·실제 장애 주입 검증.

- [코어와 분리한 읽기 전용 저널 감사 조회](decisions/015-read-only-journal-audit.md): 확정 근거·불변 기록·오류 탐지·실제 프로세스 검증.

- [플랫폼 신원으로 거래 명령 보호](decisions/016-platform-service-boundary.md): API 전용 인증·서비스 경계·실제 검증·남은 운영 요건.

- [비활성 솔라나 지갑·스왑과 SDK 제거](decisions/017-remove-retired-chain-integrations.md): 표준 검사 합 유지·서비스 키 검증·거래/복구/플랫폼 연동 회귀.

- [입금 반입의 멱등 실행과 자체 복원](decisions/018-idempotent-deposit-credit.md): 원본 내용 결합·실패 보존·확정 후 출력·실제 워커 회귀.

- [미사용 잔액 증감 HTTP 제거](decisions/019-remove-retired-balance-http.md): 중복 가능한 외부 경로 삭제·기존 금융 저널/거래 보존·404 회귀.

## 2026-10-23 실제 코드 발췌 보강

기존 결정 문서 16개의 문법/원리 설명에 현재 소스의 실제 발췌·출처 위치·처리 순서 설명을 추가했다. 코드 내용은 유지하고 공통 들여쓰기/행 끝 공백만 정리했다. 과거 구현·검증 수치를 현재 소스와 혼동하지 않도록 기준일과 부분 발췌임을 표시했다. [소스 대조 기록](reference/code-examples-verification-2026-10-23.json)을 함께 읽는다. 이 보강은 문서만 변경했으며 제품 테스트를 새로 실행한 결과가 아니다.

## 실제 코드 예시를 찾는 방법

[전체 문서 적용 범위와 소스 대조 결과](../../docs/reviews/2026-10-23-document-code-excerpts.md)에서 이 문서와 연결된 구현 설명·실제 코드·문법·작성한 테스트를 확인한다. 목차·작업 규칙·Git 이력에는 같은 제품 코드를 복사하지 않고 관련 구현 기록을 연결한다. 계획은 구현 결과와, 문서 검증은 제품 테스트와 구분한다.

- [입금 미접수·영속 거절·결과 불명 구분](decisions/020-typed-deposit-outcomes.md): 명시적 오류 타입·시간 초과 뒤 커밋/재시도·시험 집계 정정.

- [입금 원본의 내부 HTTP와 영속 결과](decisions/021-internal-deposit-credit-http.md): 문자열 정확 금액·서비스 신원·확정/미접수/결과 불명·실제 저장 차단/실패/재시작.

- [현재 금융 이력과 최초 기동](decisions/022-first-bootstrap-current-history.md): 새 경로 초기화 차단·동일 읽기 스냅샷·DB 없는 원래 WAL 복구.

- [출금별 예약·해제·완료와 최초 금융 거절](decisions/023-withdrawal-reservations.md): 단일 소유/정확 금액·자체 WAL·고유 금융 결과·강제 복원. 플랫폼/실제 체인 연결은 후속.
