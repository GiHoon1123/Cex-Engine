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
