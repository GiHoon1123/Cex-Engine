# 엔진 운영 설정의 개발용 폴백 제거

엔진이 운영 환경에서 개발용 데이터베이스 계정을 조립하던 경로를 제거했다. 필수 외부 주소와 서비스 신원이 없으면 거래 워커를 시작하지 않으며, 자체 저널은 컨테이너 수명과 분리된 볼륨에 둔다.

## 문제와 기존 동작

`main.rs`는 `DATABASE_HOST`만 읽고 `root:1234`를 포함한 연결 문자열을 코드에서 만들었다. 설정이 없으면 환경 이름을 추측해 `localhost` 또는 컨테이너 이름을 선택했다. 이 동작은 잘못된 배포가 예상하지 않은 디비에 연결되는 것을 허용하고, 비밀 회전도 막는다. Compose 파일은 필수 토큰을 선언하지 않았고 자체 저널 볼륨도 없었다.

## 기존 제품과 공식 자료의 접근

- Docker Compose는 `${VAR:?오류}` 형식으로 필수 설정이 없을 때 구성을 거부하고, `docker compose config`로 해석 결과를 검증한다.
- Docker는 컨테이너 쓰기 계층과 달리 명명된 볼륨의 수명이 컨테이너 삭제와 분리된다고 설명한다.

참고: [Compose 변수 보간](https://docs.docker.com/compose/how-tos/environment-variables/variable-interpolation), [Docker 볼륨](https://docs.docker.com/engine/storage/volumes/)

## 선택과 트레이드오프

전체 `DATABASE_URL`과 내부 서비스 키, Kafka 주소를 필수값으로 선택했다. 잘못된 환경이 즉시 실패하고 자격 증명을 코드에서 제거하는 대신, 개발자도 실행 전에 값을 명시해야 한다. 로컬 편의용 자격 증명 폴백을 엔진에 남기는 대안은 운영 오접속 위험 때문에 채택하지 않았다.

명명된 볼륨은 컨테이너 재생성으로부터 저널을 보호하지만 호스트·디스크 손실까지 보호하지 않는다. 별도 복제와 백업은 여전히 운영 필수 조건이며 이번 코드 변경의 보장 범위가 아니다.

## 구현과 관련 코드

`src/main.rs`의 실제 발췌다.

```rust
let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL이 필요합니다");
let db = Database::lazy(&database_url).expect("DATABASE_URL 형식이 올바르지 않습니다");
```

`var`는 값이 없으면 `Err`를 반환한다. `expect`가 엔진 생성보다 먼저 실행되므로 잘못된 기본 계정으로 저널이나 워커를 만들 수 없다. `lazy`는 주소 형식만 검사하고 실제 연결은 주문 접수 어댑터가 사용할 때 연다. 따라서 자체 저널 재생은 플랫폼 디비의 일시 장애와 분리된다.

`docker-compose.yml`과 `docker-compose.prod.yml`은 필수 변수 검사, 내부 수신 주소, `/app/data` 명명 볼륨, CPU 0 고정을 선언한다. 사용하지 않던 체결 조회 저장소와 빈 공통 유틸 모듈도 삭제했다.

## 이해에 필요한 문법과 원리

Rust의 `Result::expect`는 성공 값이면 내부 값을 꺼내고 실패면 지정한 메시지와 함께 즉시 종료한다. 타입스크립트에서 시작 시 환경 변수를 검사하고 예외를 던지는 것과 목적은 같지만, 이후 코드는 `String` 값을 확정된 상태로 받는다.

Compose의 `${NAME:?메시지}`는 셸 보간 단계에서 비어 있거나 누락된 값을 거부한다. 애플리케이션 내부 검사와 Compose 검사를 함께 두어 컨테이너 외 직접 실행과 Compose 실행을 모두 막는다.

## 작성한 테스트

- `tests/service_startup.rs::missing_database_url_fails_without_using_a_development_credential`: 모든 환경을 비운 실제 바이너리가 `DATABASE_URL` 누락으로 실패하고 개발 비밀번호나 저널을 남기지 않는지 검사한다.
- `tests/service_startup.rs::invalid_audit_identity_fails_before_journal_creation`: 잘못된 감사 키가 계속 기동되지 않는 기존 경계를 함께 회귀 검증한다.
- 두 Compose 파일은 필수값을 격리 값으로 넣은 `docker compose config --quiet`로 스키마와 보간을 검사한다.

## 실행 명령과 결과

```text
cargo test --test service_startup
2 passed, 0 failed, 0 ignored

docker compose -f docker-compose.yml config --quiet
성공

docker compose -f docker-compose.prod.yml config --quiet
성공
```

```text
python3 scripts/verify_order_safety.py
19개 검사 묶음, 252회 실행, 고유 테스트 214건
실패 0, 무시 0, 격리 PostgreSQL 컨테이너 회수 성공
```

SQLx 0.6의 미래 호환 경고는 기존 의존성 감사 범위에 남아 있으며 이번 설정 변경이 해결한 것으로 주장하지 않는다.

## 관련 커밋과 참고 자료

관련 커밋은 이 문서와 구현을 같은 작업 단위로 기록한다.
