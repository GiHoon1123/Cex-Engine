# 엔진 컨테이너의 내부 실행 경계

엔진 이미지를 고정된 Rust 도구 체인으로 재현하고 비특권 사용자로 실행한다. 호스트에는 엔진 포트를 공개하지 않고 플랫폼과 같은 내부 Compose 네트워크에서만 접근하게 한다.

## 문제와 기존 동작

빌더가 `rust:latest`를 사용해 같은 소스도 시점에 따라 다른 컴파일러로 빌드될 수 있었다. 실제 빌드에는 Kafka 네이티브 의존성이 요구하는 `cmake`가 없었고, 실행 이미지에는 Docker 헬스체크가 호출하는 `curl`이 없었다. 프로세스는 기본 root 사용자로 실행됐으며 독립 운영 Compose는 3002를 호스트에 게시했다.

## 기존 제품과 공식 자료의 접근

- [Docker 빌드 모범 사례](https://docs.docker.com/build/building/best-practices/)의 다단계 빌드와 기반 이미지 버전 고정을 사용한다.
- [Dockerfile `USER`](https://docs.docker.com/reference/dockerfile/#user)를 이용해 실행 단계의 기본 권한을 낮춘다.
- [Compose 포트](https://docs.docker.com/compose/how-tos/networking/)의 서비스 이름 기반 내부 통신을 사용하고 엔진 호스트 포트는 게시하지 않는다.

## 선택과 트레이드오프

빌더는 `rust:1.90-bookworm`, 실행은 `debian:bookworm-slim`으로 고정했다. 완전한 이미지 다이제스트 고정은 갱신 자동화를 함께 마련해야 하므로 이번 최소 구현에는 넣지 않았다. 버전 태그는 `latest`보다 재현성이 높지만 레지스트리 태그 변경 가능성까지 없애지는 않는다.

실행 사용자는 UID 10001의 `cex`로 낮췄다. 침해 시 기본 권한을 줄이는 대신 `/app/data`의 소유권을 이미지에서 명시해야 한다. 실시간 우선순위에는 `SYS_NICE`가 여전히 필요하지만 그 권한만 Compose가 추가한다.

## 구현과 관련 코드

- [`Dockerfile`](../../Dockerfile): 빌드 도구·헬스 도구를 실제 단계에 설치하고 비특권 사용자와 쓰기 가능한 WAL 디렉터리를 만든다.
- [`docker-compose.prod.yml`](../../docker-compose.prod.yml): `ports`를 제거하고 내부 `expose`만 유지한다.
- 공통 단일 호스트 배포는 플랫폼 저장소의 `deploy/single-host/compose.yml`이 소유한다. 이 파일은 엔진 독립 실행 경계를 확인하는 용도로 남긴다.

## 이해에 필요한 문법과 원리

실제 실행 단계 일부다.

```dockerfile
RUN useradd --system --uid 10001 --home-dir /nonexistent --shell /usr/sbin/nologin cex \
    && mkdir -p /app/data \
    && chown cex:cex /app/data
USER cex
```

`USER` 뒤의 `CMD`와 헬스체크 대상 프로세스는 기본적으로 `cex` 권한으로 실행된다. `chown`은 명명 볼륨이 처음 만들어질 때 WAL 디렉터리를 쓸 수 있게 한다. 이것은 Rust 소유권과 다른 운영체제 사용자 권한이다.

호스트 공개 대신 다음 경계를 사용한다.

```yaml
expose:
  - "3002"
```

같은 Compose 네트워크의 플랫폼은 `http://engine:3002`로 호출할 수 있지만 호스트 인터페이스에는 포트 매핑이 생기지 않는다.

## 작성한 테스트

이번 변경은 제품 로직 테스트를 새로 추가하지 않고 실제 산출물 회귀로 검증했다.

- Docker 빌드가 Rust 바이너리까지 완료되는지 확인했다.
- 플랫폼 Flyway가 준비한 별도 빈 DB에 엔진 컨테이너를 연결했다.
- `SYS_NICE`와 CPU 고정을 적용한 컨테이너에서 `/api/health` 200 응답을 확인했다.
- 컨테이너 사용자가 `cex`이고 `/app/data`를 쓸 수 있는지 확인했다.

## 실행 명령과 결과

```bash
docker build -t cex-engine:deployment-review .
curl -fsS http://127.0.0.1:13002/api/health
docker inspect -f '{{.Config.User}}' cex-engine-deployment-review
docker exec cex-engine-deployment-review sh -c 'test -w /app/data'
```

이미지 빌드와 실제 엔진 생존 응답, 사용자 `cex`, 데이터 디렉터리 쓰기 검사가 통과했다. 빌드는 `sqlx-core 0.6.3`의 미래 Rust 비호환 경고를 남겼다. 현재 빌드 실패는 아니지만 의존성 갱신 작업에서 제거해야 한다.

## 남은 한계

컨테이너 검증은 로컬 Docker Desktop과 로컬 PostgreSQL/Kafka를 사용했다. EC2 커널의 실시간 스케줄링 허용, 실제 CPU 격리, 디스크 지연, 재시작 정책, 외부 WAL 백업은 운영 호스트에서 별도 검증해야 한다. 명명 볼륨은 호스트 디스크 손실을 보호하지 않는다.

## 관련 커밋과 참고 자료

구현과 문서는 `build(engine): 내부 컨테이너 실행 경계 고정` 커밋에 포함한다. 공식 근거는 위 Docker 문서를 사용했다.
