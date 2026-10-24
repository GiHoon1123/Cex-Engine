# 비활성 솔라나 지갑·스왑을 엔진 실행 경로에서 제거하기

사용하지 않는 수탁·외부 스왑 모듈과 SDK를 삭제하고 거래 엔진의 역할을 명확히 했다. 기존 저널의 SHA-256 원본 32바이트 형식과 플랫폼 주문 접수 검증을 유지한다. 공통 작업 사이클 068이며 엔진 결정 기록은 017번이다.

## 문제와 기존 동작

사용자 지갑 HTTP 경로는 인증 도메인 제거 때 이미 끊었지만 `AppState`는 지갑·스왑 서비스를 계속 생성했다. Solana/Jupiter 클라이언트와 저장소도 컴파일 대상이었다. 현재 플랫폼에는 실제 블록체인 수탁·입출금 모듈이 없으므로 이 코드의 존재를 BTC 입출금 구현으로 오인하면 안 된다.

과거 지갑 코드가 비밀 키를 Base64로 표현했던 것은 암호화가 아니다. 이번 작업은 참조 없는 소스와 초기화 제거이며 DB·마이그레이션·기존 금융/지갑 기록을 읽거나 삭제하지 않았다. 기존 보관 데이터·키의 보존과 처리 정책은 별도 수탁·개인정보 검토가 필요하다.

## 기존 구현과 공식 자료의 접근

[sha2 0.10.9 공식 문서](https://docs.rs/sha2/0.10.9/sha2/)의 일괄 SHA-256 API와 알려진 벡터를 확인했다. 설치된 `solana-program 1.18.26/src/hash.rs`도 `Sha256`의 원본 바이트를 반환한다. [constant_time_eq 공식 구현](https://github.com/cesarb/constant_time_eq)과 설치된 0.4.2의 `[u8;32]` API를 확인해 내부 서비스 키 비교에 사용했다. 암호 알고리즘이나 고정 길이 비교를 직접 구현하지 않는다.

## 선택과 트레이드오프

| 대안 | 이점 | 비용/제약 | 선택 |
| --- | --- | --- | --- |
| 비활성 지갑과 SDK 보관 | 향후 소스 재탐색 감소 | 엔진 책임 혼재·불필요한 빌드/의존성·불안전한 과거 예제 존속 | 제거 |
| SDK 해시만 사용 | 호출 코드 유지 | 체인 클라이언트 묶음을 저널 검사 때문에 유지 | 제거 |
| 설치된 표준 해시·고정 길이 비교 직접 사용 | 작고 명확한 의존성·기존 기록 바이트 유지 | 직접 버전/보안 공지 관리 필요 | 채택 |

잠금 패키지는 **656개→328개**, 새 이름/버전 쌍은 **0개**, 제거한 쌍은 **328개**다. 전이 그래프의 동일 패키지 표시가 간단해지면서 잠금 파일 행도 크게 줄었다. 성능 향상이나 취약점 해소 수치로 해석하지 않는다. `reqwest` 등 전이 의존성 전체가 사라졌다는 뜻도 아니다. 새 OSV 전체 조회는 하지 않았다.

## 구현과 관련 코드

- `wallet`, `swap`, 외부 클라이언트, 지갑 저장소와 전용 오류 모듈을 삭제했다. 사용하지 않는 디렉터리를 호환용으로 남기지 않았다.
- `AppState`는 CEX 서비스와 엔진 명령 핸들만 조립한다. 실패할 외부 서비스 생성이 없어 `Result`/`?`도 제거했다.
- HTTP 명령/감사 키는 같은 작은 도구로 SHA-256 후 고정 길이를 비교한다. 감사 키도 32~1024바이트로 제한한다. 키가 없으면 기존처럼 감사 조회가 비활성화된다. 서로 다른 키를 배포할 책임은 유지하며 두 설정의 동일 값 금지를 새로 구현한 것은 아니다.
- 잘못된 감사 키는 거래 워커 시작 전 거부한다. DB lazy 풀 객체 생성보다 먼저 검증한다는 뜻은 아니다.
- **PostgreSQL은 유지한다.** [접수 검증](../../src/domains/cex/services/order_receipt.rs)은 코어에 명령을 넣기 전에 플랫폼의 주문 원본·수수료·예약을 확인한다. 체결 코어의 외부 I/O와 별개다. 재시작 복구는 자체 SQLite 저널 기준이다.
- 오더북·실행 잔액·매칭 루프·출력 큐 계약은 변경하지 않았다. 저널 버전/스키마·기록 형식도 변경하지 않았다.

## 이해에 필요한 문법과 실제 코드

부분 발췌이며 독립 실행 예제가 아니다.

출처: [app_state.rs](../../src/shared/services/app_state.rs#L16)

```rust
    pub fn new(db: Database, engine: EngineHandle) -> Self {
        Self {
            cex_state: CexState::new(db, engine.clone()),
            engine,
        }
    }
```

`engine.clone()`은 명령 핸들을 복제한다. 거래 상태를 복사하거나 공유 락으로 공개하지 않는다. 생성자가 `Self`를 바로 반환하는 것은 이 단계가 서비스 조립만 수행하기 때문이다.

출처: [store.rs](../../src/domains/cex/engine/runtime/journal/store.rs#L63)

```rust
fn digest(payload: &str) -> Vec<u8> {
    // 기존 SDK도 같은 SHA-256을 사용했다. 기존 저널과 동일한 원문 바이트/32바이트 BLOB을 유지한다.
    // 변조 방지 서명이 아니라 저장 손상을 재생 전에 검출하는 검사 합이다.
    Sha256::digest(payload.as_bytes()).to_vec()
}
```

`Digest` 트레이트는 `Sha256::digest`를 사용할 수 있게 한다. 문자열의 UTF-8 원본 바이트를 검사하며 결과는 32바이트다. `.to_vec()`는 기존 SQLite BLOB 저장 형태를 유지한다. Base58/16진 문자열로 바꾸는 마이그레이션을 추가하지 않는다. 검사 합은 손상 탐지용이며 관리자 변조 방지 서명이 아니다.

출처: [service_identity.rs](../../src/shared/service_identity.rs#L17)

```rust
    pub(crate) fn matches(&self, provided: &str) -> bool {
        // 길이는 호출자가 이미 아는 값이다. 비정상 크기는 해시 계산 전에 제한한다.
        if !(32..=1024).contains(&provided.len()) {
            return false;
        }
        let candidate: [u8; 32] = Sha256::digest(provided.as_bytes()).into();
        constant_time_eq::constant_time_eq_32(&self.0, &candidate)
    }
```

`[u8; 32]`는 길이가 타입에 포함된 배열이다. `constant_time_eq_32`에 같은 길이만 전달한다. `Copy`는 작은 검증값을 HTTP 클로저에 값으로 전달하게 하며 원문 키를 저장하지 않는다. 입력 길이 검사는 해시 전에 수행한다. 기능 테스트는 비교 결과를 확인하며 하드웨어 수준 타이밍 분석을 수행한 것은 아니다.

## 작성한 테스트

| 파일/함수 | 시나리오·기대 결과 |
| --- | --- |
| [service_identity.rs](../../src/shared/service_identity.rs) `service_key_limits_are_checked_before_use` | 0/31/1025바이트 거부, 32/1024바이트 수용 |
| 같은 파일 `service_identity_rejects_prefix_suffix_and_changed_byte` | 정확한 키만 수용, 접두/접미/변경/과대 키 거부 |
| 같은 파일 `service_and_audit_keys_are_independent` | 서로 다른 명령/감사 키가 상대를 통과하지 않음 |
| [store.rs](../../src/domains/cex/engine/runtime/journal/store.rs) `journal_sha256_binary_checksum_is_unchanged` | 알려진 벡터와 원본 32바이트 일치 |
| 같은 파일 `journal_checksum_rejects_changed_payload_before_decode` | 유효 JSON이어도 본문 변경 시 검사 합 거부 |
| [routes/tests.rs](../../src/routes/tests.rs) `retired_swap_routes_return_not_found` | 견적/스왑 거래/토큰 검색 3개 경로 404 |
| [service_startup.rs](../../tests/service_startup.rs) `invalid_audit_identity_fails_before_journal_creation` | 짧은 키/과대 키로 실제 프로세스 종료, 저널 미생성, 키 비노출; 제한 시간 후 소유한 자식만 종료 |

기존 `retired_wallet_routes_return_not_found`, 주문/취소 인증 경계, 실제 워커 소유권·포화·종료, 금액·정산·영속 접수·저널 재생 회귀도 재실행했다. 신규 테스트 함수 **7개**다.

## 실행 명령과 결과

2026-10-25 로컬 Rust/격리 PostgreSQL16. 플랫폼 연결 검증은 JDK21과 실행 소유 Kafka를 썼다. 기존 CEX·다른 프로젝트 DB/브로커를 사용하지 않았고 봇 상시 기동은 0명이다.

```sh
/Users/dustin/.cargo/bin/cargo check --offline
python3 scripts/verify_order_safety.py
# 플랫폼 저장소에서 실행
python3 scripts/verify_market_path.py
```

- `cargo check`: 성공. 처음 `cargo`가 PATH에 없어 종료127인 시도는 컴파일/테스트 실행으로 집계하지 않았다.
- 선택 Rust **139개 고유 테스트, 필터 중복 포함143회 통과, 실패0/무시0**. 전체 테스트 컴파일·제품 빌드 성공. 공유 DB를 쓰는 과거 전체 통합 테스트를 무조건 실행한 결과가 아니다.
- 실제 엔진·플랫폼·PostgreSQL·Kafka·공개 WebSocket 경로 `MarketPathTest` **1개 통과**. 테스트 소유 자원 회수 완료. 단위 테스트의 인증 검사가 실제 브로커 전달까지 검증한 것으로 바꿔 적지 않는다.
- 기존 미사용 코드 경고가 남으며 바이너리 빌드57개(8개 중복)와 `sqlx-core 0.6.3` 향후 Rust 비호환 안내가 있다. 경고 없는 빌드나 전체 보안 검토 완료가 아니다.
- 처리량/지연 벤치, 실제 블록체인 입출금, 호스트/디스크 손실 복제·백업, 운영 배포는 이번 범위가 아니다.

증적: [검증 결과](../reference/retired-chain-removal-verification-2026-10-25.json). 로컬 상세 로그는 공통 `.local/cycle68-proof/`에 둔다. 과거 문서001/009/010/016과 인프라 문서의 현재 발췌·설명을 갱신하고 과거 테스트 결과는 보존했다.

## 관련 커밋과 다음 경계

- 구현·테스트: `e6b56f5e5dab2c4984b3623a7795dfe81ea553ef`.
- 실제 자산·체인별 주소·격리 서명·입금 확정/재조직·출금 결과 불명·온체인 대사는 아직 후속 범위다. 비활성 코드 삭제를 수탁 기능 구현으로 보고하지 않는다.
