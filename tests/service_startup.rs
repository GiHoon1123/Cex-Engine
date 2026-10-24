//! 잘못된 내부 감사 키를 가진 실행 파일은 거래 워커를 시작하지 않아야 한다.
//! 실제 바이너리를 쓰되 외부 주소와 데이터 디렉터리는 이 검사 전용으로 격리한다.
use std::{
    fs::File,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

#[test]
fn invalid_audit_identity_fails_before_journal_creation() {
    for invalid_key in ["short".to_owned(), "x".repeat(1025)] {
        let directory = tempfile::tempdir().unwrap();
        let error_path = directory.path().join("startup-error.log");
        let mut child = Command::new(env!("CARGO_BIN_EXE_cex-engine"))
            .env(
                "ENGINE_SERVICE_TOKEN",
                "isolated-startup-service-token-32-bytes",
            )
            .env("ENGINE_AUDIT_TOKEN", &invalid_key)
            .env("ENGINE_DATA_DIR", directory.path().join("journal"))
            .env("DATABASE_HOST", "127.0.0.1:1")
            .env("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:1")
            .env("ENGINE_HTTP_ADDRESS", "127.0.0.1:0")
            .stdout(Stdio::null())
            .stderr(File::create(&error_path).unwrap())
            .spawn()
            .unwrap();
        // 회귀로 기동을 계속해도 무한히 기다리지 않고 이 검사가 소유한 자식만 종료한다.
        let deadline = Instant::now() + Duration::from_secs(10);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("잘못된 감사 키를 가진 프로세스가 종료되지 않았습니다");
            }
            thread::sleep(Duration::from_millis(20));
        };
        assert!(!status.success());
        let error = std::fs::read_to_string(error_path).unwrap();
        assert!(error.contains("잘못된 감사 조회 설정"));
        assert!(!error.contains(&invalid_key));
        // 생성자는 경로만 기억하고 영속 파일은 거래 워커 시작 때 만든다.
        // 키 검증 실패로 이 지점에 도달하지 않았음을 디렉터리 부재로 확인한다.
        assert!(!directory.path().join("journal").exists());
    }
}
