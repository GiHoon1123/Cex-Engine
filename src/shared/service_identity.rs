//! 플랫폼 명령 키와 독립 조회 키를 각 HTTP 경계에서 확인한다.
//! 해시는 표준 구현을 쓰고 고정 길이 비교는 검증된 도구에 맡긴다. 체결 상태에는 접근하지 않는다.
use anyhow::{ensure, Result};
use sha2::{Digest, Sha256};

/// 원문 키를 복제하지 않고 검증용 32바이트만 보관한다. 로그 출력을 위한 Debug는 구현하지 않는다.
#[derive(Clone, Copy)]
pub(crate) struct ServiceToken([u8; 32]);
impl ServiceToken {
    pub(crate) fn parse(value: &str) -> Result<Self> {
        ensure!(
            (32..=1024).contains(&value.len()),
            "내부 서비스 키는 32~1024바이트여야 합니다"
        );
        Ok(Self(Sha256::digest(value.as_bytes()).into()))
    }
    pub(crate) fn matches(&self, provided: &str) -> bool {
        // 길이는 호출자가 이미 아는 값이다. 비정상 크기는 해시 계산 전에 제한한다.
        if !(32..=1024).contains(&provided.len()) {
            return false;
        }
        let candidate: [u8; 32] = Sha256::digest(provided.as_bytes()).into();
        constant_time_eq::constant_time_eq_32(&self.0, &candidate)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn service_key_limits_are_checked_before_use() {
        for size in [0, 31, 1025] {
            assert!(ServiceToken::parse(&"x".repeat(size)).is_err());
        }
        for size in [32, 1024] {
            let key = "x".repeat(size);
            assert!(ServiceToken::parse(&key).unwrap().matches(&key));
        }
    }
    #[test]
    fn service_identity_rejects_prefix_suffix_and_changed_byte() {
        let original = "isolated-platform-service-token-32-bytes";
        let key = ServiceToken::parse(original).unwrap();
        assert!(key.matches(original));
        for candidate in [
            format!("{original}x"),
            original[..32].into(),
            format!("x{}", &original[1..]),
            format!("{}x", &original[..original.len() - 1]),
            "x".repeat(1025),
        ] {
            assert!(!key.matches(&candidate));
        }
    }
    #[test]
    fn service_and_audit_keys_are_independent() {
        let commands = "isolated-platform-command-token-32-bytes";
        let audit = "isolated-independent-audit-token-32-bytes";
        assert!(!ServiceToken::parse(commands).unwrap().matches(audit));
        assert!(!ServiceToken::parse(audit).unwrap().matches(commands));
    }
}
