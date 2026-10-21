//! 엔진 워커 시작 시 한 번만 요청하는 실행 정책이다.
//! 현재 사용하지 않는 저장 워커의 코어 설정과 환경별 분기를 두지 않는다.

/// 기존 엔진의 코어 0 고정과 실시간 우선순위 요청을 유지한다.
/// 리눅스에서만 적용하며, 실패 시 기존과 같이 경고를 남긴다.
/// 이 요청만으로 운영체제 차원의 코어 독점이나 성능을 보장하지는 않는다.
pub(super) fn configure_engine_thread() {
    #[cfg(target_os = "linux")]
    {
        if !core_affinity::set_for_current(core_affinity::CoreId { id: 0 }) {
            eprintln!("엔진 코어 고정 요청에 실패했다");
        }
        let parameters = libc::sched_param { sched_priority: 99 };
        // 운영체제 함수를 호출하는 구간이다. 매개변수는 호출이 끝날 때까지
        // 유효한 지역 변수이며 반환 코드를 확인한다. 권한 부여는 별도다.
        let result = unsafe { libc::sched_setscheduler(0, libc::SCHED_FIFO, &parameters) };
        if result != 0 {
            eprintln!(
                "엔진 실시간 우선순위 요청에 실패했다: {}",
                std::io::Error::last_os_error()
            );
        }
    }
}
