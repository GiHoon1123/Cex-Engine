// =====================================================
// Runtime - 통합 엔진 런타임
// =====================================================
// 역할: 고성능 체결 엔진의 실행 시점 관리
//
// 구조:
// - engine.rs: HighPerformanceEngine 구조체
// - commands.rs: OrderCommand enum
// - config.rs: 엔진 스레드 시작 시 실행 정책 요청
// - threads.rs: 스레드 루프 함수들
// =====================================================

mod config;
pub mod commands;
pub mod balance_commands;
pub mod engine;
pub mod handle;
mod threads;
mod output;
mod startup;
mod submission;
#[cfg(any(test, feature = "bench_mode"))]
pub mod bench_engine;

pub use engine::HighPerformanceEngine;
pub use handle::EngineHandle;
pub use commands::OrderCommand;
pub use balance_commands::BalanceCommand;

mod journal;

pub(crate) mod market;

pub(crate) use submission::MarketCompletion;
