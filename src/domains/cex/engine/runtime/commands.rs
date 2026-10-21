//! 요청 계층이 단일 체결 워커로 보내는 명령이다. 응답 채널은 저널 워커로
//! 이동하므로 요청자는 영속 확정을 기다리되 체결 코어는 그 완료를 기다리지 않는다.
use crate::domains::cex::engine::types::{OrderEntry, TradingPair};
use anyhow::Result;
use rust_decimal::Decimal;
use tokio::sync::oneshot;

#[derive(Debug)]
pub enum OrderCommand {
    /// 중복 검사·매칭 결과를 저널에 저장한 뒤 접수 확인을 반환한다.
    /// 실행 거부도 저장된 결과이며 접수 확인을 체결 성공으로 해석하지 않는다.
    SubmitOrder {
        order: OrderEntry,
        response: oneshot::Sender<Result<()>>,
    },
    /// 주문 삭제와 예약 해제를 한 번 처리하고, 저널 커밋 뒤 취소 결과를 반환한다.
    CancelOrder {
        order_id: u64,
        user_id: u64,
        trading_pair: TradingPair,
        response: oneshot::Sender<Result<OrderEntry>>,
    },
    /// 조회 결과도 앞선 상태 변경의 영속 확정 이후 공개한다.
    GetOrderbook {
        trading_pair: TradingPair,
        depth: Option<usize>,
        response: oneshot::Sender<Result<(Vec<OrderEntry>, Vec<OrderEntry>)>>,
    },
    GetBalance {
        user_id: u64,
        mint: String,
        response: oneshot::Sender<Result<(Decimal, Decimal)>>,
    },
    /// 단독 예약 명령도 재생 가능한 상태 변경으로 기록한다.
    LockBalance {
        user_id: u64,
        mint: String,
        amount: Decimal,
        response: oneshot::Sender<Result<()>>,
    },
    UnlockBalance {
        user_id: u64,
        mint: String,
        amount: Decimal,
        response: oneshot::Sender<Result<()>>,
    },
}
