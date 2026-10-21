//! 체결 결과의 두 자산을 실행용 캐시에 함께 반영한다.
//!
//! 캐시 검증 오류는 잔액을 바꾸지 않는다. 성공 후의 기존 DB 명령 발행은 별도 경계이며
//! 이 모듈이 DB·이벤트·주문장까지 원자적으로 확정하거나 WAL 복구를 제공하지는 않는다.

use rust_decimal::Decimal;
use anyhow::{Result, Context as AnyhowContext};
use chrono::Utc;
use crossbeam::channel::Sender;
use crate::domains::cex::engine::types::MatchResult;
use crate::domains::cex::engine::balance_cache::BalanceCache;
use crate::domains::cex::engine::wal::WalEntry;
use crate::domains::cex::engine::runtime::db_commands::DbCommand;

/// 체결 실행 결과
/// Executor가 처리한 결과를 담는 구조체
#[derive(Debug, Clone)]
pub struct ExecutionResult {
    /// 체결 정보
    pub match_result: MatchResult,
    /// 체결 성공 여부
    pub success: bool,
    /// 에러 메시지 (실패 시)
    pub error: Option<String>,
}

/// 체결 실행 엔진
/// 
/// 구성 요소:
/// - balance_cache: 메모리 잔고 관리
/// - wal_sender: WAL 메시지 발행 채널
/// 
/// 메시지 발행 방식:
/// - Executor는 WAL에 직접 쓰지 않음
/// - Channel(Sender)로 WalEntry 메시지만 발행
/// - 실제 디스크 쓰기는 WAL Thread가 처리
/// 
/// 성능 이점:
/// - 디스크 I/O 대기 없음 (Non-blocking)
/// - Channel send: ~100ns (메모리 연산)
/// - Disk write: ~0.1ms (WAL Thread에서)
/// - TPS 100배 향상 (10,000 → 100,000+)
pub struct Executor {
    /// 메모리 잔고 캐시
    balance_cache: BalanceCache,
    /// WAL 메시지 발행 채널 (Option으로 감싸서 테스트 시 None 가능)
    wal_sender: Option<Sender<WalEntry>>,
    /// DB Writer 채널 (Option으로 감싸서 테스트 시 None 가능)
    db_sender: Option<Sender<DbCommand>>,
}

impl Executor {
    /// 새 Executor 생성 (WAL Sender 포함)
    /// 
    /// # Arguments
    /// * `wal_sender` - WAL 메시지 전송 채널
    /// 
    /// # Example
    /// ```ignore
    /// let (wal_tx, wal_rx) = crossbeam::channel::bounded(10000);
    /// let executor = Executor::new(Some(wal_tx), Some(db_tx));
    /// 
    /// // WAL Thread에서 wal_rx로 메시지 수신 & 처리
    /// ```
    pub fn new(
        wal_sender: Option<Sender<WalEntry>>,
        db_sender: Option<Sender<DbCommand>>,
    ) -> Self {
        Self {
            balance_cache: BalanceCache::new(),
            wal_sender,
            db_sender,
        }
    }
    
    /// WAL 없이 생성 (테스트용)
    /// 
    /// # Note
    /// 테스트에서는 WAL 메시지 발행을 생략
    pub fn new_without_wal() -> Self {
        Self::new(None, None)
    }
    
    /// 채널 Sender 해제 (엔진 종료 시 호출)
    /// 
    /// 모든 Sender를 drop하여 채널을 닫고 스레드 루프를 종료시킵니다.
    pub fn clear_channels(&mut self) {
        self.wal_sender = None;
        self.db_sender = None;
    }
    
    /// 한 체결의 캐시 반영. 검증 실패 시 잔액·새 계정·DB 명령이 모두 그대로다.
    /// skip_db_updates=true인 시장가 경로의 DB 집계는 기존 호출자가 담당한다.
    /// false이면 성공 뒤 기존 체결 명령을 보낸다. 이 발행과 캐시는 한 트랜잭션이 아니다.
    pub fn execute_trade(&mut self, match_result: &MatchResult, skip_db_updates: bool) -> Result<ExecutionResult> {
        self.balance_cache.settle_trade(match_result)?;
        if !skip_db_updates {
            if let Some(sender) = &self.db_sender {
                let command = DbCommand::InsertTrade {
                    buy_order_id: match_result.buy_order_id, sell_order_id: match_result.sell_order_id,
                    buyer_id: match_result.buyer_id, seller_id: match_result.seller_id,
                    price: match_result.price, amount: match_result.amount,
                    base_mint: match_result.base_mint.clone(), quote_mint: match_result.quote_mint.clone(),
                    timestamp: Utc::now(),
                };
                // 기존 발행 동작을 유지한다. 채널 전송은 DB 커밋 확인이 아니다.
                // 여기서 이미 반영한 캐시를 부분 롤백하거나 새 DB 성공 보장을 만들지 않는다.
                if let Err(error) = sender.send(command) {
                    eprintln!("[Executor] 캐시 반영 후 체결 DB 명령 전달 실패: {error}");
                }
            }
        }
        Ok(ExecutionResult { match_result: match_result.clone(), success: true, error: None })
    }

    /// 주문 생성 시 잔고 잠금
    /// 
    /// # Arguments
    /// * `order_id` - 주문 ID
    /// * `user_id` - 사용자 ID
    /// * `mint` - 자산 주소
    /// * `amount` - 잠글 금액
    /// 
    /// # Returns
    /// 성공 또는 에러
    /// 
    /// # Process
    /// 1. WAL 메시지 발행 (BalanceLocked)
    /// 2. 메모리 잔고 잠금 (available → locked)
    pub fn lock_balance_for_order(
        &mut self,
        order_id: u64,
        user_id: u64,
        mint: &str,
        amount: Decimal,
    ) -> Result<()> {
        // WAL 메시지 발행 (먼저!) - 주석 처리됨 (WAL 비활성화)
        // WAL 로그 쓰기 비활성화 (디스크 공간 부족 문제로 인해 임시 비활성화)
        /*
        if let Some(sender) = &self.wal_sender {
            sender.send(WalEntry::BalanceLocked {
                user_id,
                mint: mint.to_string(),
                amount: amount.to_string(),
                timestamp: Utc::now().timestamp_millis(),
            })?;
        }
        */
        
        // 잔고 잠금
        self.balance_cache.lock_balance(user_id, mint, amount)
            .context("Failed to lock balance")?;
        
        Ok(())
    }
    
    /// 주문 취소 시 잔고 잠금 해제
    /// 
    /// # Arguments
    /// * `order_id` - 주문 ID
    /// * `user_id` - 사용자 ID
    /// * `mint` - 자산 주소
    /// * `amount` - 해제할 금액
    /// 
    /// # Process
    /// 1. WAL 메시지 발행 (OrderCancelled)
    /// 2. 메모리 잔고 잠금 해제 (locked → available)
    pub fn unlock_balance_for_cancel(
        &mut self,
        order_id: u64,
        user_id: u64,
        mint: &str,
        amount: Decimal,
    ) -> Result<()> {
        // WAL 메시지 발행 - 주석 처리됨 (WAL 비활성화)
        // WAL 로그 쓰기 비활성화 (디스크 공간 부족 문제로 인해 임시 비활성화)
        /*
        if let Some(sender) = &self.wal_sender {
            sender.send(WalEntry::OrderCancelled {
                order_id,
                user_id,
                timestamp: Utc::now().timestamp_millis(),
            })?;
        }
        */
        
        // 잔고 잠금 해제
        self.balance_cache.unlock_balance(user_id, mint, amount)
            .context("Failed to unlock balance")?;
        
        Ok(())
    }
    
    /// 잔고 캐시 참조 (읽기 전용)
    pub fn balance_cache(&self) -> &BalanceCache {
        &self.balance_cache
    }
    
    /// 잔고 캐시 참조 (가변)
    pub fn balance_cache_mut(&mut self) -> &mut BalanceCache {
        &mut self.balance_cache
    }
}
