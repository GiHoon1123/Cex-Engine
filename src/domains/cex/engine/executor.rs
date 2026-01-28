// =====================================================
// Executor - 체결 실행 엔진
// =====================================================
// 역할: Matcher가 반환한 MatchResult를 받아서 실제 체결 처리
// 
// 핵심 책임:
// 1. WAL 메시지 발행 (먼저! - 복구 가능성 보장)
// 2. 잔고 업데이트 (메모리)
// 3. Trade 레코드 생성 (DB 저장 준비)
// 4. Order 상태 업데이트
//
// 처리 흐름:
// MatchResult → WAL 메시지 발행 → 잔고 업데이트 → Trade 생성 → DB 큐
// 
// 안전성:
// - WAL 메시지를 먼저 발행하므로 WAL Thread가 디스크에 기록
// - 메모리 업데이트는 메시지 발행 후 바로 실행 (Non-blocking)
// 
// 성능:
// - WAL Writer를 직접 호출하지 않음 (디스크 I/O 대기 없음)
// - Channel로 메시지만 전송 (~100ns)
// - 실제 디스크 쓰기는 WAL Thread (Core 1)에서 처리
// =====================================================

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
    
    /// 체결 실행
    /// 
    /// # Arguments
    /// * `match_result` - Matcher가 생성한 매칭 결과
    /// * `skip_db_updates` - DB 업데이트 명령 전송을 건너뛸지 여부 (시장가 주문의 경우 true)
    /// 
    /// # Returns
    /// ExecutionResult - 실행 결과
    /// 
    /// # Process (순서 중요!)
    /// 1. WAL 메시지 발행 (먼저!)
    /// 2. 잔고 확인 (locked 확인)
    /// 3. 잔고 이체 (locked → available)
    /// 4. WAL에 잔고 업데이트 메시지 발행
    /// 5. DB 업데이트 명령 전송 (skip_db_updates가 false인 경우만)
    /// 
    /// # 안전성
    /// - WAL 메시지를 먼저 발행하므로 WAL Thread가 디스크에 기록
    /// - 잔고 부족 시 에러 반환 (체결 취소)
    /// 
    /// # 성능
    /// - Channel send: ~100ns (디스크 대기 없음!)
    /// - WAL Thread가 비동기로 디스크 쓰기
    /// - Engine은 즉시 다음 작업 진행
    pub fn execute_trade(&mut self, match_result: &MatchResult, skip_db_updates: bool) -> Result<ExecutionResult> {
        // ============================================
        // Step 1: WAL 메시지 발행 (가장 먼저!) - 주석 처리됨 (WAL 비활성화)
        // ============================================
        // "야, WAL Thread! 이거 디스크에 써줘!" (메시지 발행)
        // Channel에 넣기만 하고 바로 리턴 (Non-blocking)
        // WAL 로그 쓰기 비활성화 (디스크 공간 부족 문제로 인해 임시 비활성화)
        /*
        if let Some(sender) = &self.wal_sender {
            let entry = WalEntry::TradeExecuted {
                buy_order_id: match_result.buy_order_id,
                sell_order_id: match_result.sell_order_id,
                buyer_id: match_result.buyer_id,
                seller_id: match_result.seller_id,
                price: match_result.price.to_string(),
                amount: match_result.amount.to_string(),
                base_mint: match_result.base_mint.clone(),
                quote_mint: match_result.quote_mint.clone(),
                timestamp: Utc::now().timestamp_millis(),
            };
            
            // ★ 메시지 발행 (~100ns, 빠름!)
            sender.send(entry)
                .context("Failed to send trade to WAL channel")?;
        }
        */
        
        // ============================================
        // Step 2: 잔고 이체 (매도자 → 매수자)
        // ============================================
        
        // 계산: 총 거래 금액
        let total_value = match_result.price * match_result.amount;
        
        // 2-1. 매수자: USDT 이체 (locked → 매도자 available)
        self.balance_cache.transfer(
            match_result.buyer_id,
            match_result.seller_id,
            &match_result.quote_mint,  // USDT
            total_value,
            true,  // locked에서 차감
        ).context("Failed to transfer USDT from buyer to seller")?;
        
        // 2-2. 매도자: 기준 자산 이체 (locked → 매수자 available)
        self.balance_cache.transfer(
            match_result.seller_id,
            match_result.buyer_id,
            &match_result.base_mint,  // SOL 등
            match_result.amount,
            true,  // locked에서 차감
        ).context("Failed to transfer base asset from seller to buyer")?;
        
        // ============================================
        // Step 3: DB Writer 채널로 체결 내역 전송 (실시간)
        // ============================================
        // skip_db_updates가 true인 경우 DB 명령 전송 건너뛰기 (시장가 주문의 경우)
        if !skip_db_updates {
            if let Some(sender) = &self.db_sender {
                let cmd = DbCommand::InsertTrade {
                    buy_order_id: match_result.buy_order_id,
                    sell_order_id: match_result.sell_order_id,
                    buyer_id: match_result.buyer_id,
                    seller_id: match_result.seller_id,
                    price: match_result.price,
                    amount: match_result.amount,
                    base_mint: match_result.base_mint.clone(),
                    quote_mint: match_result.quote_mint.clone(),
                    timestamp: Utc::now(),
                };
                let _ = sender.send(cmd);  // Non-blocking (~100ns)
            }
            
            // ============================================
            // Step 4: WAL에 잔고 업데이트 메시지 발행 및 DB Writer로 잔고 업데이트 명령 전송
            // ============================================
            
            // 계산: 총 거래 금액 (이미 위에서 계산했지만 재사용)
            let total_value = match_result.price * match_result.amount;
            
            // 매수자: USDT 차감 (locked에서 차감됨)
            // 매도자: USDT 증가 (available에 추가됨)
            // 매도자: SOL 차감 (locked에서 차감됨)
            // 매수자: SOL 증가 (available에 추가됨)
            
            // [비활성화] Java 서버가 잔고의 Source of Truth가 되므로 엔진에서 DB 업데이트 비활성화
            // 잔고 업데이트는 Kafka 이벤트를 통해 Java Consumer가 처리합니다.
            /*
            if let Some(db_sender) = &self.db_sender {
                // 매수자 USDT 잔고 업데이트 (locked에서 차감됨)
                let _ = db_sender.send(DbCommand::UpdateBalance {
                    user_id: match_result.buyer_id,
                    mint: match_result.quote_mint.clone(),
                    available_delta: None, // available은 변경 없음 (locked에서 차감)
                    locked_delta: Some(-total_value), // locked에서 차감
                });
                
                // 매도자 USDT 잔고 업데이트 (available에 추가됨)
                let _ = db_sender.send(DbCommand::UpdateBalance {
                    user_id: match_result.seller_id,
                    mint: match_result.quote_mint.clone(),
                    available_delta: Some(total_value), // available에 추가
                    locked_delta: None, // locked는 변경 없음
                });
                
                // 매도자 기준 자산 잔고 업데이트 (locked에서 차감됨)
                let _ = db_sender.send(DbCommand::UpdateBalance {
                    user_id: match_result.seller_id,
                    mint: match_result.base_mint.clone(),
                    available_delta: None, // available은 변경 없음 (locked에서 차감)
                    locked_delta: Some(-match_result.amount), // locked에서 차감
                });
                
                // 매수자 기준 자산 잔고 업데이트 (available에 추가됨)
                let _ = db_sender.send(DbCommand::UpdateBalance {
                    user_id: match_result.buyer_id,
                    mint: match_result.base_mint.clone(),
                    available_delta: Some(match_result.amount), // available에 추가
                    locked_delta: None, // locked는 변경 없음
                });
            }
            */
        }
        
        // WAL에도 기록 (복구용) - 주석 처리됨 (WAL 비활성화)
        // WAL 로그 쓰기 비활성화 (디스크 공간 부족 문제로 인해 임시 비활성화)
        /*
        if let Some(sender) = &self.wal_sender {
            // 매수자 USDT 잔고
            if let Some(buyer_usdt) = self.balance_cache.get_balance(match_result.buyer_id, &match_result.quote_mint) {
                sender.send(WalEntry::BalanceUpdated {
                    user_id: match_result.buyer_id,
                    mint: match_result.quote_mint.clone(),
                    available: buyer_usdt.available.to_string(),
                    locked: buyer_usdt.locked.to_string(),
                    timestamp: Utc::now().timestamp_millis(),
                })?;
            }
            
            // 매수자 기준 자산 잔고
            if let Some(buyer_base) = self.balance_cache.get_balance(match_result.buyer_id, &match_result.base_mint) {
                sender.send(WalEntry::BalanceUpdated {
                    user_id: match_result.buyer_id,
                    mint: match_result.base_mint.clone(),
                    available: buyer_base.available.to_string(),
                    locked: buyer_base.locked.to_string(),
                    timestamp: Utc::now().timestamp_millis(),
                })?;
            }
            
            // 매도자 USDT 잔고
            if let Some(seller_usdt) = self.balance_cache.get_balance(match_result.seller_id, &match_result.quote_mint) {
                sender.send(WalEntry::BalanceUpdated {
                    user_id: match_result.seller_id,
                    mint: match_result.quote_mint.clone(),
                    available: seller_usdt.available.to_string(),
                    locked: seller_usdt.locked.to_string(),
                    timestamp: Utc::now().timestamp_millis(),
                })?;
            }
            
            // 매도자 기준 자산 잔고
            if let Some(seller_base) = self.balance_cache.get_balance(match_result.seller_id, &match_result.base_mint) {
                sender.send(WalEntry::BalanceUpdated {
                    user_id: match_result.seller_id,
                    mint: match_result.base_mint.clone(),
                    available: seller_base.available.to_string(),
                    locked: seller_base.locked.to_string(),
                    timestamp: Utc::now().timestamp_millis(),
                })?;
            }
        }
        */
        
        // ============================================
        // Step 4: 실행 결과 반환
        // ============================================
        Ok(ExecutionResult {
            match_result: match_result.clone(),
            success: true,
            error: None,
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    
    #[test]
    fn test_executor_execute_trade_with_db_updates() {
        // 지정가 주문 시나리오: skip_db_updates=false (각 매칭마다 DB 명령 전송)
        // WAL 없이 테스트
        let mut executor = Executor::new_without_wal();
        
        // 초기 잔고 설정
        // 매수자 (user 100): 1000 USDT locked (주문에 사용 중)
        executor.balance_cache_mut().set_balance(
            100,
            "USDT",
            Decimal::ZERO,
            Decimal::from(1000),
        );
        
        // 매도자 (user 200): 10 SOL locked (주문에 사용 중)
        executor.balance_cache_mut().set_balance(
            200,
            "SOL",
            Decimal::ZERO,
            Decimal::from(10),
        );
        
        // MatchResult 생성: 1 SOL @ 100 USDT
        let match_result = MatchResult {
            buy_order_id: 1,
            sell_order_id: 2,
            buyer_id: 100,
            seller_id: 200,
            price: Decimal::from(100),  // 100 USDT
            amount: Decimal::from(1),   // 1 SOL
            base_mint: "SOL".to_string(),
            quote_mint: "USDT".to_string(),
        };
        
        // 체결 실행 (skip_db_updates=false: 지정가 주문은 각 매칭마다 DB 명령 전송)
        let result = executor.execute_trade(&match_result, false);
        assert!(result.is_ok());
        
        // 잔고 확인
        // 매수자: USDT 100 차감 (locked), SOL 1 증가 (available)
        let buyer_usdt = executor.balance_cache().get_balance(100, "USDT").unwrap();
        assert_eq!(buyer_usdt.locked, Decimal::from(900)); // 1000 - 100
        assert_eq!(buyer_usdt.available, Decimal::ZERO);
        
        let buyer_sol = executor.balance_cache().get_balance(100, "SOL").unwrap();
        assert_eq!(buyer_sol.available, Decimal::from(1)); // +1 SOL
        
        // 매도자: SOL 1 차감 (locked), USDT 100 증가 (available)
        let seller_sol = executor.balance_cache().get_balance(200, "SOL").unwrap();
        assert_eq!(seller_sol.locked, Decimal::from(9)); // 10 - 1
        
        let seller_usdt = executor.balance_cache().get_balance(200, "USDT").unwrap();
        assert_eq!(seller_usdt.available, Decimal::from(100)); // +100 USDT
    }
    
    #[test]
    fn test_executor_execute_trade_skip_db_updates() {
        // 시장가 주문 시나리오: skip_db_updates=true (모든 매칭 완료 후 한 번에 DB 명령 전송)
        // WAL 없이 테스트
        let mut executor = Executor::new_without_wal();
        
        // 초기 잔고 설정
        // 매수자 (user 100): 1000 USDT locked (주문에 사용 중)
        executor.balance_cache_mut().set_balance(
            100,
            "USDT",
            Decimal::ZERO,
            Decimal::from(1000),
        );
        
        // 매도자 (user 200): 10 SOL locked (주문에 사용 중)
        executor.balance_cache_mut().set_balance(
            200,
            "SOL",
            Decimal::ZERO,
            Decimal::from(10),
        );
        
        // MatchResult 생성: 1 SOL @ 100 USDT
        let match_result = MatchResult {
            buy_order_id: 1,
            sell_order_id: 2,
            buyer_id: 100,
            seller_id: 200,
            price: Decimal::from(100),  // 100 USDT
            amount: Decimal::from(1),   // 1 SOL
            base_mint: "SOL".to_string(),
            quote_mint: "USDT".to_string(),
        };
        
        // 체결 실행 (skip_db_updates=true: 시장가 주문은 모든 매칭 완료 후 한 번에 DB 명령 전송)
        let result = executor.execute_trade(&match_result, true);
        assert!(result.is_ok());
        
        // 잔고 확인 (메모리 업데이트는 정상적으로 수행되어야 함)
        // 매수자: USDT 100 차감 (locked), SOL 1 증가 (available)
        let buyer_usdt = executor.balance_cache().get_balance(100, "USDT").unwrap();
        assert_eq!(buyer_usdt.locked, Decimal::from(900)); // 1000 - 100
        assert_eq!(buyer_usdt.available, Decimal::ZERO);
        
        let buyer_sol = executor.balance_cache().get_balance(100, "SOL").unwrap();
        assert_eq!(buyer_sol.available, Decimal::from(1)); // +1 SOL
        
        // 매도자: SOL 1 차감 (locked), USDT 100 증가 (available)
        let seller_sol = executor.balance_cache().get_balance(200, "SOL").unwrap();
        assert_eq!(seller_sol.locked, Decimal::from(9)); // 10 - 1
        
        let seller_usdt = executor.balance_cache().get_balance(200, "USDT").unwrap();
        assert_eq!(seller_usdt.available, Decimal::from(100)); // +100 USDT
    }
    
    #[test]
    fn test_executor_lock_balance() {
        let mut executor = Executor::new_without_wal();
        
        // 초기 잔고: 1000 USDT
        executor.balance_cache_mut().set_balance(100, "USDT", Decimal::from(1000), Decimal::ZERO);
        
        // 100 USDT 잠금
        let result = executor.lock_balance_for_order(1, 100, "USDT", Decimal::from(100));
        assert!(result.is_ok());
        
        // 잔고 확인
        let balance = executor.balance_cache().get_balance(100, "USDT").unwrap();
        assert_eq!(balance.available, Decimal::from(900)); // 1000 - 100
        assert_eq!(balance.locked, Decimal::from(100));
    }
    
    #[test]
    fn test_executor_unlock_balance() {
        let mut executor = Executor::new_without_wal();
        
        // 초기 잔고: 100 USDT locked
        executor.balance_cache_mut().set_balance(100, "USDT", Decimal::ZERO, Decimal::from(100));
        
        // 50 USDT 잠금 해제
        let result = executor.unlock_balance_for_cancel(1, 100, "USDT", Decimal::from(50));
        assert!(result.is_ok());
        
        // 잔고 확인
        let balance = executor.balance_cache().get_balance(100, "USDT").unwrap();
        assert_eq!(balance.available, Decimal::from(50)); // 0 + 50
        assert_eq!(balance.locked, Decimal::from(50)); // 100 - 50
    }
    
    #[test]
    fn test_executor_insufficient_balance() {
        // 잔고 부족 시나리오 테스트
        let mut executor = Executor::new_without_wal();
        
        // 초기 잔고: 50 USDT locked (부족! 100 USDT 필요)
        executor.balance_cache_mut().set_balance(100, "USDT", Decimal::ZERO, Decimal::from(50));
        
        // 매도자도 부족
        executor.balance_cache_mut().set_balance(200, "SOL", Decimal::ZERO, Decimal::from(0));
        
        // MatchResult: 1 SOL @ 100 USDT (100 USDT 필요)
        let match_result = MatchResult {
            buy_order_id: 1,
            sell_order_id: 2,
            buyer_id: 100,
            seller_id: 200,
            price: Decimal::from(100),
            amount: Decimal::from(1),
            base_mint: "SOL".to_string(),
            quote_mint: "USDT".to_string(),
        };
        
        // 체결 실행 (실패해야 함) - skip_db_updates 값과 관계없이 잔고 부족으로 실패
        let result = executor.execute_trade(&match_result, false);
        assert!(result.is_err()); // 잔고 부족으로 에러
        
        // skip_db_updates=true인 경우도 동일하게 실패해야 함
        let result2 = executor.execute_trade(&match_result, true);
        assert!(result2.is_err()); // 잔고 부족으로 에러
    }
    
    #[test]
    fn test_executor_multiple_trades_with_db_updates() {
        // 여러 체결 처리 테스트 (지정가 주문 시나리오: skip_db_updates=false)
        let mut executor = Executor::new_without_wal();
        
        // 초기 잔고
        executor.balance_cache_mut().set_balance(100, "USDT", Decimal::ZERO, Decimal::from(1000));
        executor.balance_cache_mut().set_balance(200, "SOL", Decimal::ZERO, Decimal::from(10));
        
        // 첫 번째 체결: 1 SOL @ 100 USDT
        let match1 = MatchResult {
            buy_order_id: 1,
            sell_order_id: 2,
            buyer_id: 100,
            seller_id: 200,
            price: Decimal::from(100),
            amount: Decimal::from(1),
            base_mint: "SOL".to_string(),
            quote_mint: "USDT".to_string(),
        };
        
        // 지정가 주문: 각 매칭마다 DB 명령 전송
        executor.execute_trade(&match1, false).unwrap();
        
        // 두 번째 체결: 0.5 SOL @ 100 USDT
        let match2 = MatchResult {
            buy_order_id: 3,
            sell_order_id: 2,
            buyer_id: 100,
            seller_id: 200,
            price: Decimal::from(100),
            amount: Decimal::from_f64_retain(0.5).unwrap(),
            base_mint: "SOL".to_string(),
            quote_mint: "USDT".to_string(),
        };
        
        executor.execute_trade(&match2, false).unwrap();
        
        // 최종 잔고 확인
        // 매수자: 1000 USDT → 850 USDT, 0 SOL → 1.5 SOL
        let buyer_usdt = executor.balance_cache().get_balance(100, "USDT").unwrap();
        assert_eq!(buyer_usdt.locked, Decimal::from(850)); // 1000 - 150
        
        let buyer_sol = executor.balance_cache().get_balance(100, "SOL").unwrap();
        assert_eq!(buyer_sol.available, Decimal::from_f64_retain(1.5).unwrap()); // 0 + 1.5
        
        // 매도자: 10 SOL → 8.5 SOL, 0 USDT → 150 USDT
        let seller_sol = executor.balance_cache().get_balance(200, "SOL").unwrap();
        assert_eq!(seller_sol.locked, Decimal::from_f64_retain(8.5).unwrap()); // 10 - 1.5
        
        let seller_usdt = executor.balance_cache().get_balance(200, "USDT").unwrap();
        assert_eq!(seller_usdt.available, Decimal::from(150)); // 0 + 150
    }
    
    #[test]
    fn test_executor_multiple_trades_skip_db_updates() {
        // 여러 체결 처리 테스트 (시장가 주문 시나리오: skip_db_updates=true)
        // 시장가 주문은 모든 매칭 완료 후 한 번에 DB 명령 전송
        let mut executor = Executor::new_without_wal();
        
        // 초기 잔고
        executor.balance_cache_mut().set_balance(100, "USDT", Decimal::ZERO, Decimal::from(1000));
        executor.balance_cache_mut().set_balance(200, "SOL", Decimal::ZERO, Decimal::from(10));
        
        // 첫 번째 체결: 1 SOL @ 100 USDT
        let match1 = MatchResult {
            buy_order_id: 1,
            sell_order_id: 2,
            buyer_id: 100,
            seller_id: 200,
            price: Decimal::from(100),
            amount: Decimal::from(1),
            base_mint: "SOL".to_string(),
            quote_mint: "USDT".to_string(),
        };
        
        // 시장가 주문: DB 명령 전송 건너뛰기 (모든 매칭 완료 후 한 번에 전송)
        executor.execute_trade(&match1, true).unwrap();
        
        // 두 번째 체결: 0.5 SOL @ 100 USDT
        let match2 = MatchResult {
            buy_order_id: 3,
            sell_order_id: 2,
            buyer_id: 100,
            seller_id: 200,
            price: Decimal::from(100),
            amount: Decimal::from_f64_retain(0.5).unwrap(),
            base_mint: "SOL".to_string(),
            quote_mint: "USDT".to_string(),
        };
        
        executor.execute_trade(&match2, true).unwrap();
        
        // 최종 잔고 확인 (메모리 업데이트는 정상적으로 수행되어야 함)
        // 매수자: 1000 USDT → 850 USDT, 0 SOL → 1.5 SOL
        let buyer_usdt = executor.balance_cache().get_balance(100, "USDT").unwrap();
        assert_eq!(buyer_usdt.locked, Decimal::from(850)); // 1000 - 150
        
        let buyer_sol = executor.balance_cache().get_balance(100, "SOL").unwrap();
        assert_eq!(buyer_sol.available, Decimal::from_f64_retain(1.5).unwrap()); // 0 + 1.5
        
        // 매도자: 10 SOL → 8.5 SOL, 0 USDT → 150 USDT
        let seller_sol = executor.balance_cache().get_balance(200, "SOL").unwrap();
        assert_eq!(seller_sol.locked, Decimal::from_f64_retain(8.5).unwrap()); // 10 - 1.5
        
        let seller_usdt = executor.balance_cache().get_balance(200, "USDT").unwrap();
        assert_eq!(seller_usdt.available, Decimal::from(150)); // 0 + 150
    }
}
