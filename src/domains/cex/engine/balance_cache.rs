// =====================================================
// BalanceCache - 메모리 기반 잔고 관리
// =====================================================
// 역할: 사용자 잔고를 메모리에 캐싱하여 초고속 조회/업데이트
// 
// 핵심 설계:
// 1. HashMap으로 O(1) 조회/업데이트
// 2. available + locked 분리 관리
// 3. DB와 비동기 동기화 (엔진은 메모리만 사용)
//
// 잔고 상태 변화:
// 1. 주문 생성 → available 차감, locked 증가
// 2. 주문 체결 → locked 차감, 상대방 available 증가
// 3. 주문 취소 → locked 차감, available 증가
// =====================================================

use std::collections::HashMap;
use rust_decimal::Decimal;
use anyhow::{Result, ensure};

mod settlement;

/// 사용자별 자산별 잔고
/// Key: (user_id, mint_address)
/// Value: Balance
/// 
/// 예시:
/// (123, "SOL") -> Balance { available: 10.0, locked: 1.0 }
/// (123, "USDT") -> Balance { available: 1000.0, locked: 50.0 }
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Balance {
    /// 사용 가능 잔고 (즉시 거래 가능)
    pub available: Decimal,
    /// 잠긴 잔고 (주문에 사용 중)
    pub locked: Decimal,
}

#[cfg(test)]
mod settlement_tests;

impl Balance {
    /// 새 잔고 생성 (모두 0)
    pub fn new() -> Self {
        Self {
            available: Decimal::ZERO,
            locked: Decimal::ZERO,
        }
    }
    
    /// 초기 잔고로 생성
    pub fn with_available(amount: Decimal) -> Self {
        Self {
            available: amount,
            locked: Decimal::ZERO,
        }
    }
    
    /// 총 잔고 (available + locked)
    pub fn total(&self) -> Decimal {
        self.available + self.locked
    }
}

/// 메모리 기반 잔고 캐시
/// 
/// 구조:
/// HashMap {
///   (user_id, mint) -> Balance { available, locked }
/// }
/// 
/// 예시:
/// (123, "SOL") -> { available: 10.0, locked: 1.0 }
/// (123, "USDT") -> { available: 1000.0, locked: 50.0 }
/// (456, "SOL") -> { available: 5.0, locked: 0.0 }
pub struct BalanceCache {
    /// Key: (user_id, mint_address)
    /// Value: Balance
    balances: HashMap<(u64, String), Balance>,
}

impl BalanceCache {
    /// 새 BalanceCache 생성
    pub fn new() -> Self {
        Self {
            balances: HashMap::new(),
        }
    }
    
    /// 용량 지정하여 생성 (메모리 사전 할당)
    /// with_capacity()는 HashMap 내부 배열 크기를 미리 확보
    /// 재할당(rehash) 방지로 성능 향상
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            balances: HashMap::with_capacity(capacity),
        }
    }
    
    /// 잔고 조회 (없으면 0으로 초기화)
    /// 
    /// # Arguments
    /// * `user_id` - 사용자 ID
    /// * `mint` - 자산 주소
    /// 
    /// # Returns
    /// 잔고 (가변 참조)
    pub fn get_balance_mut(&mut self, user_id: u64, mint: &str) -> &mut Balance {
        // entry()는 키가 없으면 기본값으로 삽입하고 참조 반환
        // or_insert_with()는 클로저로 기본값 생성 (필요할 때만 실행)
        self.balances
            .entry((user_id, mint.to_string()))
            .or_insert_with(Balance::new)
    }
    
    /// 잔고 조회 (읽기 전용, 없으면 None)
    pub fn get_balance(&self, user_id: u64, mint: &str) -> Option<&Balance> {
        self.balances.get(&(user_id, mint.to_string()))
    }
    
    /// 사용 가능 잔고 확인
    /// 
    /// # Arguments
    /// * `user_id` - 사용자 ID
    /// * `mint` - 자산 주소
    /// * `required_amount` - 필요한 금액
    /// 
    /// # Returns
    /// 잔고가 충분하면 true
    pub fn check_sufficient_balance(
        &self,
        user_id: u64,
        mint: &str,
        required_amount: Decimal,
    ) -> bool {
        match self.get_balance(user_id, mint) {
            Some(balance) => balance.available >= required_amount,
            None => false,
        }
    }
    
    /// 잔고 잠금 (주문 생성 시)
    /// available 차감 → locked 증가
    /// 
    /// # Arguments
    /// * `user_id` - 사용자 ID
    /// * `mint` - 자산 주소
    /// * `amount` - 잠글 금액
    /// 
    /// # Returns
    /// 성공 또는 에러 (잔고 부족 시)
    pub fn lock_balance(
        &mut self,
        user_id: u64,
        mint: &str,
        amount: Decimal,
    ) -> Result<()> {
        self.change_reservation(user_id,mint,amount,true)
    }

    /// 취소·IOC 잔여·가격 개선 금액 해제. 두 결과를 검사한 뒤 한 행을 반영한다.
    pub fn unlock_balance(&mut self,user_id:u64,mint:&str,amount:Decimal)->Result<()> {
        self.change_reservation(user_id,mint,amount,false)
    }

    fn change_reservation(&mut self,user_id:u64,mint:&str,value:Decimal,lock:bool)->Result<()> {
        use super::amount;
        ensure!(value>Decimal::ZERO,"Reservation amount must be positive");
        amount::units(value)?;
        let balance=self.get_balance(user_id,mint).cloned().ok_or_else(||anyhow::anyhow!("Missing reservation balance"))?;
        let (available,locked)=if lock {
            (amount::subtract(balance.available,value)?,amount::add(balance.locked,value)?)
        } else {(amount::add(balance.available,value)?,amount::subtract(balance.locked,value)?)};
        self.set_balance(user_id,mint,available,locked);
        Ok(())
    }

    /// 후보 계산에 필요한 계정만 복사한다. 없는 수신 계정은 정산이 성공할 때 생성한다.
    pub(crate) fn copy_account_from(&mut self,source:&Self,user:u64,asset:&str) {
        if self.get_balance(user,asset).is_none() {
            if let Some(balance)=source.get_balance(user,asset) {
                self.set_balance(user,asset,balance.available,balance.locked);
            }
        }
    }

    /// 전체 후보가 검증된 뒤 영향받은 계정만 반영한다. 호출자는 주문장·Executor
    /// 락을 함께 유지한다. 오류가 생길 수 있는 금액 변환은 이미 끝나 있어야 한다.
    pub(crate) fn commit_prepared(&mut self,prepared:Self) {
        for (key,balance) in prepared.balances { self.balances.insert(key,balance); }
    }

    /// 초기 잔고 설정 (테스트용)
    pub fn set_balance(&mut self, user_id: u64, mint: &str, available: Decimal, locked: Decimal) {
        self.balances.insert(
            (user_id, mint.to_string()),
            Balance { available, locked },
        );
    }

    /// 사용 가능 잔고 증가/감소 (입금/출금용)
    /// 
    /// # Arguments
    /// * `user_id` - 사용자 ID
    /// * `mint` - 자산 종류
    /// * `delta` - 증감량 (양수: 입금, 음수: 출금)
    /// 
    /// # 처리 과정
    /// 1. 잔고 조회 (없으면 0으로 초기화)
    /// 2. available += delta
    /// 3. 음수 잔고 방지 (출금 시 잔고 부족 체크는 호출자가 해야 함)
    /// 
    /// # 예시
    /// ```rust
    /// // 100 USDT 입금
    /// cache.add_available(123, "USDT", Decimal::new(100, 0));
    /// 
    /// // 50 USDT 출금
    /// cache.add_available(123, "USDT", Decimal::new(-50, 0));
    /// ```
    pub fn add_available(&mut self, user_id: u64, mint: &str, delta: Decimal) {
        let balance = self.get_balance_mut(user_id, mint);
        balance.available += delta;
        
        // 음수 잔고 방지 (안전장치)
        if balance.available < Decimal::ZERO {
            balance.available = Decimal::ZERO;
        }
    }

    /// 모든 잔고 삭제 (벤치마크/테스트 초기화용)
    pub fn clear(&mut self) {
        self.balances.clear();
    }
}


#[cfg(test)]
impl BalanceCache {
    /// 테스트에서 원본 Map 전체와 새 행 생성까지 비교한다. 제품 경로에서는 노출하지 않는다.
    pub(crate) fn test_balances_snapshot(&self) -> HashMap<(u64, String), Balance> {
        self.balances.clone()
    }
}
