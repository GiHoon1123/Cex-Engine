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
use anyhow::{Result, bail};

/// 사용자별 자산별 잔고
/// Key: (user_id, mint_address)
/// Value: Balance
/// 
/// 예시:
/// (123, "SOL") -> Balance { available: 10.0, locked: 1.0 }
/// (123, "USDT") -> Balance { available: 1000.0, locked: 50.0 }
#[derive(Debug, Clone)]
pub struct Balance {
    /// 사용 가능 잔고 (즉시 거래 가능)
    pub available: Decimal,
    /// 잠긴 잔고 (주문에 사용 중)
    pub locked: Decimal,
}

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
        let balance = self.get_balance_mut(user_id, mint);
        
        // 잔고 확인
        if balance.available < amount {
            bail!(
                "Insufficient balance: user={}, mint={}, required={}, available={}",
                user_id, mint, amount, balance.available
            );
        }
        
        // 잠금 실행
        balance.available -= amount;
        balance.locked += amount;
        
        Ok(())
    }
    
    /// 잔고 잠금 해제 (주문 취소 시)
    /// locked 차감 → available 증가
    pub fn unlock_balance(
        &mut self,
        user_id: u64,
        mint: &str,
        amount: Decimal,
    ) -> Result<()> {
        let balance = self.get_balance_mut(user_id, mint);
        
        if balance.locked < amount {
            bail!("Not enough locked balance to unlock");
        }
        
        balance.locked -= amount;
        balance.available += amount;
        
        Ok(())
    }
    
    /// 잔고 이체 (체결 시)
    /// from_user의 locked 차감 → to_user의 available 증가
    /// 
    /// # Arguments
    /// * `from_user` - 보내는 사용자
    /// * `to_user` - 받는 사용자
    /// * `mint` - 자산 주소
    /// * `amount` - 이체 금액
    /// * `from_locked` - from_user가 locked에서 차감? (true: locked, false: available)
    pub fn transfer(
        &mut self,
        from_user: u64,
        to_user: u64,
        mint: &str,
        amount: Decimal,
        from_locked: bool,
    ) -> Result<()> {
        // from_user 차감
        {
            let from_balance = self.get_balance_mut(from_user, mint);
            if from_locked {
                if from_balance.locked < amount {
                    bail!(
                        "Insufficient locked balance: user={}, mint={}, required={}, locked={}, available={}",
                        from_user, mint, amount, from_balance.locked, from_balance.available
                    );
                }
                from_balance.locked -= amount;
            } else {
                if from_balance.available < amount {
                    bail!(
                        "Insufficient available balance: user={}, mint={}, required={}, available={}, locked={}",
                        from_user, mint, amount, from_balance.available, from_balance.locked
                    );
                }
                from_balance.available -= amount;
            }
        }
        
        // to_user 증가
        {
            let to_balance = self.get_balance_mut(to_user, mint);
            to_balance.available += amount;
        }
        
        Ok(())
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

