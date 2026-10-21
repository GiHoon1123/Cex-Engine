//! 실제 캐시 정산을 호출해 성공·실패 시 전체 잔액을 검증한다.
//! 외부 DB·Kafka·봇을 사용하지 않고, 실패 전후 HashMap 전체로 새 행 생성도 확인한다.
use super::*;
use crate::domains::cex::engine::{executor::Executor, types::MatchResult};
use std::panic::{catch_unwind, AssertUnwindSafe};

fn trade() -> MatchResult {
    MatchResult {
        buy_order_id: 1,
        sell_order_id: 2,
        buyer_id: 101,
        seller_id: 102,
        price: Decimal::from(100),
        amount: Decimal::from(2),
        base_mint: "SOL".into(),
        quote_mint: "USDT".into(),
    }
}

fn fixture() -> Executor {
    let mut executor = Executor::new();
    let cache = executor.balance_cache_mut();
    cache.set_balance(101, "USDT", Decimal::from(800), Decimal::from(200));
    cache.set_balance(102, "SOL", Decimal::from(8), Decimal::from(2));
    executor
}

fn assert_failure_unchanged(mut executor: Executor, trade: MatchResult) {
    let before = executor.balance_cache().balances.clone();
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        executor.balance_cache_mut().settle_trade(&trade)
    }));
    assert!(outcome.is_ok(), "유효성·범위 오류가 panic이 되면 안 된다");
    assert!(outcome.unwrap().is_err(), "잘못된 체결을 성공 처리했다");
    assert_eq!(
        executor.balance_cache().balances,
        before,
        "실패한 체결이 잔액이나 행을 변경했다"
    );
}

#[test]
fn second_asset_shortage_does_not_leave_first_transfer_applied() {
    let mut executor = fixture();
    executor
        .balance_cache_mut()
        .set_balance(102, "SOL", Decimal::from(8), Decimal::ONE);
    assert_failure_unchanged(executor, trade());
}

#[test]
fn missing_seller_asset_does_not_create_balances_or_move_quote() {
    let mut executor = fixture();
    executor
        .balance_cache_mut()
        .balances
        .remove(&(102, "SOL".into()));
    assert_failure_unchanged(executor, trade());
}

#[test]
fn first_asset_shortage_keeps_all_balances_unchanged() {
    let mut executor = fixture();
    executor
        .balance_cache_mut()
        .set_balance(101, "USDT", Decimal::ZERO, Decimal::from(199));
    assert_failure_unchanged(executor, trade());
}

#[test]
fn negative_and_zero_trade_values_are_rejected_before_mutation() {
    for (price, amount) in [
        (Decimal::ZERO, Decimal::ONE),
        (Decimal::ONE, Decimal::ZERO),
        (-Decimal::ONE, Decimal::ONE),
        (Decimal::ONE, -Decimal::ONE),
    ] {
        let executor = fixture();
        let mut invalid = trade();
        invalid.price = price;
        invalid.amount = amount;
        assert_failure_unchanged(executor, invalid);
    }
}

#[test]
fn quote_multiplication_overflow_returns_error_without_panic() {
    let executor = fixture();
    let mut invalid = trade();
    invalid.price = Decimal::MAX;
    assert_failure_unchanged(executor, invalid);
}

#[test]
fn second_recipient_overflow_does_not_apply_either_asset() {
    let mut executor = fixture();
    executor
        .balance_cache_mut()
        .set_balance(101, "SOL", Decimal::MAX, Decimal::ZERO);
    assert_failure_unchanged(executor, trade());
}

#[test]
fn quote_finer_than_database_precision_is_not_rounded() {
    let executor = fixture();
    let mut invalid = trade();
    invalid.price = "0.000000001".parse().unwrap();
    invalid.amount = "0.1".parse().unwrap();
    assert_failure_unchanged(executor, invalid);
}

#[test]
fn invalid_asset_pair_is_rejected_before_mutation() {
    let executor = fixture();
    let mut invalid = trade();
    invalid.base_mint = "USDT".into();
    assert_failure_unchanged(executor, invalid);
}

#[test]
fn successful_trade_moves_both_assets() {
    let mut executor = fixture();
    executor.balance_cache_mut().settle_trade(&trade()).unwrap();
    let cache = executor.balance_cache();
    assert_eq!(
        cache.get_balance(101, "USDT").unwrap().locked,
        Decimal::ZERO
    );
    assert_eq!(
        cache.get_balance(102, "USDT").unwrap().available,
        Decimal::from(200)
    );
    assert_eq!(cache.get_balance(102, "SOL").unwrap().locked, Decimal::ZERO);
    assert_eq!(
        cache.get_balance(101, "SOL").unwrap().available,
        Decimal::from(2)
    );
}

#[test]
fn reservation_result_precision_failure_preserves_cache() {
    let mut executor = fixture();
    executor.balance_cache_mut().set_balance(
        101,
        "USDT",
        Decimal::from_str_exact("100000000000000000000").unwrap(),
        Decimal::ZERO,
    );
    let before = executor.balance_cache().balances.clone();
    assert!(executor
        .balance_cache_mut()
        .lock_balance(101, "USDT", Decimal::new(1, 9))
        .is_err());
    assert_eq!(executor.balance_cache().balances, before);
}

#[test]
fn missing_buyer_asset_does_not_create_a_zero_row() {
    let mut executor = fixture();
    executor
        .balance_cache_mut()
        .balances
        .remove(&(101, "USDT".into()));
    assert_failure_unchanged(executor, trade());
}

#[test]
fn valid_database_balance_cannot_overflow_on_either_recipient() {
    for (user, asset) in [(102, "USDT"), (101, "SOL")] {
        let mut executor = fixture();
        executor.balance_cache_mut().set_balance(
            user,
            asset,
            "999999999999999999999".parse().unwrap(),
            Decimal::ZERO,
        );
        assert_failure_unchanged(executor, trade());
    }
}

#[test]
fn decimal_precision_loss_on_last_recipient_rejects_all_changes() {
    let mut executor = fixture();
    let mut tiny = trade();
    tiny.amount = "0.000000001".parse().unwrap();
    executor.balance_cache_mut().set_balance(
        101,
        "SOL",
        "99999999999999999999.12345678".parse().unwrap(),
        Decimal::ZERO,
    );
    assert_failure_unchanged(executor, tiny);
}

#[test]
fn price_and_quantity_must_fit_current_database_precision() {
    for price in [true, false] {
        let executor = fixture();
        let mut invalid = trade();
        if price {
            invalid.price = "0.0000000001".parse().unwrap();
            invalid.amount = Decimal::from(10);
        } else {
            invalid.amount = "0.0000000001".parse().unwrap();
        }
        assert_failure_unchanged(executor, invalid);
    }
}

#[test]
fn smallest_supported_quote_is_transferred_exactly() {
    let mut executor = fixture();
    let mut tiny = trade();
    tiny.price = "0.000000001".parse().unwrap();
    tiny.amount = Decimal::ONE;
    executor.balance_cache_mut().settle_trade(&tiny).unwrap();
    assert_eq!(
        executor
            .balance_cache()
            .get_balance(102, "USDT")
            .unwrap()
            .available,
        tiny.price
    );
    assert_eq!(
        executor
            .balance_cache()
            .get_balance(101, "USDT")
            .unwrap()
            .locked,
        "199.999999999".parse::<Decimal>().unwrap()
    );
}

#[test]
fn same_customer_keys_are_coalesced_without_losing_balances() {
    let mut executor = fixture();
    let mut self_trade = trade();
    self_trade.seller_id = 101;
    executor
        .balance_cache_mut()
        .set_balance(101, "SOL", Decimal::from(8), Decimal::from(2));
    let other = executor
        .balance_cache()
        .get_balance(102, "SOL")
        .unwrap()
        .clone();
    executor
        .balance_cache_mut()
        .settle_trade(&self_trade)
        .unwrap();
    assert_eq!(
        executor
            .balance_cache()
            .get_balance(101, "USDT")
            .unwrap()
            .available,
        Decimal::from(1000)
    );
    assert_eq!(
        executor
            .balance_cache()
            .get_balance(101, "USDT")
            .unwrap()
            .locked,
        Decimal::ZERO
    );
    assert_eq!(
        executor
            .balance_cache()
            .get_balance(101, "SOL")
            .unwrap()
            .available,
        Decimal::from(10)
    );
    assert_eq!(
        executor
            .balance_cache()
            .get_balance(101, "SOL")
            .unwrap()
            .locked,
        Decimal::ZERO
    );
    assert_eq!(
        executor.balance_cache().get_balance(102, "SOL").unwrap(),
        &other
    );
}

#[test]
fn existing_balances_and_unrelated_assets_are_preserved() {
    let mut executor = fixture();
    executor
        .balance_cache_mut()
        .set_balance(101, "SOL", Decimal::from(7), Decimal::from(4));
    executor
        .balance_cache_mut()
        .set_balance(102, "USDT", Decimal::from(11), Decimal::from(9));
    executor
        .balance_cache_mut()
        .set_balance(103, "BTC", Decimal::from(5), Decimal::from(6));
    executor.balance_cache_mut().settle_trade(&trade()).unwrap();
    let cache = executor.balance_cache();
    assert_eq!(
        cache.get_balance(101, "SOL").unwrap(),
        &Balance {
            available: Decimal::from(9),
            locked: Decimal::from(4)
        }
    );
    assert_eq!(
        cache.get_balance(102, "USDT").unwrap(),
        &Balance {
            available: Decimal::from(211),
            locked: Decimal::from(9)
        }
    );
    assert_eq!(
        cache.get_balance(103, "BTC").unwrap(),
        &Balance {
            available: Decimal::from(5),
            locked: Decimal::from(6)
        }
    );
    for (asset, total) in [("USDT", Decimal::from(1020)), ("SOL", Decimal::from(21))] {
        let sum: Decimal = cache
            .balances
            .iter()
            .filter(|((_, mint), _)| mint == asset)
            .map(|(_, b)| b.total())
            .sum();
        assert_eq!(sum, total, "자산별 총액이 보존돼야 한다");
    }
}

#[test]
fn corrupt_negative_balance_is_not_hidden_or_normalized() {
    for (user, asset) in [(101, "USDT"), (102, "USDT"), (102, "SOL"), (101, "SOL")] {
        let mut executor = fixture();
        executor
            .balance_cache_mut()
            .get_balance_mut(user, asset)
            .available = -Decimal::ONE;
        assert_failure_unchanged(executor, trade());
    }
}

#[test]
fn invalid_user_order_and_asset_identifiers_are_rejected() {
    for case in 0..5 {
        let executor = fixture();
        let mut invalid = trade();
        match case {
            0 => invalid.buyer_id = 0,
            1 => invalid.buy_order_id = 0,
            2 => invalid.sell_order_id = 1,
            3 => invalid.base_mint = " SOL".into(),
            _ => invalid.quote_mint = "".into(),
        }
        assert_failure_unchanged(executor, invalid);
    }
}

#[test]
fn enormous_product_of_valid_operands_fails_without_panic() {
    let executor = fixture();
    let mut invalid = trade();
    invalid.price = "100000000000000000000".parse().unwrap();
    invalid.amount = invalid.price;
    assert_failure_unchanged(executor, invalid);
}

#[test]
fn exact_valid_product_beyond_i128_intermediate_is_supported() {
    let mut executor = fixture();
    let mut large = trade();
    large.price = "999999999999999999999".parse().unwrap();
    large.amount = Decimal::ONE;
    executor
        .balance_cache_mut()
        .set_balance(101, "USDT", Decimal::ZERO, large.price);
    executor.balance_cache_mut().settle_trade(&large).unwrap();
    assert_eq!(
        executor
            .balance_cache()
            .get_balance(102, "USDT")
            .unwrap()
            .available,
        large.price
    );
    assert_eq!(
        executor
            .balance_cache()
            .get_balance(101, "USDT")
            .unwrap()
            .locked,
        Decimal::ZERO
    );
}
