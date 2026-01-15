use std::time::{Duration, Instant};

use anyhow::Result;
use chrono::Utc;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rust_decimal::Decimal;

#[cfg(feature = "bench_mode")]
use cex_engine::domains::cex::engine::runtime::bench_engine::BenchEngine;
use cex_engine::domains::cex::engine::types::OrderEntry;

const NUM_TEST_USERS: u64 = 100;
// 벤치마크 실행 시간 단축을 위해 배치 크기 줄임
const ORDER_BATCHES: [usize; 2] = [1_000, 5_000]; // 빠른 측정을 위해 2개만

fn initial_sol_balance() -> Decimal {
    Decimal::new(10_000, 0)
}

fn initial_usdt_balance() -> Decimal {
    Decimal::new(10_000_000, 0)
}

fn bench_limit_order_tps(c: &mut Criterion) {
    let mut engine = BenchEngine::new();
    let mut group = c.benchmark_group("limit_order_tps");
    group.sample_size(10); // criterion 최소값
    group.measurement_time(Duration::from_secs(2)); // 2초로 줄임
    group.warm_up_time(Duration::from_millis(500)); // 워밍업 0.5초

    for &order_count in ORDER_BATCHES.iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(order_count),
            &order_count,
            |b, &count| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        reset_bench_state(&mut engine);
                        seed_orderbook(&mut engine);
                        let start = Instant::now();
                        submit_limit_orders_direct(&mut engine, count)
                            .expect("failed to submit orders");
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

fn bench_market_buy_tps(c: &mut Criterion) {
    let mut engine = BenchEngine::new();
    let mut group = c.benchmark_group("market_buy_tps");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));

    for &order_count in ORDER_BATCHES.iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(order_count),
            &order_count,
            |b, &count| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        reset_bench_state(&mut engine);
                        seed_orderbook(&mut engine);
                        let start = Instant::now();
                        submit_market_buys_direct(&mut engine, count)
                            .expect("failed to submit market buys");
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

fn bench_mixed_tps(c: &mut Criterion) {
    let mut engine = BenchEngine::new();
    let mut group = c.benchmark_group("mixed_tps");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));

    for &order_count in ORDER_BATCHES.iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(order_count),
            &order_count,
            |b, &count| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        reset_bench_state(&mut engine);
                        seed_orderbook(&mut engine);
                        let start = Instant::now();
                        submit_mixed_orders_direct(&mut engine, count)
                            .expect("failed to submit mixed orders");
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

fn reset_bench_state(engine: &mut BenchEngine) {
    engine.clear_orderbooks();
    engine.clear_balances();
    seed_balances(engine);
}

fn seed_balances(engine: &mut BenchEngine) {
    for user_id in 1..=NUM_TEST_USERS {
        engine.set_balance(user_id, "SOL", initial_sol_balance(), Decimal::ZERO);
        engine.set_balance(user_id, "USDT", initial_usdt_balance(), Decimal::ZERO);
    }
}

fn seed_orderbook(engine: &mut BenchEngine) {
    let order_id = 10_000_000;
    let price = Decimal::new(10_000, 2); // 100.00 USDT
    let user_id = 10_000;
    let amount = Decimal::new(1_000_000, 0);
    engine.set_balance(user_id, "SOL", amount, Decimal::ZERO);
    let order = build_limit_order(order_id, user_id, price, amount, false);
    engine
        .submit_direct(order)
        .expect("failed to seed ask");
}

fn submit_limit_orders_direct(engine: &mut BenchEngine, total: usize) -> Result<()> {
    for idx in 0..total {
        let order_id = 1_000_000 + idx as u64;
        let user_id = (idx as u64 % NUM_TEST_USERS) + 1;
        let amount = Decimal::new(10, 1); // 1.0 SOL
        let price = Decimal::new(10_000 + (idx as i64 % 50) * 10, 2);
        let is_buy = idx % 2 == 0;

        let order = build_limit_order(order_id, user_id, price, amount, is_buy);
        engine.submit_direct(order)?;
    }

    Ok(())
}

fn submit_market_buys_direct(engine: &mut BenchEngine, total: usize) -> Result<()> {
    for idx in 0..total {
        let order_id = 2_000_000 + idx as u64;
        let user_id = (idx as u64 % NUM_TEST_USERS) + 1;
        let quote_amount = Decimal::new(20_000, 2); // 200 USDT covers highest ask level
        let order = build_market_buy_order(order_id, user_id, quote_amount);
        engine.submit_direct(order)?;
    }
    Ok(())
}

fn submit_mixed_orders_direct(engine: &mut BenchEngine, total: usize) -> Result<()> {
    for idx in 0..total {
        let order_id = 3_000_000 + idx as u64;
        let base_user = (idx as u64 % NUM_TEST_USERS) + 1;
        let price = Decimal::new(10_000 + (idx as i64 % 20) * 10, 2);
        let amount = Decimal::new(10, 1);
        match idx % 3 {
            0 => {
                let order = build_limit_order(order_id, base_user, price, amount, true);
                engine.submit_direct(order)?;
            }
            1 => {
                let order = build_limit_order(order_id, base_user, price, amount, false);
                engine.submit_direct(order)?;
            }
            _ => {
                let quote_amount = Decimal::new(1_000, 2);
                let order = build_market_buy_order(order_id, base_user, quote_amount);
                engine.submit_direct(order)?;
            }
        }
    }
    Ok(())
}

fn build_limit_order(
    order_id: u64,
    user_id: u64,
    price: Decimal,
    amount: Decimal,
    is_buy: bool,
) -> OrderEntry {
    OrderEntry {
        id: order_id,
        user_id,
        order_type: if is_buy { "buy" } else { "sell" }.to_string(),
        order_side: "limit".to_string(),
        base_mint: "SOL".to_string(),
        quote_mint: "USDT".to_string(),
        price: Some(price),
        amount,
        quote_amount: None,
        filled_amount: Decimal::ZERO,
        remaining_amount: amount,
        remaining_quote_amount: None,
        created_at: Utc::now(),
    }
}

fn build_market_buy_order(order_id: u64, user_id: u64, quote_amount: Decimal) -> OrderEntry {
    OrderEntry {
        id: order_id,
        user_id,
        order_type: "buy".to_string(),
        order_side: "market".to_string(),
        base_mint: "SOL".to_string(),
        quote_mint: "USDT".to_string(),
        price: None,
        amount: Decimal::ZERO,
        quote_amount: Some(quote_amount),
        filled_amount: Decimal::ZERO,
        remaining_amount: Decimal::ZERO,
        remaining_quote_amount: Some(quote_amount),
        created_at: Utc::now(),
    }
}

criterion_group!(
    benches,
    bench_limit_order_tps,
    bench_market_buy_tps,
    bench_mixed_tps
);
criterion_main!(benches);


