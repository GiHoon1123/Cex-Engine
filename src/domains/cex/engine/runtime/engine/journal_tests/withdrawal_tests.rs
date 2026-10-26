//! 출금 계산과 실제 단일 코어·자체 저널·출력의 경계를 검증한다. 실제 블록체인 서명/전송 시험은 아니다.
use super::super::super::{
    WithdrawalAction as Action, WithdrawalCommand as Change, WithdrawalError, WithdrawalFunds,
};
use super::*;
fn change(id: uuid::Uuid, user: u64, asset: &str, value: u64, action: Action) -> Change {
    Change {
        action,
        funds: WithdrawalFunds {
            withdrawal_id: id,
            user_id: user,
            asset: asset.into(),
            amount: Decimal::from(value),
        },
    }
}
fn run(state: &mut EngineState, command: &Change) -> Outcome {
    apply(state, &Command::Withdrawal(command.clone()))
}
fn assert_balance(state: &EngineState, user: u64, asset: &str, available: u64, locked: u64) {
    let b = state
        .executor
        .balance_cache()
        .get_balance(user, asset)
        .unwrap();
    assert_eq!(
        (b.available, b.locked),
        (Decimal::from(available), Decimal::from(locked))
    );
}
#[test]
fn withdrawal_reserve_release_and_duplicates_preserve_other_holds() {
    let mut state = seeded();
    state
        .executor
        .balance_cache_mut()
        .lock_balance(1, "USDT", Decimal::from(100))
        .unwrap();
    let mut cmd = change(uuid::Uuid::new_v4(), 1, "USDT", 250, Action::Reserve);
    assert!(matches!(run(&mut state, &cmd), Outcome::Applied(Some(_))));
    assert_balance(&state, 1, "USDT", 650, 350);
    assert!(matches!(run(&mut state, &cmd), Outcome::Applied(None)));
    cmd.action = Action::Release;
    assert!(matches!(run(&mut state, &cmd), Outcome::Applied(Some(_))));
    assert!(matches!(run(&mut state, &cmd), Outcome::Applied(None)));
    assert_balance(&state, 1, "USDT", 900, 100);
    for action in [Action::Reserve, Action::Complete] {
        cmd.action = action;
        assert!(matches!(run(&mut state, &cmd), Outcome::Failed(_)));
        assert_balance(&state, 1, "USDT", 900, 100);
    }
}
#[test]
fn withdrawal_complete_debits_only_the_original_hold_once() {
    let mut state = seeded();
    let mut a = change(uuid::Uuid::new_v4(), 1, "USDT", 250, Action::Reserve);
    let b = change(uuid::Uuid::new_v4(), 1, "USDT", 100, Action::Reserve);
    run(&mut state, &a);
    run(&mut state, &b);
    a.action = Action::Complete;
    assert!(matches!(run(&mut state, &a), Outcome::Applied(Some(_))));
    assert!(matches!(run(&mut state, &a), Outcome::Applied(None)));
    assert_balance(&state, 1, "USDT", 650, 100);
    for action in [Action::Reserve, Action::Release] {
        a.action = action;
        assert!(matches!(run(&mut state, &a), Outcome::Failed(_)));
        assert_balance(&state, 1, "USDT", 650, 100);
    }
}
#[test]
fn withdrawal_identifier_never_changes_customer_asset_or_amount() {
    let mut state = seeded();
    let cmd = change(uuid::Uuid::new_v4(), 1, "USDT", 250, Action::Reserve);
    run(&mut state, &cmd);
    for action in [Action::Reserve, Action::Release, Action::Complete] {
        for field in 0..3 {
            let mut changed = cmd.clone();
            changed.action = action;
            match field {
                0 => changed.funds.user_id = 2,
                1 => changed.funds.asset = "SOL".into(),
                _ => changed.funds.amount = Decimal::ONE,
            };
            assert!(matches!(run(&mut state, &changed), Outcome::Failed(_)));
            assert_balance(&state, 1, "USDT", 750, 250);
        }
    }
}
#[test]
fn withdrawal_insufficient_reservation_is_permanent_even_after_new_funds() {
    let mut state = seeded();
    let mut cmd = change(uuid::Uuid::new_v4(), 1, "USDT", 2000, Action::Reserve);
    assert!(matches!(
        run(&mut state, &cmd),
        Outcome::Applied(Some(output::OutputBatch::WithdrawalRejected { .. }))
    ));
    assert_balance(&state, 1, "USDT", 1000, 0);
    let mut credit = super::deposit_credit(uuid::Uuid::new_v4(), 1, Decimal::from(5000));
    credit.asset = "USDT".into();
    apply(&mut state, &Command::CreditDeposit(credit));
    for action in [Action::Reserve, Action::Release, Action::Complete] {
        cmd.action = action;
        assert!(matches!(run(&mut state, &cmd), Outcome::Failed(_)));
        assert_balance(&state, 1, "USDT", 6000, 0);
    }
}
#[test]
fn withdrawal_resolution_cannot_create_an_unknown_reservation() {
    let mut state = seeded();
    let mut cmd = change(uuid::Uuid::new_v4(), 1, "USDT", 250, Action::Release);
    for action in [Action::Release, Action::Complete] {
        cmd.action = action;
        assert!(matches!(run(&mut state, &cmd), Outcome::Failed(_)));
        assert!(state.withdrawal_receipts.is_empty());
        assert_balance(&state, 1, "USDT", 1000, 0);
    }
    cmd.action = Action::Reserve;
    assert!(matches!(run(&mut state, &cmd), Outcome::Applied(Some(_))));
}
#[test]
fn invalid_withdrawal_never_consumes_identity_or_creates_accounts() {
    let mut state = seeded();
    let original = change(uuid::Uuid::new_v4(), 3, "BTC", 1, Action::Reserve);
    for index in 0..9 {
        let mut cmd = original.clone();
        match index {
            0 => cmd.funds.withdrawal_id = uuid::Uuid::nil(),
            1 => cmd.funds.user_id = 0,
            2 => cmd.funds.user_id = u64::MAX,
            3 => cmd.funds.asset = "".into(),
            4 => cmd.funds.asset = "btc".into(),
            5 => cmd.funds.asset = "BTC ".into(),
            6 => cmd.funds.amount = Decimal::ZERO,
            7 => cmd.funds.amount = -Decimal::ONE,
            _ => cmd.funds.amount = Decimal::new(1, 10),
        };
        assert!(matches!(run(&mut state, &cmd), Outcome::Failed(_)));
        assert!(state.withdrawal_receipts.is_empty());
        assert!(state
            .executor
            .balance_cache()
            .get_balance(3, "BTC")
            .is_none());
    }
}
#[test]
fn withdrawal_locked_overflow_preserves_both_balances() {
    let mut state = seeded();
    let max = Decimal::from_str_exact("999999999999999999999").unwrap();
    state
        .executor
        .balance_cache_mut()
        .set_balance(3, "BTC", Decimal::ONE, max);
    let cmd = change(uuid::Uuid::new_v4(), 3, "BTC", 1, Action::Reserve);
    assert!(matches!(
        run(&mut state, &cmd),
        Outcome::Applied(Some(output::OutputBatch::WithdrawalRejected { .. }))
    ));
    let b = state
        .executor
        .balance_cache()
        .get_balance(3, "BTC")
        .unwrap();
    assert_eq!((b.available, b.locked), (Decimal::ONE, max));
}
#[test]
fn order_fills_and_cancellation_preserve_the_separate_withdrawal_share() {
    let mut state = seeded();
    let mut withdrawal = change(uuid::Uuid::new_v4(), 2, "SOL", 3, Action::Reserve);
    run(&mut state, &withdrawal);
    assert!(matches!(
        apply(&mut state, &Command::Submit(order(1, 2, false, 4))),
        Outcome::Applied(_)
    ));
    assert_balance(&state, 2, "SOL", 3, 7);
    apply(&mut state, &Command::Submit(order(2, 1, true, 2)));
    assert_balance(&state, 2, "SOL", 3, 5);
    apply(
        &mut state,
        &Command::Cancel {
            order_id: 1,
            user_id: 2,
            pair: TradingPair::new("SOL".into(), "USDT".into()),
        },
    );
    assert_balance(&state, 2, "SOL", 5, 3);
    withdrawal.action = Action::Complete;
    assert!(matches!(
        run(&mut state, &withdrawal),
        Outcome::Applied(Some(_))
    ));
    assert_balance(&state, 2, "SOL", 5, 0);
    assert_balance(&state, 1, "SOL", 2, 0);
}
#[test]
fn withdrawal_history_cannot_be_silently_reseeded_as_genesis() -> Result<()> {
    for action in [Action::Reserve, Action::Release, Action::Complete] {
        let directory = Directory::new();
        let mut state = seeded();
        let mut cmd = change(uuid::Uuid::new_v4(), 1, "USDT", 250, Action::Reserve);
        run(&mut state, &cmd);
        cmd.action = action;
        run(&mut state, &cmd);
        let mut store = Store::open(&directory.0)?;
        assert!(store.initialize(&state).is_err());
        assert!(!store.initialized()?);
    }
    Ok(())
}
#[tokio::test]
async fn withdrawal_worker_concurrent_same_reservation_publishes_only_once_and_restarts(
) -> Result<()> {
    let directory = Directory::new();
    let store = initialize(&directory.0);
    let mut engine = HighPerformanceEngine::new_bench();
    let (sent, events) = bounded(20);
    attach(&mut engine, store, move |batch, pos| {
        output::visit_events(batch, pos, |_, raw| {
            sent.send(raw).unwrap();
            Ok(())
        })
    });
    let command = change(uuid::Uuid::new_v4(), 1, "USDT", 250, Action::Reserve);
    let handle = engine.handle();
    let (a, b) = tokio::join!(
        handle.change_withdrawal(command.clone()),
        handle.change_withdrawal(command.clone())
    );
    a?;
    b?;
    assert_eq!(
        handle.get_balance(1, "USDT").await?,
        (Decimal::from(750), Decimal::from(250))
    );
    engine.stop().await?;
    let sent: Vec<_> = events.try_iter().collect();
    assert_eq!(sent.len(), 1);
    let event: serde_json::Value = serde_json::from_str(&sent[0])?;
    assert_eq!(event["event_type"], "withdrawal_reserved");
    assert_eq!(
        event["withdrawal_id"],
        command.funds.withdrawal_id.to_string()
    );
    let mut restarted = HighPerformanceEngine::new_bench();
    let (tx, rx) = bounded(20);
    attach(
        &mut restarted,
        Store::open(&directory.0)?,
        move |batch, pos| {
            output::visit_events(batch, pos, |_, raw| {
                tx.send(raw).unwrap();
                Ok(())
            })
        },
    );
    restarted
        .handle()
        .change_withdrawal(command.clone())
        .await?;
    let release = Change {
        action: Action::Release,
        ..command.clone()
    };
    restarted
        .handle()
        .change_withdrawal(release.clone())
        .await?;
    restarted.handle().change_withdrawal(release).await?;
    assert!(restarted.handle().change_withdrawal(command).await.is_err());
    assert_eq!(
        restarted.handle().get_balance(1, "USDT").await?,
        (Decimal::from(1000), Decimal::ZERO)
    );
    restarted.stop().await?;
    let released: Vec<_> = rx.try_iter().collect();
    assert_eq!(released.len(), 1);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&released[0])?["event_type"],
        "withdrawal_released"
    );
    Ok(())
}
#[tokio::test]
async fn withdrawal_resolve_race_has_one_winner_and_no_negative_balance() -> Result<()> {
    let directory = Directory::new();
    let store = initialize(&directory.0);
    let mut engine = HighPerformanceEngine::new_bench();
    attach(&mut engine, store, |_, _| Ok(()));
    let handle = engine.handle();
    let cmd = change(uuid::Uuid::new_v4(), 1, "USDT", 250, Action::Reserve);
    handle.change_withdrawal(cmd.clone()).await?;
    let (released, completed) = tokio::join!(
        handle.change_withdrawal(Change {
            action: Action::Release,
            ..cmd.clone()
        }),
        handle.change_withdrawal(Change {
            action: Action::Complete,
            ..cmd
        })
    );
    assert_ne!(released.is_ok(), completed.is_ok());
    let balance = handle.get_balance(1, "USDT").await?;
    assert_eq!(balance.1, Decimal::ZERO);
    assert_eq!(
        balance.0,
        if released.is_ok() {
            Decimal::from(1000)
        } else {
            Decimal::from(750)
        }
    );
    engine.stop().await?;
    Ok(())
}
#[tokio::test]
async fn withdrawal_journal_failure_never_confirms_or_publishes() -> Result<()> {
    let directory = Directory::new();
    let store = initialize(&directory.0);
    store.connection.execute_batch("CREATE TRIGGER reject_withdrawal BEFORE INSERT ON records BEGIN SELECT RAISE(ABORT,'격리 출금 저널 장애'); END;")?;
    let mut engine = HighPerformanceEngine::new_bench();
    let (sent, events) = bounded(10);
    attach(&mut engine, store, move |batch, pos| {
        output::visit_events(batch, pos, |_, raw| {
            sent.send(raw).unwrap();
            Ok(())
        })
    });
    assert!(matches!(
        engine
            .handle()
            .change_withdrawal(change(
                uuid::Uuid::new_v4(),
                1,
                "USDT",
                250,
                Action::Reserve
            ))
            .await,
        Err(WithdrawalError::OutcomeUnknown)
    ));
    assert!(engine.stop().await.is_err());
    assert!(events.try_recv().is_err());
    let store = Store::open(&directory.0)?;
    assert_eq!(store.last_sequence()?, 0);
    assert_balance(&store.recover()?.0, 1, "USDT", 1000, 0);
    Ok(())
}
#[tokio::test]
async fn withdrawal_output_failure_replays_the_exact_original_event() -> Result<()> {
    let directory = Directory::new();
    let store = initialize(&directory.0);
    let mut engine = HighPerformanceEngine::new_bench();
    let (sent, events) = bounded(10);
    attach(&mut engine, store, move |batch, pos| {
        output::visit_events(batch, pos, |index, raw| {
            sent.send(raw).unwrap();
            Err(output::DeliveryError {
                next_event: index,
                reason: "격리 출금 브로커 응답 유실".into(),
            })
        })
    });
    let command = change(uuid::Uuid::new_v4(), 1, "USDT", 250, Action::Reserve);
    engine.handle().change_withdrawal(command.clone()).await?;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while engine.handle().is_accepting() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(engine.stop().await.is_err());
    let original = events.try_recv()?;
    let store = Store::open(&directory.0)?;
    assert_eq!(store.delivered()?, 0);
    let mut restarted = HighPerformanceEngine::new_bench();
    let (sent, events) = bounded(10);
    attach(&mut restarted, store, move |batch, pos| {
        output::visit_events(batch, pos, |_, raw| {
            sent.send(raw).unwrap();
            Ok(())
        })
    });
    restarted.handle().change_withdrawal(command).await?;
    restarted.stop().await?;
    assert_eq!(events.try_recv()?, original);
    assert!(events.try_recv().is_err());
    Ok(())
}
#[tokio::test]
async fn withdrawal_timeout_can_commit_later_without_creating_a_second_reservation() -> Result<()> {
    let directory = Directory::new();
    let mut store = initialize(&directory.0);
    let (mut engine, receiver) = super::deposit_gated_core(seeded());
    let handle = engine.handle();
    let cmd = change(uuid::Uuid::new_v4(), 1, "USDT", 250, Action::Reserve);
    let original = cmd.clone();
    let task = tokio::spawn(async move { handle.change_withdrawal(cmd).await });
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while receiver.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(matches!(task.await?, Err(WithdrawalError::OutcomeUnknown)));
    assert_eq!(store.last_sequence()?, 0);
    // 응답 시간 초과는 계산이나 커밋 취소가 아니다. 같은 원본으로만 재확인한다.
    let group: Vec<_> = receiver.try_iter().collect();
    store.append(&group, &mut 0)?;
    for item in group {
        item.finish();
    }
    engine.stop().await?;
    let mut restarted = HighPerformanceEngine::new_bench();
    let (sent, events) = bounded(10);
    attach(&mut restarted, store, move |batch, pos| {
        output::visit_events(batch, pos, |_, raw| {
            sent.send(raw).unwrap();
            Ok(())
        })
    });
    restarted.handle().change_withdrawal(original).await?;
    assert_eq!(
        restarted.handle().get_balance(1, "USDT").await?,
        (Decimal::from(750), Decimal::from(250))
    );
    restarted.stop().await?;
    assert_eq!(events.try_iter().count(), 1);
    Ok(())
}
fn router(engine: &HighPerformanceEngine) -> axum::Router {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgresql://unused:unused@127.0.0.1:1/unused")
        .unwrap();
    crate::routes::create_router("isolated-engine-service-token-32-bytes").with_state(
        crate::shared::services::AppState::new(
            crate::shared::database::Database::noop(pool),
            engine.handle(),
        ),
    )
}
fn request(id: uuid::Uuid, action: &str) -> axum::http::Request<axum::body::Body> {
    axum::http::Request::post("/api/cex/withdrawals/reservations").header("x-engine-service-token","isolated-engine-service-token-32-bytes").header("content-type","application/json").body(axum::body::Body::from(serde_json::json!({"withdrawal_id":id,"user_id":1,"asset":"USDT","amount":"250","action":action}).to_string())).unwrap()
}
#[tokio::test]
async fn withdrawal_http_waits_for_the_actual_journal_commit_before_success_or_output() -> Result<()>
{
    use tower::ServiceExt;
    let directory = Directory::new();
    let store = initialize(&directory.0);
    let mut engine = HighPerformanceEngine::new_bench();
    let (sent, events) = bounded(10);
    attach(&mut engine, store, move |batch, pos| {
        output::visit_events(batch, pos, |_, raw| {
            sent.send(raw).unwrap();
            Ok(())
        })
    });
    let blocker = rusqlite::Connection::open(directory.0.join("journal.sqlite3"))?;
    blocker.execute_batch("BEGIN IMMEDIATE")?;
    let app = router(&engine);
    let response = app.oneshot(request(uuid::Uuid::new_v4(), "reserve"));
    tokio::pin!(response);
    tokio::select! {result=&mut response=>panic!("저널 확정 전에 출금 예약을 응답함: {:?}",result),_=tokio::time::sleep(std::time::Duration::from_millis(100))=>{}}
    assert!(events.try_recv().is_err());
    blocker.execute_batch("ROLLBACK")?;
    assert_eq!(response.await?.status(), axum::http::StatusCode::OK);
    engine.stop().await?;
    assert_eq!(events.try_iter().count(), 1);
    Ok(())
}
#[tokio::test]
async fn withdrawal_http_terminal_state_survives_recovery() -> Result<()> {
    use tower::ServiceExt;
    for terminal in ["release", "complete"] {
        let directory = Directory::new();
        let store = initialize(&directory.0);
        let mut engine = HighPerformanceEngine::new_bench();
        attach(&mut engine, store, |_, _| Ok(()));
        let id = uuid::Uuid::new_v4();
        let app = router(&engine);
        for action in ["reserve", terminal] {
            let response = app.clone().oneshot(request(id, action)).await?;
            assert_eq!(response.status(), axum::http::StatusCode::OK);
            assert_eq!(response.headers()["cache-control"], "no-store");
        }
        engine.stop().await?;
        let mut restarted = HighPerformanceEngine::new_bench();
        attach(&mut restarted, Store::open(&directory.0)?, |_, _| Ok(()));
        let app = router(&restarted);
        assert_eq!(
            app.clone().oneshot(request(id, terminal)).await?.status(),
            axum::http::StatusCode::OK
        );
        assert_eq!(
            app.oneshot(request(id, "reserve")).await?.status(),
            axum::http::StatusCode::CONFLICT
        );
        let expected = if terminal == "release" { 1000 } else { 750 };
        assert_eq!(
            restarted.handle().get_balance(1, "USDT").await?,
            (Decimal::from(expected), Decimal::ZERO)
        );
        restarted.stop().await?;
    }
    Ok(())
}
#[tokio::test]
async fn withdrawal_insufficient_funds_is_durable_only_after_the_record_is_committed() -> Result<()>
{
    let directory = Directory::new();
    let mut store = initialize(&directory.0);
    let (mut engine, receiver) = super::deposit_gated_core(seeded());
    let handle = engine.handle();
    let command = change(uuid::Uuid::new_v4(), 1, "USDT", 2000, Action::Reserve);
    let original = command.clone();
    let task = tokio::spawn(async move { handle.change_withdrawal(command).await });
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while receiver.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(!task.is_finished());
    let group: Vec<_> = receiver.try_iter().collect();
    assert!(matches!(
        group[0].outcome,
        Outcome::Applied(Some(output::OutputBatch::WithdrawalRejected { .. }))
    ));
    store.append(&group, &mut 0)?;
    for item in group {
        item.finish();
    }
    assert!(matches!(task.await?, Err(WithdrawalError::Rejected { .. })));
    engine.stop().await?;
    let mut restarted = HighPerformanceEngine::new_bench();
    attach(&mut restarted, store, |_, _| Ok(()));
    restarted
        .handle()
        .update_balance(1, "USDT", Decimal::from(5000))
        .await?;
    assert!(matches!(
        restarted.handle().change_withdrawal(original).await,
        Err(WithdrawalError::Rejected { .. })
    ));
    assert_eq!(
        restarted.handle().get_balance(1, "USDT").await?,
        (Decimal::from(6000), Decimal::ZERO)
    );
    restarted.stop().await?;
    Ok(())
}

#[tokio::test]
async fn withdrawal_closed_or_full_input_is_not_an_accepted_command() -> Result<()> {
    use super::super::super::{balance_commands::BalanceCommand, EngineHandle};
    for full in [true, false] {
        let (orders, _order_rx) = bounded(1);
        let (balances, balance_rx) = bounded(1);
        let handle = EngineHandle {
            order_tx: orders,
            balance_tx: balances.clone(),
            closed: Arc::new(AtomicBool::new(!full)),
        };
        if full {
            balances
                .send(BalanceCommand::UpdateBalance {
                    user_id: 1,
                    mint: "USDT".into(),
                    available_delta: Decimal::ZERO,
                    response: tokio::sync::oneshot::channel().0,
                })
                .unwrap();
        }
        // 포화 입력에서 동기 send나 3초 응답 대기를 수행하면 이 짧은 비동기 기한을 넘는다.
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            handle.change_withdrawal(change(
                uuid::Uuid::new_v4(),
                1,
                "USDT",
                250,
                Action::Reserve,
            )),
        )
        .await?;
        assert!(matches!(result, Err(WithdrawalError::NotAccepted)));
        assert_eq!(balance_rx.len(), usize::from(full));
    }
    Ok(())
}

#[test]
fn withdrawal_safety_share_survives_market_budget_and_fee_completion() {
    let mut state = seeded();
    let mut withdrawal = change(uuid::Uuid::new_v4(), 1, "USDT", 100, Action::Reserve);
    assert!(matches!(
        run(&mut state, &withdrawal),
        Outcome::Applied(Some(_))
    ));
    apply(&mut state, &Command::Submit(order(1, 2, false, 4)));
    let mut market = order(2, 1, true, 0);
    market.order_side = "market".into();
    market.price = None;
    market.quote_amount = Some(Decimal::from(250));
    market.remaining_quote_amount = market.quote_amount;
    market.fee_rate = Decimal::new(1, 2);
    assert!(matches!(
        apply(&mut state, &Command::Submit(market)),
        Outcome::Applied(Some(_))
    ));
    assert_balance(&state, 1, "USDT", 650, 100);
    assert_eq!(
        state
            .executor
            .balance_cache()
            .get_balance(1, "SOL")
            .unwrap()
            .available,
        Decimal::new(2475, 3)
    );
    // 시장가의 예산 잔여 해제가 출금 예약분까지 반환하면 이 단계의 금액이 달라진다.
    withdrawal.action = Action::Release;
    assert!(matches!(
        run(&mut state, &withdrawal),
        Outcome::Applied(Some(_))
    ));
    assert_balance(&state, 1, "USDT", 750, 0);
}

#[tokio::test]
async fn withdrawal_safety_distinct_concurrent_reservations_cannot_spend_the_same_available(
) -> Result<()> {
    let directory = Directory::new();
    let mut engine = HighPerformanceEngine::new_bench();
    attach(&mut engine, initialize(&directory.0), |_, _| Ok(()));
    let handle = engine.handle();
    let first = change(uuid::Uuid::new_v4(), 1, "USDT", 700, Action::Reserve);
    let second = change(uuid::Uuid::new_v4(), 1, "USDT", 700, Action::Reserve);
    let (a, b) = tokio::join!(
        handle.change_withdrawal(first),
        handle.change_withdrawal(second)
    );
    assert_ne!(a.is_ok(), b.is_ok());
    assert_eq!(
        handle.get_balance(1, "USDT").await?,
        (Decimal::from(300), Decimal::from(700))
    );
    engine.stop().await?;
    assert_balance(
        &Store::open(&directory.0)?.recover()?.0,
        1,
        "USDT",
        300,
        700,
    );
    Ok(())
}

#[tokio::test]
async fn withdrawal_rejection_with_failed_recording_is_unknown_and_has_no_financial_result(
) -> Result<()> {
    let directory = Directory::new();
    let store = initialize(&directory.0);
    store.connection.execute_batch("CREATE TRIGGER reject_withdrawal_result BEFORE INSERT ON records BEGIN SELECT RAISE(ABORT,'격리 출금 거절 기록 장애'); END;")?;
    let mut engine = HighPerformanceEngine::new_bench();
    let (sent, events) = bounded(10);
    attach(&mut engine, store, move |batch, pos| {
        output::visit_events(batch, pos, |_, raw| {
            sent.send(raw).unwrap();
            Ok(())
        })
    });
    let result = engine
        .handle()
        .change_withdrawal(change(
            uuid::Uuid::new_v4(),
            1,
            "USDT",
            2000,
            Action::Reserve,
        ))
        .await;
    assert!(matches!(result, Err(WithdrawalError::OutcomeUnknown)));
    assert!(engine.stop().await.is_err());
    assert!(events.try_recv().is_err());
    let store = Store::open(&directory.0)?;
    assert_eq!(store.last_sequence()?, 0);
    assert!(store.recover()?.0.withdrawal_receipts.is_empty());
    Ok(())
}
