//! 실제 주문 조회·취소가 입력 경계와 같은 금액을 보존하는지 검증한다.
use super::*;
use sqlx::Executor as _;

async fn schema(pool: &PgPool) -> Result<()> {
    pool.execute("CREATE TABLE orders(id BIGSERIAL PRIMARY KEY,user_id BIGINT,order_type TEXT,order_side TEXT,
        base_mint TEXT,quote_mint TEXT,price NUMERIC(30,9),amount NUMERIC(30,9),filled_amount NUMERIC(30,9),
        filled_quote_amount NUMERIC(30,9),status TEXT,created_at TIMESTAMPTZ DEFAULT now(),updated_at TIMESTAMPTZ DEFAULT now());
        INSERT INTO orders(id,user_id,order_type,order_side,base_mint,quote_mint,price,amount,filled_amount,filled_quote_amount,status)
        VALUES(1,101,'buy','limit','SOL','USDT',100,79228162514264337593.543950335,0,0,'pending'),
        (2,101,'buy','limit','SOL','USDT',9,1,0,0,'pending')").await?;
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn order_reads_preserve_exact_amount_and_numeric_price_priority(pool: PgPool) -> Result<()> {
    schema(&pool).await?;
    let repo = OrderRepository::new(pool);
    let amount = Decimal::from_str_exact("79228162514264337593.543950335")?;
    assert_eq!(repo.get_by_id(1).await?.unwrap().amount, amount);
    assert!(repo.get_by_id(999).await?.is_none());
    assert_eq!(repo.get_all_by_user(101, None, None).await?.len(), 2);
    assert_eq!(
        repo.get_by_user_and_status(101, "pending", None, None)
            .await?
            .len(),
        2
    );
    for filter in [None, Some("buy")] {
        assert_eq!(
            repo.get_active_orders("SOL", "USDT", filter).await?.len(),
            2
        );
    }
    assert_eq!(repo.get_all_active_orders().await?.len(), 2);
    let (buys, sells) = repo.get_orderbook("SOL", "USDT", None).await?;
    assert!(sells.is_empty());
    assert_eq!(
        buys.iter().map(|order| order.id).collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(repo.update_status(1, "partial").await?.amount, amount);
    assert_eq!(
        repo.update_filled_amount(1, Decimal::ONE)
            .await?
            .filled_amount,
        Decimal::ONE
    );
    assert_eq!(repo.cancel_order(1).await?.amount, amount);
    // INSERT/RETURNING도 같은 변환을 쓰므로 실제 쓰기 경계를 함께 검사한다.
    sqlx::query("SELECT setval('orders_id_seq',2)")
        .execute(&repo.pool)
        .await?;
    let created = repo
        .create(&OrderCreate {
            user_id: 101,
            order_type: "sell".into(),
            order_side: "limit".into(),
            base_mint: "SOL".into(),
            quote_mint: "USDT".into(),
            price: Some(Decimal::ONE),
            amount,
        })
        .await?;
    assert_eq!(created.amount, amount);
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn order_read_rejects_unrepresentable_database_amount_without_panic(
    pool: PgPool,
) -> Result<()> {
    schema(&pool).await?;
    pool.execute("UPDATE orders SET amount=999999999999999999999.123456789 WHERE id=1")
        .await?;
    assert!(OrderRepository::new(pool).get_by_id(1).await.is_err());
    Ok(())
}
