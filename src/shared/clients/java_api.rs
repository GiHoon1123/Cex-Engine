use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use rust_decimal::Decimal;
use serde_json::json;

/// Java API 클라이언트
/// Java API Client
/// 
/// 역할: Java API 서버와 통신하여 주문 생성
/// 봇 주문도 Java를 통해 생성하여 orders 테이블에 저장되도록 함
pub struct JavaApiClient {
    http_client: Client,
    base_url: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct CreateOrderRequest {
    order_type: String,
    order_side: String,
    base_mint: String,
    quote_mint: String,
    price: Option<String>,
    amount: Option<String>,
    quote_amount: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OrderResponse {
    order: OrderDto,
    message: String,
}

#[derive(Debug, Deserialize)]
struct OrderDto {
    #[serde(deserialize_with = "deserialize_u64_from_string")]
    id: u64,
    #[serde(deserialize_with = "deserialize_u64_from_string")]
    user_id: u64,
    order_type: String,
    order_side: String,
    base_mint: String,
    quote_mint: String,
    price: Option<String>,
    amount: String,
    filled_amount: String,
    filled_quote_amount: String,
    status: String,
    created_at: String,
    updated_at: String,
}

fn deserialize_u64_from_string<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let s = String::deserialize(deserializer)?;
    s.parse::<u64>().map_err(Error::custom)
}

impl JavaApiClient {
    /// Java API 클라이언트 생성
    pub fn new(java_api_url: Option<String>) -> Result<Self> {
        let http_client = Client::builder()
            .build()
            .context("Failed to create HTTP client")?;

        let base_url = java_api_url.unwrap_or_else(|| "http://localhost:8080".to_string());

        Ok(Self {
            http_client,
            base_url,
        })
    }

    /// Java API를 통해 주문 생성
    /// Create order through Java API
    /// 
    /// 봇 주문도 Java를 통해 생성하여 orders 테이블에 저장되도록 함
    pub async fn create_order(
        &self,
        user_id: u64,
        order_type: &str,
        order_side: &str,
        base_mint: &str,
        quote_mint: &str,
        price: Option<Decimal>,
        amount: Option<Decimal>,
        quote_amount: Option<Decimal>,
    ) -> Result<u64> {
        let request = CreateOrderRequest {
            order_type: order_type.to_string(),
            order_side: order_side.to_string(),
            base_mint: base_mint.to_string(),
            quote_mint: quote_mint.to_string(),
            price: price.map(|p| p.to_string()),
            amount: amount.map(|a| a.to_string()),
            quote_amount: quote_amount.map(|q| q.to_string()),
        };

        let url = format!("{}/api/orders", self.base_url);
        
        // JWT 토큰 없이 호출 (봇 주문은 인증 불필요)
        // Java API에서 봇 user_id를 허용하도록 설정되어 있어야 함
        let response = self
            .http_client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&json!({
                "order_type": request.order_type,
                "order_side": request.order_side,
                "base_mint": request.base_mint,
                "quote_mint": request.quote_mint,
                "price": request.price,
                "amount": request.amount,
                "quote_amount": request.quote_amount,
            }))
            .send()
            .await
            .context("Failed to send request to Java API")?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Java API returned error: status={}, body={}",
                status,
                text
            ));
        }

        let order_response: OrderResponse = response
            .json()
            .await
            .context("Failed to parse Java API response")?;

        Ok(order_response.order.id)
    }
}
