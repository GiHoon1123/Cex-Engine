# Rust 엔진 코드 삭제 검토 문서

## 📋 현재 Java와 연동되는 기능 (유지 필요)

### 1. HTTP API 엔드포인트
- ✅ `POST /api/cex/orders` - 주문 생성 (Java → Rust)
- ✅ `DELETE /api/cex/orders/:id` - 주문 취소 (Java → Rust)
- ✅ `POST /api/cex/balances/sync` - 잔고 동기화 (Java → Rust)
- ✅ `GET /api/health` - 헬스체크

### 2. 핵심 엔진 기능
- ✅ 매칭 엔진 (`domains/cex/engine/`)
  - `executor.rs` - 주문 실행 로직
  - `matcher.rs` - 매칭 알고리즘
  - `orderbook.rs` - 오더북 관리
  - `balance_cache.rs` - 메모리 잔고 캐시
  - `runtime/threads.rs` - 엔진 스레드 루프
  - `runtime/engine.rs` - 엔진 런타임

### 3. Kafka 이벤트 발행
- ✅ `shared/kafka/producer.rs` - Kafka 프로듀서
- ✅ `shared/kafka/events.rs` - 이벤트 구조체 (TradeExecutedEvent 등)

### 4. Java API 클라이언트 (봇용)
- ✅ `shared/clients/java_api.rs` - 봇이 Java API를 통해 주문 생성

---

## 🗑️ 삭제 가능한 기능들

### 1. 전체 도메인 삭제 가능

#### ❌ `domains/auth/` (인증/인가)
- **이유**: Java에서 JWT 인증 처리
- **파일들**:
  - `domains/auth/handlers/` - 모든 파일
  - `domains/auth/models/` - 모든 파일
  - `domains/auth/routes.rs`
  - `domains/auth/services/` - 모든 파일
- **영향**: 없음 (이미 주석 처리됨)

#### ❌ `domains/wallet/` (지갑 관리)
- **이유**: Java에서 지갑 관리 처리
- **파일들**:
  - `domains/wallet/handlers/` - 모든 파일
  - `domains/wallet/models/` - 모든 파일
  - `domains/wallet/routes.rs`
  - `domains/wallet/services/` - 모든 파일
- **영향**: 없음 (이미 주석 처리됨)

#### ❌ `domains/swap/` (스왑 기능)
- **이유**: 사용하지 않음
- **파일들**:
  - `domains/swap/handlers/` - 모든 파일
  - `domains/swap/models/` - 모든 파일
  - `domains/swap/routes.rs`
  - `domains/swap/services/` - 모든 파일
- **영향**: 없음 (이미 주석 처리됨)

---

### 2. CEX 도메인 내부 삭제 가능

#### ❌ `domains/cex/handlers/trade_handler.rs`
- **이유**: 체결 내역 조회는 Java에서 처리
- **기능**: `GET /api/cex/trades`, `GET /api/cex/trades/my` 등
- **영향**: 없음 (이미 주석 처리됨)

#### ❌ `domains/cex/handlers/position_handler.rs`
- **이유**: 포지션 조회는 Java에서 처리
- **기능**: `GET /api/cex/positions`, `GET /api/cex/positions/:mint` 등
- **영향**: 없음 (이미 주석 처리됨)

#### ❌ `domains/cex/handlers/balance_handler.rs` (일부)
- **유지**: `sync_balance` 함수만 필요 (잔고 동기화)
- **삭제 가능**: `get_all_balances`, `get_balance` 등 조회 함수들
- **영향**: 없음 (이미 주석 처리됨)

#### ❌ `domains/cex/handlers/order_handler.rs` (일부)
- **유지**: `create_order`, `cancel_order` 함수만 필요
- **삭제 가능**: `get_order`, `get_my_orders`, `get_orderbook` 등 조회 함수들
- **영향**: 없음 (이미 주석 처리됨)

---

### 3. 서비스 레이어 삭제 가능

#### ❌ `domains/cex/services/trade_service.rs`
- **이유**: 체결 내역 조회는 Java에서 처리
- **영향**: 없음

#### ❌ `domains/cex/services/position_service.rs`
- **이유**: 포지션 조회는 Java에서 처리
- **영향**: 없음

#### ❌ `domains/cex/services/balance_service.rs` (일부)
- **유지**: 잔고 동기화 관련 함수만 필요
- **삭제 가능**: 조회 함수들
- **영향**: 없음

#### ❌ `domains/cex/services/order_service.rs` (일부)
- **유지**: 주문 생성/취소 관련 함수만 필요
- **삭제 가능**: 조회 함수들
- **영향**: 없음

#### ✅ `domains/cex/services/udp_orderbook_feed.rs` (유지)
- **이유**: 사용자가 유지하고 싶어함
- **상태**: 유지

#### ❌ `domains/cex/services/udp_orderbook_feed_test.rs`
- **이유**: 테스트 파일
- **영향**: 없음

---

### 4. 데이터베이스 관련 삭제 가능

#### ❌ `shared/database/repositories/cex/order_repository.rs`
- **이유**: 주문 저장은 Java에서 처리 (Rust 엔진은 메모리만 사용)
- **영향**: 없음 (코드에서 이미 사용 안 함)

#### ❌ `shared/database/repositories/cex/trade_repository.rs`
- **이유**: 체결 내역 저장은 Java에서 처리
- **영향**: 없음

#### ❌ `shared/database/repositories/cex/balance_repository.rs`
- **이유**: 잔고 저장은 Java에서 처리
- **영향**: 없음

#### ❌ `shared/database/repositories/auth/` (전체)
- **이유**: 인증은 Java에서 처리
- **영향**: 없음

#### ❌ `shared/database/repositories/wallet/` (전체)
- **이유**: 지갑 관리는 Java에서 처리
- **영향**: 없음

#### ⚠️ `shared/database/repositories/cex/fee_repository.rs`
- **검토 필요**: 수수료 설정 조회가 필요한지 확인
- **현재 사용 여부**: 확인 필요

---

### 5. 엔진 런타임 내부 삭제 가능

#### ❌ `domains/cex/engine/runtime/bench_engine.rs`
- **이유**: 벤치마크 코드 (프로덕션 불필요)
- **영향**: 없음 (feature flag로 보호됨)

#### ❌ `domains/cex/engine/runtime/db_commands.rs`
- **이유**: DB 명령어 (주문 저장 등)는 Java에서 처리
- **영향**: 없음 (코드에서 이미 사용 안 함)

#### ⚠️ `domains/cex/engine/runtime/balance_commands.rs`
- **검토 필요**: 잔고 명령어가 필요한지 확인
- **현재 사용 여부**: 확인 필요

#### ⚠️ `domains/cex/engine/runtime/commands.rs`
- **검토 필요**: 명령어 처리 로직 확인 필요
- **현재 사용 여부**: 확인 필요

#### ❌ `domains/cex/engine/mock.rs`
- **이유**: Mock 엔진 (테스트용)
- **영향**: 없음

#### ✅ `domains/cex/engine/wal.rs` (유지)
- **이유**: 사용자가 유지하고 싶어함
- **상태**: 유지

---

### 6. 클라이언트 삭제 가능

#### ❌ `shared/clients/jupiter.rs`
- **이유**: Jupiter 스왑 클라이언트 (사용 안 함)
- **영향**: 없음

#### ❌ `shared/clients/solana.rs`
- **이유**: Solana RPC 클라이언트 (사용 안 함)
- **영향**: 없음

---

### 7. 에러 처리 삭제 가능

#### ❌ `shared/errors/auth_error.rs`
- **이유**: 인증 에러는 Java에서 처리
- **영향**: 없음

#### ❌ `shared/errors/wallet_error.rs`
- **이유**: 지갑 에러는 Java에서 처리
- **영향**: 없음

---

### 8. 미들웨어 삭제 가능

#### ❌ `shared/middleware/auth.rs`
- **이유**: 인증 미들웨어는 Java에서 처리
- **영향**: 없음 (이미 사용 안 함)

---

### 9. 모델 삭제 가능

#### ❌ `domains/cex/models/trade.rs` (일부)
- **유지**: Kafka 이벤트용 TradeExecutedEvent 구조체
- **삭제 가능**: 조회용 Trade 모델

#### ❌ `domains/cex/models/position.rs`
- **이유**: 포지션 조회는 Java에서 처리
- **영향**: 없음

#### ❌ `domains/auth/models/` (전체)
- **이유**: 인증은 Java에서 처리
- **영향**: 없음

#### ❌ `domains/wallet/models/` (전체)
- **이유**: 지갑 관리는 Java에서 처리
- **영향**: 없음

#### ❌ `domains/swap/models/` (전체)
- **이유**: 스왑 기능 사용 안 함
- **영향**: 없음

---

### 10. 벤치마크 코드 (유지)

#### ✅ `benches/` 디렉토리 전체 (유지)
- **이유**: 사용자가 유지하고 싶어함
- **상태**: 유지

#### ✅ `Cargo.toml`의 벤치마크 설정 (유지)
- **이유**: 벤치마크 관련 설정 유지
- **상태**: 유지

---

## ⚠️ 삭제 전 확인 필요 항목

### 1. `domains/cex/engine/runtime/balance_commands.rs`
- **확인**: 잔고 명령어가 실제로 사용되는지 확인
- **조치**: 사용 안 하면 삭제

### 2. `domains/cex/engine/runtime/commands.rs`
- **확인**: 명령어 처리 로직이 실제로 사용되는지 확인
- **조치**: 사용 안 하면 삭제

### 3. `domains/cex/engine/wal.rs`
- **확인**: Write-Ahead Log가 필요한지 확인
- **조치**: 필요 없으면 삭제

### 4. `shared/database/repositories/cex/fee_repository.rs`
- **확인**: 수수료 설정 조회가 필요한지 확인
- **조치**: 필요 없으면 삭제

### 5. `domains/bot/services/orderbook_sync.rs`
- **확인**: 봇의 오더북 동기화가 필요한지 확인
- **조치**: 필요 없으면 삭제

---

## 📊 삭제 예상 효과

### 코드 라인 수 감소
- 예상: 약 30-40% 코드 감소
- 주로: 인증, 지갑, 스왑, 조회 API 핸들러

### 컴파일 시간 단축
- 예상: 약 20-30% 단축
- 이유: 불필요한 의존성 제거

### 바이너리 크기 감소
- 예상: 약 15-25% 감소
- 이유: 사용하지 않는 코드 제거

### 유지보수성 향상
- 이유: 코드베이스 단순화

---

## 🔄 단계적 마이그레이션 계획 (조회 API)

### Phase 1: Java에서 조회 API 구현 (카멜 케이스)

다음 조회 API들을 Java에서 먼저 구현합니다. 

**중요**: Java는 이미 카멜 케이스를 사용하고 있으므로, 응답값은 Rust와 동일하되 필드명은 스네이크 케이스 → 카멜 케이스로 변환합니다.

**변환 규칙**:
- Rust (snake_case) → Java (camelCase)
- 예: `user_id` → `userId`, `order_type` → `orderType`, `filled_amount` → `filledAmount`

#### 1. 주문 조회 API
- `GET /api/cex/orders/:id` - 단일 주문 조회
- `GET /api/cex/orders/my` - 내 주문 목록
- `GET /api/cex/orderbook` - 오더북 조회

**Rust 응답 구조 (snake_case)**:
```rust
// Order 모델
{
  "order_id": 123,
  "user_id": 1,
  "order_type": "buy",
  "order_side": "limit",
  "base_mint": "SOL",
  "quote_mint": "USDT",
  "price": "100.5",
  "amount": "10.0",
  "filled_amount": "5.0",
  "filled_quote_amount": "502.5",
  "status": "partial",
  "created_at": "2026-01-29T00:00:00Z",
  "updated_at": "2026-01-29T00:00:00Z"
}

// OrderbookResponse
{
  "bids": [Order, ...],
  "asks": [Order, ...]
}
```

**Java 응답 구조 (camelCase - 현재 사용 중)**:
```java
// OrderResponse.OrderDto (이미 카멜 케이스 사용 중)
{
  "id": "123",  // 문자열로 직렬화 (JavaScript 정밀도 손실 방지)
  "userId": 1,
  "orderType": "buy",
  "orderSide": "limit",
  "baseMint": "SOL",
  "quoteMint": "USDT",
  "price": "100.5",
  "amount": "10.0",
  "filledAmount": "5.0",
  "filledQuoteAmount": "502.5",
  "status": "partial",
  "createdAt": "2026-01-29T00:00:00Z",
  "updatedAt": "2026-01-29T00:00:00Z"
}

// OrderbookResponse
{
  "bids": [OrderResponse, ...],
  "asks": [OrderResponse, ...]
}
```

#### 2. 체결 내역 조회 API
- `GET /api/cex/trades` - 거래쌍별 체결 내역
- `GET /api/cex/trades/my` - 내 체결 내역

**Rust 응답 구조 (snake_case)**:
```rust
// Trade 모델
{
  "trade_id": 456,
  "buy_order_id": 123,
  "sell_order_id": 124,
  "buyer_id": 1,
  "seller_id": 2,
  "base_mint": "SOL",
  "quote_mint": "USDT",
  "price": "100.5",
  "amount": "5.0",
  "created_at": "2026-01-29T00:00:00Z"
}
```

**Java 응답 구조 (camelCase - 현재 사용 중)**:
```java
// TradeResponse (카멜 케이스로 구현)
{
  "id": 456,  // 또는 "tradeId": 456
  "buyOrderId": 123,
  "sellOrderId": 124,
  "buyerId": 1,
  "sellerId": 2,
  "baseMint": "SOL",
  "quoteMint": "USDT",
  "price": "100.5",
  "amount": "5.0",
  "createdAt": "2026-01-29T00:00:00Z"
}
```

#### 3. 포지션 조회 API
- `GET /api/cex/positions` - 모든 자산 포지션 조회
- `GET /api/cex/positions/:mint` - 특정 자산 포지션 조회

**Rust 응답 구조 (snake_case)**:
```rust
// AssetPosition 모델
{
  "mint": "SOL",
  "current_balance": "11.0",
  "available": "10.0",
  "locked": "1.0",
  "average_entry_price": "100.5",
  "total_bought_amount": "15.0",
  "total_bought_cost": "1507.5",
  "current_market_price": "110.0",
  "current_value": "1210.0",
  "unrealized_pnl": "702.5",
  "unrealized_pnl_percent": "46.6",
  "trade_summary": {
    "total_buy_trades": 5,
    "total_sell_trades": 2,
    "realized_pnl": "50.0"
  }
}
```

**Java 응답 구조 (camelCase - 구현 필요)**:
```java
// PositionResponse (카멜 케이스로 구현)
{
  "mint": "SOL",
  "currentBalance": "11.0",
  "available": "10.0",
  "locked": "1.0",
  "averageEntryPrice": "100.5",
  "totalBoughtAmount": "15.0",
  "totalBoughtCost": "1507.5",
  "currentMarketPrice": "110.0",
  "currentValue": "1210.0",
  "unrealizedPnl": "702.5",
  "unrealizedPnlPercent": "46.6",
  "tradeSummary": {
    "totalBuyTrades": 5,
    "totalSellTrades": 2,
    "realizedPnl": "50.0"
  }
}
```

#### 4. 잔고 조회 API
- `GET /api/cex/balances` - 모든 잔고 조회
- `GET /api/cex/balances/:mint` - 특정 자산 잔고 조회

**Rust 응답 구조 (snake_case)**:
```rust
// UserBalance 모델
{
  "user_id": 1,
  "mint_address": "SOL",
  "available": "10.0",
  "locked": "1.0"
}
```

**Java 응답 구조 (camelCase - 구현 필요)**:
```java
// BalanceResponse (카멜 케이스로 구현)
{
  "userId": 1,
  "mintAddress": "SOL",  // 또는 "mint": "SOL"
  "available": "10.0",
  "locked": "1.0"
}
```

### Phase 2: Rust에서 조회 API 삭제

Java에서 조회 API 구현이 완료되고 테스트가 완료되면, Rust에서 다음 파일들을 삭제합니다:

#### 삭제할 핸들러
- `domains/cex/handlers/trade_handler.rs` (전체)
- `domains/cex/handlers/position_handler.rs` (전체)
- `domains/cex/handlers/order_handler.rs` (조회 함수만: `get_order`, `get_my_orders`, `get_orderbook`)
- `domains/cex/handlers/balance_handler.rs` (조회 함수만: `get_all_balances`, `get_balance`)

#### 삭제할 서비스
- `domains/cex/services/trade_service.rs` (전체)
- `domains/cex/services/position_service.rs` (전체)
- `domains/cex/services/order_service.rs` (조회 함수만)
- `domains/cex/services/balance_service.rs` (조회 함수만)

#### 삭제할 모델 (일부)
- `domains/cex/models/trade.rs` (조회용 Trade 모델, Kafka 이벤트용은 유지)
- `domains/cex/models/position.rs` (전체)

---

## 🔍 삭제 순서 제안

### 즉시 삭제 가능 (Java 연동 없음)

1. **1단계**: 전체 도메인 삭제 (auth, wallet, swap)
2. **2단계**: 데이터베이스 리포지토리 정리 (order, trade, balance, auth, wallet)
3. **3단계**: 클라이언트 및 에러 처리 정리 (jupiter, solana, auth_error, wallet_error)
4. **4단계**: 미들웨어 정리 (auth middleware)

### Java 마이그레이션 후 삭제 (조회 API)

5. **5단계**: Java에서 조회 API 구현 완료 후
   - CEX 도메인 내부 조회 핸들러/서비스 정리
   - 조회용 모델 정리

### 유지 항목

- ✅ 벤치마크 코드 (`benches/`, `bench_engine.rs`)
- ✅ UDP 오더북 피드 (`udp_orderbook_feed.rs`)
- ✅ WAL (`wal.rs`)

---

## ✅ 삭제 후 확인 사항

1. 컴파일 성공 확인
2. 엔진 정상 시작 확인
3. 주문 생성/취소 동작 확인
4. 잔고 동기화 동작 확인
5. Kafka 이벤트 발행 확인
