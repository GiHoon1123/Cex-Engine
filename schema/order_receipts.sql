-- 플랫폼이 orders와 order_submissions를 만든 뒤 적용하는 신규 접수 테이블 정의다.
-- 과거 주문에 접수를 추측해 채우거나 기존 테이블을 변경하지 않는다.
-- 접수는 체결 결과가 아니다. 이 기록만으로 주문을 자동 재실행하지 않는다.
CREATE TABLE engine_order_receipts (
    order_id BIGINT PRIMARY KEY REFERENCES order_submissions(order_id) ON DELETE RESTRICT,
    user_id BIGINT NOT NULL CHECK (user_id > 0),
    order_type TEXT NOT NULL CHECK (order_type IN ('buy', 'sell')),
    order_side TEXT NOT NULL CHECK (order_side IN ('limit', 'market')),
    base_mint TEXT NOT NULL,
    quote_mint TEXT NOT NULL,
    price NUMERIC(30,9),
    amount NUMERIC(30,9),
    quote_amount NUMERIC(30,9),
    accepted_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (order_id > 0)
);
