-- =====================================================
-- 주문 테이블 (orders)
-- =====================================================
-- 설명: 사용자가 생성한 매수/매도 주문을 저장합니다.
-- 봇이 생성한 주문도 일반 사용자 주문과 동일하게 저장됩니다.
-- 
-- 주문 타입:
-- - order_type: 'buy' (매수) 또는 'sell' (매도)
-- - order_side: 'limit' (지정가) 또는 'market' (시장가)
-- 
-- 주문 상태:
-- - pending: 대기 중 (아직 체결 안 됨)
-- - partial: 부분 체결 (일부만 체결됨)
-- - filled: 전량 체결 완료
-- - cancelled: 주문 취소됨
-- 
-- 예시:
-- - 지정가 매수: "SOL을 100 USDT에 1개 구매하고 싶다"
--   → order_type='buy', order_side='limit', price=100.0, amount=1.0
-- - 시장가 매도: "지금 시장가로 SOL 1개 판매하고 싶다"
--   → order_type='sell', order_side='market', price=NULL, amount=1.0
-- 
-- 봇 주문:
-- - 봇이 생성한 주문도 일반 주문과 동일하게 저장
-- - user_id를 특별한 값(예: 0 또는 봇 전용 사용자)으로 설정할 수 있음
-- - 테이블 구조상으로는 일반 주문과 구분 없음
-- =====================================================

CREATE TABLE IF NOT EXISTS orders (
    -- 기본 정보
    id BIGSERIAL PRIMARY KEY,  -- 주문 고유 ID
    
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,  -- 주문한 사용자 ID (봇 주문의 경우 특별한 user_id 사용 가능)
    
    -- 주문 유형
    -- order_type: 매수 또는 매도
    -- - 'buy': 매수 주문 (USDT로 다른 자산 구매)
    -- - 'sell': 매도 주문 (보유 자산을 USDT로 판매)
    order_type VARCHAR(20) NOT NULL CHECK (order_type IN ('buy', 'sell')),
    
    -- order_side: 지정가 또는 시장가
    -- - 'limit': 지정가 주문 (원하는 가격에 주문 등록, 매칭될 때까지 대기)
    -- - 'market': 시장가 주문 (즉시 체결, 오더북의 최적 가격으로 매칭)
    order_side VARCHAR(20) NOT NULL CHECK (order_side IN ('limit', 'market')),
    
    -- 거래쌍 정보
    -- base_mint: 구매/판매하려는 자산 (예: SOL, USDC, RAY 등)
    -- quote_mint: 기준 통화 (항상 USDT)
    -- 예: SOL/USDT 거래 → base_mint='SOL', quote_mint='USDT'
    base_mint VARCHAR(255) NOT NULL,   -- 기준 자산
    quote_mint VARCHAR(255) NOT NULL DEFAULT 'USDT',  -- 기준 통화 (항상 USDT)
    
    -- 주문 가격 (지정가 주문만 필요, 시장가는 NULL)
    -- price: USDT 기준 가격 (1 SOL = 100 USDT 라면 price=100.0)
    -- NULL 허용: 시장가 주문의 경우 가격을 지정하지 않음
    price DECIMAL(30, 9),  -- 지정가 (시장가 주문은 NULL)
    
    -- 주문 수량 정보
    -- amount: 주문한 총 수량 (base_mint 기준)
    -- filled_amount: 체결된 수량 (부분 체결 가능)
    -- filled_quote_amount: 체결된 금액 (USDT 기준, 시장가 주문의 총 결제 금액)
    -- 예: SOL 10개 주문 → 3개 체결 → amount=10.0, filled_amount=3.0
    amount DECIMAL(30, 9) NOT NULL,              -- 주문 수량 (base_mint)
    filled_amount DECIMAL(30, 9) NOT NULL DEFAULT 0,  -- 체결된 수량
    filled_quote_amount DECIMAL(20, 8) NOT NULL DEFAULT 0,  -- 체결된 금액 (USDT 기준, 시장가 주문의 총 결제 금액)
    
    -- 주문 상태
    -- - 'pending': 대기 중 (체결 안 됨)
    -- - 'partial': 부분 체결 (일부만 체결됨)
    -- - 'filled': 전량 체결 완료
    -- - 'cancelled': 주문 취소됨
    -- - 'rejected': 주문 거부됨 (엔진 통신 실패 또는 엔진 거부)
    status VARCHAR(50) NOT NULL DEFAULT 'pending' 
        CHECK (status IN ('pending', 'partial', 'filled', 'cancelled', 'rejected')),
    
    -- 타임스탬프
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),  -- 주문 생성 시간
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()   -- 주문 정보 마지막 업데이트 시간
);

-- 테이블 코멘트
COMMENT ON TABLE orders IS '거래소 주문 테이블 (매수/매도 주문 관리, 봇 주문 포함)';

-- 컬럼 코멘트
COMMENT ON COLUMN orders.id IS '주문 고유 ID';
COMMENT ON COLUMN orders.user_id IS '주문한 사용자 ID (users 테이블 외래키, 사용자 삭제 시 CASCADE 삭제, 봇 주문의 경우 특별한 user_id 사용 가능)';
COMMENT ON COLUMN orders.order_type IS '주문 유형: buy(매수) 또는 sell(매도)';
COMMENT ON COLUMN orders.order_side IS '주문 방식: limit(지정가) 또는 market(시장가)';
COMMENT ON COLUMN orders.base_mint IS '거래하려는 자산 (SOL, USDC, RAY 등)';
COMMENT ON COLUMN orders.quote_mint IS '기준 통화 (항상 USDT)';
COMMENT ON COLUMN orders.price IS '지정가 가격 (USDT 기준, 시장가 주문은 NULL)';
COMMENT ON COLUMN orders.amount IS '주문 수량 (base_mint 기준, 예: SOL 1.0개)';
COMMENT ON COLUMN orders.filled_amount IS '체결된 수량 (부분 체결 가능, amount와 같으면 전량 체결)';
COMMENT ON COLUMN orders.filled_quote_amount IS '체결된 금액 (USDT 기준, 시장가 주문의 총 결제 금액)';
COMMENT ON COLUMN orders.status IS '주문 상태: pending(대기), partial(부분체결), filled(완료), cancelled(취소), rejected(거부)';
COMMENT ON COLUMN orders.created_at IS '주문 생성 시간';
COMMENT ON COLUMN orders.updated_at IS '주문 정보 마지막 업데이트 시간';

-- 인덱스 생성 (성능 최적화)
-- 오더북 조회 최적화: 거래쌍별, 상태별, 주문 타입별, 가격순 조회
-- 이 인덱스는 오더북(호가창) 조회 시 매우 중요한 인덱스입니다.
-- 부분 인덱스: 대기 중인 주문만 인덱싱하여 인덱스 크기 최소화
CREATE INDEX IF NOT EXISTS idx_orders_book ON orders(base_mint, quote_mint, status, order_type, price, created_at) 
    WHERE status IN ('pending', 'partial');

-- 사용자별 주문 조회 최적화 (내 주문 내역 페이지에서 사용)
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id, status, created_at DESC);

-- 특정 거래쌍의 활성 주문 조회 (매칭 엔진에서 사용)
-- 부분 인덱스: 대기 중인 주문만 인덱싱
CREATE INDEX IF NOT EXISTS idx_orders_pair_status ON orders(base_mint, quote_mint, status) 
    WHERE status IN ('pending', 'partial');

