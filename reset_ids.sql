-- =====================================================
-- Orders와 Trades 테이블의 ID를 1부터 다시 시작하도록 시퀀스 리셋
-- =====================================================

-- 방법 1: 데이터를 모두 삭제한 후 시퀀스를 1로 리셋
-- (주의: 모든 데이터가 삭제됩니다!)

-- TRUNCATE TABLE trades CASCADE;  -- Foreign key 때문에 먼저 삭제
-- TRUNCATE TABLE orders CASCADE;
-- ALTER SEQUENCE orders_id_seq RESTART WITH 1;
-- ALTER SEQUENCE trades_id_seq RESTART WITH 1;

-- 방법 2: 데이터를 유지하면서 시퀀스만 현재 최대값 다음으로 설정
-- (기존 데이터는 유지되고, 새로운 데이터부터 연속된 ID 사용)

-- Orders 테이블 시퀀스 리셋
SELECT setval('orders_id_seq', COALESCE((SELECT MAX(id) FROM orders), 0) + 1, false);

-- Trades 테이블 시퀀스 리셋
SELECT setval('trades_id_seq', COALESCE((SELECT MAX(id) FROM trades), 0) + 1, false);

-- 방법 3: 강제로 1부터 시작 (데이터 삭제 후)
-- TRUNCATE TABLE trades CASCADE;
-- TRUNCATE TABLE orders CASCADE;
-- ALTER SEQUENCE orders_id_seq RESTART WITH 1;
-- ALTER SEQUENCE trades_id_seq RESTART WITH 1;

-- 확인 쿼리 (현재 시퀀스 값 확인)
SELECT last_value FROM orders_id_seq;
SELECT last_value FROM trades_id_seq;
