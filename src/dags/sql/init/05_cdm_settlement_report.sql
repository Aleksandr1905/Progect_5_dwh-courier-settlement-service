CREATE TABLE IF NOT EXISTS cdm.dm_settlement_report (
    id SERIAL PRIMARY KEY,
    restaurant_id VARCHAR NOT NULL,
    restaurant_name VARCHAR NOT NULL,
    settlement_date DATE NOT NULL,
    orders_count INTEGER NOT NULL DEFAULT 0 CHECK (orders_count >= 0),
    orders_total_sum NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (orders_total_sum >= 0),
    orders_bonus_payment_sum NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (orders_bonus_payment_sum >= 0),
    orders_bonus_granted_sum NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (orders_bonus_granted_sum >= 0),
    order_processing_fee NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (order_processing_fee >= 0),
    restaurant_reward_sum NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (restaurant_reward_sum >= 0),
    -- Уникальный ключ, чтобы ON CONFLICT понимал, что обновлять
    CONSTRAINT dm_settlement_report_settlement_date_restaurant_id_unique UNIQUE (restaurant_id, settlement_date)
);