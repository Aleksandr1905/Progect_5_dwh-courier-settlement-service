CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
    id SERIAL PRIMARY KEY,
    courier_id VARCHAR NOT NULL,             -- ID курьера из API
    courier_name VARCHAR NOT NULL,           -- Ф.И.О. курьера
    settlement_year INTEGER NOT NULL,        -- Год отчёта
    settlement_month INTEGER NOT NULL,       -- Месяц отчёта (1-12)
    orders_count INTEGER NOT NULL DEFAULT 0, -- Кол-во заказов за месяц
    orders_total_sum NUMERIC(14, 2) NOT NULL DEFAULT 0, -- Общая стоимость заказов
    rate_avg NUMERIC(3, 2) NOT NULL DEFAULT 0,          -- Средний рейтинг курьера
    order_processing_fee NUMERIC(14, 2) NOT NULL DEFAULT 0, -- Удержано компанией (sum * 0.25)
    courier_order_sum NUMERIC(14, 2) NOT NULL DEFAULT 0,    -- Выплата за заказы (по рейтингу)
    courier_tips_sum NUMERIC(14, 2) NOT NULL DEFAULT 0,     -- Чаевые курьеру
    courier_reward_sum NUMERIC(14, 2) NOT NULL DEFAULT 0,   -- Итого к перечислению (order_sum + tips * 0.95)

    -- Ограничения на значения
    CONSTRAINT dm_courier_ledger_month_check CHECK (settlement_month >= 1 AND settlement_month <= 12),
    CONSTRAINT dm_courier_ledger_rate_check CHECK (rate_avg >= 0 AND rate_avg <= 5),

    -- Уникальный ключ для инкрементальной загрузки/обновления
    CONSTRAINT dm_courier_ledger_unique UNIQUE (courier_id, settlement_year, settlement_month)
);