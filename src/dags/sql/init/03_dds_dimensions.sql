-- Пользователи
CREATE TABLE IF NOT EXISTS dds.dm_users (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR NOT NULL UNIQUE,
    user_name VARCHAR NOT NULL,
    user_login VARCHAR NOT NULL
);

-- Рестораны
CREATE TABLE IF NOT EXISTS dds.dm_restaurants (
    id SERIAL PRIMARY KEY,
    restaurant_id VARCHAR NOT NULL UNIQUE,
    restaurant_name VARCHAR NOT NULL,
    active_from TIMESTAMP NOT NULL,
    active_to TIMESTAMP NOT NULL
);

-- Продукты
CREATE TABLE IF NOT EXISTS dds.dm_products (
    id SERIAL PRIMARY KEY,
    restaurant_id INTEGER NOT NULL,
    product_id VARCHAR NOT NULL UNIQUE,
    product_name VARCHAR NOT NULL,
    product_price NUMERIC(14, 2) NOT NULL DEFAULT 0,
    active_from TIMESTAMP NOT NULL,
    active_to TIMESTAMP NOT NULL,
    CONSTRAINT dm_products_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id)
);

-- Календарь (Таймстампы)
CREATE TABLE IF NOT EXISTS dds.dm_timestamps (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMP NOT NULL UNIQUE,
    year SMALLINT NOT NULL CHECK (year >= 2022 AND year < 2500),
    month SMALLINT NOT NULL CHECK (month >= 1 AND month <= 12),
    day SMALLINT NOT NULL CHECK (day >= 1 AND day <= 31),
    time TIME NOT NULL,
    date DATE NOT NULL
);

-- Заказы
CREATE TABLE IF NOT EXISTS dds.dm_orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    restaurant_id INTEGER NOT NULL,
    timestamp_id INTEGER NOT NULL,
    order_key VARCHAR NOT NULL UNIQUE,
    order_status VARCHAR NOT NULL,
    CONSTRAINT dm_orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES dds.dm_users(id),
    CONSTRAINT dm_orders_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id),
    CONSTRAINT dm_orders_timestamp_id_fkey FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id)
);

CREATE TABLE IF NOT EXISTS dds.dm_couriers(
    id SERIAL PRIMARY KEY,
    courier_id VARCHAR NOT NULL UNIQUE,
    courier_name VARCHAR NOT NULL
);