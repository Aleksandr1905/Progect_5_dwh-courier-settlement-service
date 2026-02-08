CREATE TABLE IF NOT EXISTS dds.fct_product_sales (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    order_id INTEGER NOT NULL,
    count INTEGER NOT NULL DEFAULT 0 CHECK (count >= 0),
    price NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (price >= 0),
    total_sum NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (total_sum >= 0),
    bonus_payment NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (bonus_payment >= 0),
    bonus_grant NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (bonus_grant >= 0),
    CONSTRAINT fct_product_sales_product_id_fkey FOREIGN KEY (product_id) REFERENCES dds.dm_products(id),
    CONSTRAINT fct_product_sales_order_id_fkey FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id)
);