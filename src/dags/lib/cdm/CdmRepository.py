from psycopg import Connection


class CdmRepository:
    def load_cdm_settlement_report(self, conn: Connection) -> None:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                insert into cdm.dm_settlement_report(
                restaurant_id, restaurant_name, settlement_date,
                orders_count, orders_total_sum, orders_bonus_payment_sum,
                orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
                
                select
                    res.id as restaurant_id
                    , res.restaurant_name as restaurant_name
                    , tim.date as settlement_date
                    , count(distinct ord.id) as orders_count
                    , sum(fct.total_sum) as orders_total_sum
                    , sum(fct.bonus_payment) as orders_bonus_payment_sum
                    , sum(fct.bonus_grant) as orders_bonus_granted_sum
                    , sum(fct.total_sum) * 0.25 as order_processing_fee
                    , sum(fct.total_sum) * 0.75 - sum(fct.bonus_payment) as restaurant_reward_sum
                from dds.fct_product_sales fct join dds.dm_orders ord
                on fct.order_id = ord.id
                join dds.dm_restaurants res 
                on ord.restaurant_id = res.id
                join dds.dm_timestamps tim
                on ord.timestamp_id = tim.id
                where ord.order_status = 'CLOSED'
                group by res.id, res.restaurant_name, tim.date
                
                on conflict (restaurant_id, settlement_date)
                do update set
                restaurant_name = EXCLUDED.restaurant_name,
                orders_count = EXCLUDED.orders_count,
                orders_total_sum = EXCLUDED.orders_total_sum,
                orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                order_processing_fee = EXCLUDED.order_processing_fee,
                restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;""")

    def load_cdm_courier_ledger(self, conn: Connection) -> None:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO cdm.dm_courier_ledger (
                    courier_id, courier_name, settlement_year, settlement_month,
                    orders_count, orders_total_sum, rate_avg, order_processing_fee,
                    courier_order_sum, courier_tips_sum, courier_reward_sum
                )
               WITH courier_data AS (
                    SELECT 
                        c.id AS c_id,
                        c.courier_name AS c_name,
                        -- 1. Берем год и месяц ИЗ КАЛЕНДАРЯ (t), к которому привязан заказ
                        t.year AS s_year,
                        t.month AS s_month,
                        COUNT(f.id) AS o_count,
                        SUM(f.sum) AS o_sum,
                        AVG(f.rate) AS a_rate,
                        SUM(f.tip_sum) AS t_sum
                    FROM dds.fct_deliveries f
                    -- 2. Джойним курьеров (чтобы взять имя)
                    JOIN dds.dm_couriers c ON f.courier_id = c.id
                    -- 3. Джойним заказы (чтобы найти, в какой день они были сделаны)
                    JOIN dds.dm_orders o ON f.order_id = o.id 
                    -- 4. Джойним календарь по нашему timestamp_id
                    JOIN dds.dm_timestamps t ON o.timestamp_id = t.id 
                    GROUP BY 1, 2, 3, 4
                )
                SELECT 
                    c_id, c_name, s_year, s_month, o_count, o_sum, a_rate,
                    o_sum * 0.25 AS fee, -- 25% комиссия сервиса
                    -- Расчет выплаты курьеру за заказы:
                    CASE 
                        WHEN a_rate < 4 THEN GREATEST(o_sum * 0.05, o_count * 100)
                        WHEN a_rate < 4.5 THEN GREATEST(o_sum * 0.07, o_count * 150)
                        WHEN a_rate < 4.8 THEN GREATEST(o_sum * 0.08, o_count * 175)
                        ELSE GREATEST(o_sum * 0.1, o_count * 200)
                    END AS c_order_sum,
                    t_sum,
                    -- Итого: Выплата за заказы + чаевые (за вычетом 5% налога на чаевые)
                    (CASE 
                        WHEN a_rate < 4 THEN GREATEST(o_sum * 0.05, o_count * 100)
                        WHEN a_rate < 4.5 THEN GREATEST(o_sum * 0.07, o_count * 150)
                        WHEN a_rate < 4.8 THEN GREATEST(o_sum * 0.08, o_count * 175)
                        ELSE GREATEST(o_sum * 0.1, o_count * 200)
                    END) + (t_sum * 0.95) AS reward
                FROM courier_data
                ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE SET
                    orders_count = EXCLUDED.orders_count,
                    orders_total_sum = EXCLUDED.orders_total_sum,
                    rate_avg = EXCLUDED.rate_avg,
                    courier_order_sum = EXCLUDED.courier_order_sum,
                    courier_tips_sum = EXCLUDED.courier_tips_sum,
                    courier_reward_sum = EXCLUDED.courier_reward_sum;""")
