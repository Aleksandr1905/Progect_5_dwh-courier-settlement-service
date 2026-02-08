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
