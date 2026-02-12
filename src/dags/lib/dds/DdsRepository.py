from psycopg import Connection, Cursor
from datetime import datetime


class DdsRepository:
    def insert_user(self, cursor: Cursor, user_id: int, user_name: str, user_login: str) -> None:
        cursor.execute(
            """
            INSERT INTO dds.dm_users (user_id, user_name, user_login)
            VALUES (%(user_id)s, %(user_name)s, %(user_login)s)
            ON CONFLICT (user_id) DO UPDATE SET
            user_name = EXCLUDED.user_name,
            user_login = EXCLUDED.user_login""",
            {"user_id": user_id, "user_name": user_name, "user_login": user_login},
        )

    def insert_restaurants(
            self,
            cursor: Cursor,
            restaurant_id: str,
            restaurant_name: str,
            active_from: datetime,
            active_to: datetime) -> None:
        cursor.execute(
            """
            INSERT INTO dds.dm_restaurants (restaurant_id, restaurant_name, active_from, active_to)
            VALUES (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s)
            """, {"restaurant_id": restaurant_id, "restaurant_name": restaurant_name, "active_from": active_from,
                      "active_to": active_to}
        )

    def insert_timestamps(
            self,
            cursor: Cursor,
            ts: datetime,
            year: int,
            month: int,
            day: int,
            date: datetime,
            time: datetime) -> None:
        cursor.execute(
            """
            INSERT INTO dds.dm_timestamps (ts, year, month, day, date, time)
            VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(date)s, %(time)s)
            ON CONFLICT (ts) DO NOTHING""",
            {"ts": ts, "year": year, "month": month, "day": day, "date": date, "time": time}
        )

    def get_restaurant_id(self, cursor: Cursor, mongo_id: str) -> int:
        cursor.execute(
            """
            SELECT id from dds.dm_restaurants WHERE restaurant_id = %(mongo_id)s
            """,
            {"mongo_id": mongo_id}
        )
        res = cursor.fetchone()
        return res[0] if res else None

    def get_timestamp_id(self, cursor: Cursor, mongo_id: str) -> int:
        cursor.execute(
            """
            SELECT id from dds.dm_timestamps WHERE ts = %(mongo_id)s
            """,
            {"mongo_id": mongo_id}
        )
        res = cursor.fetchone()
        return res[0] if res else None

    def get_user_id(self, cursor: Cursor, mongo_id: str) -> int:
        cursor.execute(
            """
            SELECT id from dds.dm_users WHERE user_id = %(mongo_id)s
            """,
            {"mongo_id": mongo_id}
        )
        res = cursor.fetchone()
        return res[0] if res else None

    def get_order_id(self, cursor: Cursor, mongo_id: str) -> int:
        cursor.execute(
            """SELECT id from dds.dm_orders WHERE order_key = %(mongo_id)s"""
            ,
            {"mongo_id": mongo_id}
        )
        res = cursor.fetchone()
        return res[0] if res else None

    def get_product_id(self, cursor: Cursor, mongo_id: str) -> int:
        cursor.execute(
            """SELECT id from dds.dm_products WHERE product_id = %(mongo_id)s"""
            ,
            {"mongo_id": mongo_id}
        )
        res = cursor.fetchone()
        return res[0] if res else None

    def insert_products(
            self,
            cursor: Cursor,
            product_id: str,
            product_name: str,
            product_price: float,
            active_from: datetime,
            restaurant_id: int) -> None:
        cursor.execute("""
                INSERT INTO dds.dm_products (product_id, product_name, product_price, active_from, active_to, restaurant_id)
                VALUES ( %(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s, %(restaurant_id)s)
                ON CONFLICT (product_id) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                product_price = EXCLUDED.product_price,
                restaurant_id = EXCLUDED.restaurant_id,
                active_from = EXCLUDED.active_from
            """, {
            "product_id": product_id,
            "product_name": product_name,
            "product_price": product_price,
            "restaurant_id": restaurant_id,
            "active_from": active_from,
            "active_to": datetime(2099, 12, 31)})

    def insert_orders(
            self,
            cursor: Cursor,
            order_key: str,
            order_status: str,
            restaurant_id: int,
            timestamp_id: int,
            user_id: int) -> None:
        cursor.execute("""
                INSERT INTO dds.dm_orders (order_key, order_status, restaurant_id, timestamp_id, user_id)
                VALUES (%(order_key)s, %(order_status)s, %(restaurant_id)s, %(timestamp_id)s, %(user_id)s)
                ON CONFLICT (order_key) DO UPDATE SET
                order_status = EXCLUDED.order_status,
                restaurant_id = EXCLUDED.restaurant_id,
                timestamp_id = EXCLUDED.timestamp_id,
                user_id = EXCLUDED.user_id
                """, {
            "order_key": order_key,
            "order_status": order_status,
            "restaurant_id": restaurant_id,
            "timestamp_id": timestamp_id,
            "user_id": user_id})

    def insert_product_sales(
            self,
            cursor: Cursor,
            product_id: int,
            order_id: int,
            count: int,
            price: float,
            total_sum: float,
            bonus_payment: float,
            bonus_grant: float) -> None:
        cursor.execute("""
                INSERT INTO dds.fct_product_sales
                    (product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                VALUES
                    (%(product_id)s,
                    %(order_id)s,
                    %(count)s,
                    %(price)s,
                    %(total_sum)s,
                    %(bonus_payment)s,
                    %(bonus_grant)s)
                    """, {'product_id': product_id,
                          'order_id': order_id,
                          'count': count,
                          'price': price,
                          'total_sum': total_sum,
                          'bonus_payment': bonus_payment,
                          'bonus_grant': bonus_grant})

    def insert_courier(self, cursor, courier_id, courier_name):
        cursor.execute(
            """
                INSERT INTO dds.dm_couriers (courier_id, courier_name)
                VALUES (%(courier_id)s, %(courier_name)s)
                ON CONFLICT (courier_id) DO UPDATE
                SET courier_name = EXCLUDED.courier_name;
            """,
            {
                'courier_id': courier_id,
                'courier_name': courier_name
            })

    def update_order_courier(self, cursor, order_key, courier_id):
        cursor.execute(
            """
                UPDATE dds.dm_orders
                SET courier_id = (Select id from dds.dm_couriers where courier_id = %(courier_id)s)
                WHERE order_key = %(order_key)s
            """,
            {"order_key": order_key,"courier_id": courier_id})

    def insert_fct_delivery(self, cursor, delivery_data):
        cursor.execute("""
                INSERT INTO dds.fct_deliveries (order_id,courier_id, delivery_id, delivery_ts, rate, sum, tip_sum
                )
                SELECT
                    o.id,
                    c.id,
                    %(delivery_id)s,
                    %(delivery_ts)s,
                    %(rate)s,
                    %(sum)s,
                    %(tip_sum)s
                FROM dds.dm_orders o
                JOIN dds.dm_couriers c ON c.courier_id = %(courier_id)s
                WHERE o.order_key = %(order_key)s
                ON CONFLICT (delivery_id) DO NOTHING;
        """, delivery_data)
