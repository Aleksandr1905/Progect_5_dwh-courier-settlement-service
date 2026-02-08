import logging
import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable

from lib.pg_connect import ConnectionBuilder

from lib.settings_repository import EtlSettingsRepository
from lib.dds.DdsLoader import DdsLoader
from lib.dds.DdsRepository import DdsRepository
from lib.dds.StgReader import StgReader
from lib.dict_util import str2json
from datetime import datetime

log = logging.getLogger(__name__)

dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
settings_repo = EtlSettingsRepository()
stg_reader = StgReader(dwh_pg_connect)


@dag(
    dag_id="stg_to_dds",
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2026, 2, 4, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['dds'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def stg_to_dds_dag():
    def save_users_handler(conn, row):
        payload = str2json(row[2])
        repo = DdsRepository()

        repo.insert_user(
            conn,
            user_id=row[1],
            user_name=payload.get('name'),
            user_login=payload.get('login')
        )

    def save_restaurants_handler(conn, row):
        payload = str2json(row[2])
        repo = DdsRepository()

        repo.insert_restaurants(
            conn,
            restaurant_id=row[1],
            restaurant_name=payload.get('name'),
            active_from=row[3],
            active_to=datetime(2099, 12, 31))

    def save_timestamps_handler(conn, row):
        payload = str2json(row[2])
        dt_str = payload.get('date')

        dt = datetime.fromisoformat(dt_str)
        repo = DdsRepository()
        repo.insert_timestamps(
            conn,
            ts=dt,
            year=dt.year,
            month=dt.month,
            day=dt.day,
            date=dt.date(),
            time=dt.time(),
        )

    def save_products_handler(conn, row):
        payload = str2json(row[2])
        mongo_id = payload.get('restaurant').get('id')
        repo = DdsRepository()
        rest_id = repo.get_restaurant_id(conn, mongo_id)

        if not rest_id:
            return

        products = payload.get('order_items')
        for item in products:
            repo.insert_products(
                conn,
                product_id=item['id'],
                product_name=item['name'],
                product_price=item['price'],
                active_from=row[3],
                restaurant_id=rest_id
            )

    def save_orders_handler(conn, row):

        payload = str2json(row[2])
        repo = DdsRepository()

        rest_mongo_id = payload.get('restaurant').get('id')
        rest_id = repo.get_restaurant_id(conn, rest_mongo_id)
        if not rest_id:
            return

        user_mongo_id = payload.get('user').get('id')
        user_id = repo.get_user_id(conn, user_mongo_id)
        if not user_id:
            return

        ts_str = payload.get('update_ts')
        timestamp_dt = datetime.fromisoformat(ts_str)
        timestamp_id = repo.get_timestamp_id(conn, timestamp_dt)
        if not timestamp_id:
            return

        repo.insert_orders(
            conn,
            order_key=payload['_id'],
            order_status=payload['final_status'],
            restaurant_id=rest_id,
            timestamp_id=timestamp_id,
            user_id=user_id)

    def save_fct_sales_handler(conn, row):
        payload = str2json(row[3])

        repo = DdsRepository()
        order_mongo_id = payload.get('order_id')
        order_id = repo.get_order_id(conn, order_mongo_id)

        if not order_id:
            return

        products = payload.get('product_payments')
        for item in products:
            product_mongo_id = item.get('product_id')
            product_id = repo.get_product_id(conn, product_mongo_id)

            if product_id:
                repo.insert_product_sales(
                    conn,
                    product_id=product_id,
                    order_id=order_id,
                    count=int(item.get('quantity')),
                    price=float(item.get('price')),
                    total_sum=float(item.get('product_cost')),
                    bonus_payment=float(item.get('bonus_payment')),
                    bonus_grant=float(item.get('bonus_grant'))
                )



    @task(task_id='dm_user_load')
    def load_users():
        loader = DdsLoader(
            reader=stg_reader,
            settings_repository=settings_repo,
            save_handler=save_users_handler,
            workflow_key='users_stg_to_dds',
            stg_table_name='stg.ordersystem_users',
            pg_dest=dwh_pg_connect,
            logger=log
        )
        loader.run_copy()

    @task(task_id='dm_restaurant_load')
    def load_restaurants():
        loader = DdsLoader(
            reader=stg_reader,
            settings_repository=settings_repo,
            save_handler=save_restaurants_handler,
            workflow_key='restaurants_stg_to_dds',
            stg_table_name='stg.ordersystem_restaurants',
            pg_dest=dwh_pg_connect,
            logger=log
        )
        loader.run_copy()

    @task(task_id='dm_timestamp_load')
    def load_timestamps():
        loader = DdsLoader(
            reader=stg_reader,
            settings_repository=settings_repo,
            save_handler=save_timestamps_handler,
            workflow_key='timestamp_stg_to_dds',
            stg_table_name='stg.ordersystem_orders',
            pg_dest=dwh_pg_connect,
            logger=log
        )
        loader.run_copy()

    @task(task_id='dm_products_load')
    def load_products():
        loader = DdsLoader(
            reader=stg_reader,
            settings_repository=settings_repo,
            save_handler=save_products_handler,
            workflow_key='products_stg_to_dds',
            stg_table_name='stg.ordersystem_orders',
            pg_dest=dwh_pg_connect,
            logger=log
        )
        loader.run_copy()

    @task(task_id='dm_orders_load')
    def load_orders():
        loader = DdsLoader(
            reader=stg_reader,
            settings_repository=settings_repo,
            save_handler=save_orders_handler,
            workflow_key='orders_stg_to_dds',
            stg_table_name='stg.ordersystem_orders',
            pg_dest=dwh_pg_connect,
            logger=log
        )
        loader.run_copy()

    @task(task_id='fct_product_sales_load')
    def load_fct_product_sales():
        loader = DdsLoader(
            reader=stg_reader,
            settings_repository=settings_repo,
            save_handler=save_fct_sales_handler,
            workflow_key='bonussystem_events_stg_to_dds',
            stg_table_name='stg.bonussystem_events',
            pg_dest=dwh_pg_connect,
            logger=log
        )
        loader.run_copy()

    load_users_task = load_users()
    load_restaurants_task = load_restaurants()
    load_timestamps_task = load_timestamps()
    load_products_task = load_products()
    load_orders_task = load_orders()
    load_fct_product_sales_task = load_fct_product_sales()

    load_users_task >> load_restaurants_task >> load_timestamps_task >> load_products_task >> load_orders_task >>load_fct_product_sales_task


stg_dds_dag = stg_to_dds_dag()
