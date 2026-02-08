import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from lib.pg_saver import PgSaver
from lib.order_system.OrderSystemLoader import OrderSystemLoader
from lib.order_system.restaurant_reader import RestaurantReader
from lib.order_system.order_reader import OrderReader
from lib.order_system.user_reader import UserReader
from lib.pg_connect import ConnectionBuilder
from lib.mongo_connect import  MongoConnect

log = logging.getLogger(__name__)


@dag(
    dag_id='stg_order_system_load',
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2026, 2, 8, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['stg', 'order', 'mongo'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def stg_order_system():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Получаем переменные из Airflow.
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task(task_id="order_system_restaurants")
    def load_restaurants():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = OrderSystemLoader(
            reader = RestaurantReader(mongo_connect),
            pg_dest = dwh_pg_connect,
            pg_saver = pg_saver,
            logger = log,
            workflow_key = 'restaurants_to_stg',
            table_name = 'stg.ordersystem_restaurants')

        # Запускаем копирование данных.
        loader.run_copy()

    @task(task_id="order_system_user")
    def load_users():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = OrderSystemLoader(
            reader=UserReader(mongo_connect),
            pg_dest=dwh_pg_connect,
            pg_saver=pg_saver,
            logger=log,
            workflow_key='users_to_stg',
            table_name='stg.ordersystem_users')

        # Запускаем копирование данных.
        loader.run_copy()

    @task(task_id="order_system_orders")
    def load_orders():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = OrderSystemLoader(
            reader=OrderReader(mongo_connect),
            pg_dest=dwh_pg_connect,
            pg_saver=pg_saver,
            logger=log,
            workflow_key='orders_to_stg',
            table_name='stg.ordersystem_orders')

        # Запускаем копирование данных.
        loader.run_copy()

    restaurant_loader = load_restaurants()
    users_loader = load_users()
    orders_loader = load_orders()

    # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
    restaurant_loader >> users_loader >> orders_loader # type: ignore


order_stg_dag = stg_order_system()  # noqa
