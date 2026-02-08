import logging

import pendulum
from airflow.decorators import dag, task
from lib.bonus_system.ranks_loader import RankLoader
from lib.bonus_system.users_loader import UserLoader
from lib.bonus_system.outbox_loader import OutboxLoader
from lib.pg_connect import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    dag_id='stg_bonus_system_load',
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2026, 2, 8, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['stg', 'bonus_system'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def stg_bonus_system_ranks_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="ranks_load")
    def load_ranks():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_ranks()  # Вызываем функцию, которая перельет данные.

    @task(task_id="users_load")
    def load_users():
        user_loader = UserLoader(origin_pg_connect, dwh_pg_connect, log)
        user_loader.load_users()

    @task(task_id="outbox_load")
    def load_outbox():
        outbox_loader = OutboxLoader(origin_pg_connect, dwh_pg_connect, log)
        outbox_loader.load_events()

    # Инициализируем объявленные таски.
    ranks_dict = load_ranks()
    users_dict = load_users()
    outbox_dict = load_outbox()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    ranks_dict >> users_dict >>outbox_dict# type: ignore


stg_bonus_system_ranks_dag = stg_bonus_system_ranks_dag()
