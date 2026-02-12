import logging
import pendulum
from airflow.decorators import dag, task
from lib.pg_connect import ConnectionBuilder
from lib.cdm.CdmRepository import CdmRepository

log = logging.getLogger(__name__)

dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

@dag(
    dag_id="courier_ledger",
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2026, 2, 4, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['cdm'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def courier_ledger_dag():


    @task(task_id='cdm_courier_ledger_load')
    def load_cdm_courier_ledger():
        repo = CdmRepository()
        with dwh_pg_connect.connection() as conn:
            repo.load_cdm_courier_ledger(conn)



    load_cdm_courier_ledger_task = load_cdm_courier_ledger()

    load_cdm_courier_ledger_task


load_cdm_courier_ledger = courier_ledger_dag()
