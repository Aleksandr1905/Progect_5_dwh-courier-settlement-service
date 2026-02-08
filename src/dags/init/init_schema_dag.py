import logging
import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from lib.schema_init import SchemaDdl
from lib.pg_connect import ConnectionBuilder
log = logging.getLogger(__name__)


@dag(
    dag_id="init_schema_dag",
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2026, 2, 8, tz="UTC"),  # Дата начала выполнения дага.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['project5','schema', 'ddl'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def stg_init_schema_dag():

    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Забираем путь до каталога с SQL-файлами из переменных Airflow.
    ddl_path = Variable.get("STG_INIT_FILES_PATH")

    # Объявляем таск, который создает структуру таблиц.
    @task(task_id="schema_init")
    def schema_init():
        rest_loader = SchemaDdl(dwh_pg_connect, log)
        rest_loader.init_schema(ddl_path)

    # Инициализируем объявленные таски.
    init_schema = schema_init()

    # Задаем последовательность выполнения тасков. У нас только инициализация схемы.
    init_schema  # type: ignore


# Вызываем функцию, описывающую даг.
stg_init_schema_dag = stg_init_schema_dag()  # noqa
