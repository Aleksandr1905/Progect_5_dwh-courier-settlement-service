import logging
import pendulum


from airflow.decorators import dag, task
from airflow.models import Variable


from lib.courier_system.courier_reader import CourierReader
from lib.pg_connect import ConnectionBuilder

log = logging.getLogger(__name__)

@dag(
    dag_id='stg_courier_system_load',
    schedule_interval='0 0 * * *',
    start_date=pendulum.datetime(2026, 2, 10),
    catchup=False,
    tags=['courier', 'stg', 'api'],
    is_paused_upon_creation=True
)
def stg_courier_system_load():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task
    def load_couriers():
        reader = CourierReader(
            nickname=Variable.get("NICKNAME"),
            cohort=Variable.get("COHORT"),
            api_key=Variable.get("COURIER_API_KEY")
        )

        data = reader.load_everything('couriers', {'sort_field':'id'})
        log.info(f"Найдено {len(data)} курьеров")

        reader.save_to_stg(conn=dwh_pg_connect, table_name='stg.api_couriers', data=data)

    @task
    def load_deliveries(ds=None, **kwargs):

        from_date_raw = kwargs.get('macros').ds_add(ds, -7)
        date = f"{from_date_raw} 00:00:00"
        log.info(f"Jinja рассчитала дату начала: {date}")

        reader = CourierReader(
            nickname=Variable.get("NICKNAME"),
            cohort=Variable.get("COHORT"),
            api_key=Variable.get("COURIER_API_KEY")
        )

        data = reader.load_everything('deliveries', {'from': date})
        log.info(f"Найдено {len(data)} курьеров")

        reader.save_to_stg(conn=dwh_pg_connect, table_name='stg.api_deliveries', data=data)

    load_couriers() >> load_deliveries()

dag_obj = stg_courier_system_load()