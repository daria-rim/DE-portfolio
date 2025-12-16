import logging

import pendulum
from airflow.decorators import dag, task
from examples.stg.bonus_system_ranks_dag.ranks_loader import RankLoader
from examples.stg.bonus_system_ranks_dag.users_loader import UserLoader
from examples.stg.bonus_system_ranks_dag.events_loader import EventLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_example_stg_bonus_system_ranks_dag():
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
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = UserLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_users()  # Вызываем функцию, которая перельет данные.

    @task(task_id="events_load")
    def load_events():
        event_loader = EventLoader(origin_pg_connect, dwh_pg_connect, log)
        event_loader.load_events()

    ranks_dict = load_ranks()
    users_dict = load_users()
    events_dict = load_events()

    ranks_dict >> users_dict >> events_dict

stg_bonus_system_ranks_dag = sprint5_example_stg_bonus_system_ranks_dag()