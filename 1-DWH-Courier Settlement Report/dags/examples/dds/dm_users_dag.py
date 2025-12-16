import logging

import pendulum
from airflow.decorators import dag, task
from examples.dds.dm_users_loader import DmUserLoader
from examples.dds.dm_restaurants_loader import DmRestaurantLoader
from examples.dds.dm_timestamps_loader import DmTimestampLoader
from examples.dds.dm_products_loader import DmProductLoader
from examples.dds.dm_orders_loader import DmOrderLoader
from examples.dds.fct_product_sales_loader import FctProductSaleLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_example_dds_dm_users_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_users_load")
    def load_dm_users():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmUserLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_dm_users()  # Вызываем функцию, которая перельет данные.

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_restaurants_load")
    def load_dm_restaurants():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmRestaurantLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_dm_restaurants()  # Вызываем функцию, которая перельет данные.

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_timestamps_load")
    def load_dm_timestamps():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmTimestampLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_dm_timestamps()  # Вызываем функцию, которая перельет данные.

    @task(task_id="dm_products_load")
    def load_dm_products():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmProductLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_dm_products()  # Вызываем функцию, которая перельет данные.

    @task(task_id="dm_orders_load")
    def load_dm_orders():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmOrderLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_dm_orders()  # Вызываем функцию, которая перельет данные.
    
    @task(task_id="fct_product_sales_load")
    def load_fct_product_sales():
        rest_loader = FctProductSaleLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_fct_product_sales()

    dm_users_dict = load_dm_users()
    dm_restaurants_dict = load_dm_restaurants()
    dm_timestamps_dict = load_dm_timestamps()
    dm_products_dict = load_dm_products()
    dm_orders_dict = load_dm_orders()
    fct_product_sales_dict = load_fct_product_sales()

    # Правильный порядок зависимостей:
    dm_users_dict >> [dm_restaurants_dict, dm_timestamps_dict]
    dm_restaurants_dict >> dm_products_dict
    [dm_users_dict, dm_restaurants_dict, dm_timestamps_dict] >> dm_orders_dict
    [dm_products_dict, dm_orders_dict] >> fct_product_sales_dict

dds_dm_users_dag = sprint5_example_dds_dm_users_dag()