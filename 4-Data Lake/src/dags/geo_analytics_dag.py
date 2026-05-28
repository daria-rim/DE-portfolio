from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'dariarim',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Создаем DAG для геоаналитики
dag = DAG(
    'geo_analytics',
    default_args=default_args,
    description='Geo analytics data processing',
    schedule_interval='0 2 * * *',
    catchup=False
)

# Задача для создания витрины пользователей
user_analytics_task = BashOperator(
    task_id='user_analytics',
    bash_command='spark-submit /home/dariarim/scripts/user_analytics.py',
    dag=dag
)

# Задача для создания витрины географических зон
zone_analytics_task = BashOperator(
    task_id='zone_analytics',
    bash_command='spark-submit /home/dariarim/scripts/zone_analytics.py',
    dag=dag
)

# Задача для создания рекомендаций друзей
friend_recommendations_task = SparkSubmitOperator(
    task_id='friend_recommendations',
    application='/home/dariarim/scripts/friend_recommendations.py',
    conn_id='spark_default',
    dag=dag
)

# Задачи выполняются параллельно
user_analytics_task
zone_analytics_task
friend_recommendations_task