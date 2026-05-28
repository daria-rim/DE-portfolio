from airflow import DAG
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.vertica.operators.vertica import VerticaOperator
from datetime import datetime
import boto3
import vertica_python
import pandas as pd
import os


# Конфигурация
AWS_ACCESS_KEY_ID = ""
AWS_SECRET_ACCESS_KEY = ""
BUCKET_NAME = 'sprint6'
FILES_TO_DOWNLOAD = ['users.csv', 'groups.csv', 'dialogs.csv', 'group_log.csv']
DOWNLOAD_PATH = '/data/'
SCHEMA_NAME = 'STV202507313__STAGING'

VERTICA_CONN = {
    'host': 'vertica.tgcloudenv.ru',
    'port': 5433,
    'user': 'stv202507313',
    'password': '',
    'database': 'dwh',
    'autocommit': True
}

def fetch_s3_file(bucket: str, key: str):
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=f'{DOWNLOAD_PATH}{key}'
    )

def process_csv_with_correct_types(table_name: str):
    """
    Обрабатывает CSV файл, устанавливая правильные типы данных для столбцов,
    которые могут содержать пустые значения и удаляет дубликаты
    """
    file_path = f'{DOWNLOAD_PATH}{table_name}.csv'
    
    # Читаем CSV с указанием правильных типов данных
    if table_name == 'users':
        dtype_mapping = {
            'id': 'Int64',
            'chat_name': 'str',
            'registration_date': 'str',
            'country': 'str',
            'age': 'Int64'
        }
    elif table_name == 'groups':
        dtype_mapping = {
            'id': 'Int64',
            'admin_id': 'Int64',
            'group_name': 'str',
            'registration_date': 'str',
            'is_private': 'boolean'
        }
    elif table_name == 'dialogs':
        dtype_mapping = {
            'message_id': 'Int64',
            'message_ts': 'str',
            'message_from': 'Int64',
            'message_to': 'Int64',
            'message': 'str',
            'message_group': 'Int64'
        }
    elif table_name == 'group_log':
        dtype_mapping = {
            'group_id': 'Int64',
            'user_id': 'Int64',
            'user_id_from': 'Int64',
            'event': 'str',
            'datetime': 'str'
        }
    else:
        dtype_mapping = None
    
    # Читаем CSV файл с правильными типами данных
    df = pd.read_csv(file_path, dtype=dtype_mapping)
    
    # Удаляем дубликаты из CSV перед загрузкой
    if table_name == 'users':
        df = df.drop_duplicates(subset=['id'])
    elif table_name == 'groups':
        df = df.drop_duplicates(subset=['id'])
    elif table_name == 'dialogs':
        df = df.drop_duplicates(subset=['message_id'])
    elif table_name == 'group_log':
        # Для логов используем комбинацию полей для определения уникальности
        df = df.drop_duplicates(subset=['group_id', 'user_id', 'event', 'datetime'])
    
    # Сохраняем обратно с правильными типами и без дубликатов
    df.to_csv(file_path, index=False)
    print(f"Processed {table_name} with correct data types, removed duplicates")

def load_data_to_vertica(table_name: str):
    # Обрабатываем CSV для установки правильных типов данных и удаления дубликатов
    process_csv_with_correct_types(table_name)
    
    conn = vertica_python.connect(**VERTICA_CONN)
    cursor = conn.cursor()
    try:
        # Загружаем данные
        cursor.execute(f"""
            COPY {SCHEMA_NAME}.{table_name}
            FROM LOCAL '{DOWNLOAD_PATH}{table_name}.csv'
            DELIMITER ','
            SKIP 1
            REJECTED DATA AS TABLE {SCHEMA_NAME}.{table_name}_rej
        """)
        print(f"Successfully loaded {table_name} to Vertica")
        
    except Exception as e:
        print(f"Error loading {table_name}: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()


@dag(
    schedule_interval=None,
    start_date=datetime(2022, 7, 13),
    catchup=False,
    tags=['s3', 'vertica', 'staging']
)
def sprint6_dag_load_staging():
    download_tasks = []
    for filename in FILES_TO_DOWNLOAD:
        task = PythonOperator(
            task_id=f'download_{filename}',
            python_callable=fetch_s3_file,
            op_kwargs={
                'bucket': BUCKET_NAME,
                'key': filename
            },
        )
        download_tasks.append(task)

    load_users = PythonOperator(
        task_id='load_users',
        python_callable=load_data_to_vertica,
        op_kwargs={'table_name': 'users'}
    )
    
    load_groups = PythonOperator(
        task_id='load_groups',
        python_callable=load_data_to_vertica,
        op_kwargs={'table_name': 'groups'}
    )
    
    load_dialogs = PythonOperator(
        task_id='load_dialogs',
        python_callable=load_data_to_vertica,
        op_kwargs={'table_name': 'dialogs'}
    )

    load_group_log = PythonOperator(
        task_id='load_group_log',
        python_callable=load_data_to_vertica,
        op_kwargs={'table_name': 'group_log'}
    )

    # Правильно устанавливаем зависимости
    for download_task in download_tasks:
        download_task >> load_users
        download_task >> load_groups
        download_task >> load_dialogs
        download_task >> load_group_log


sprint6_dag = sprint6_dag_load_staging()
