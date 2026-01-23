import time
import requests
import json
import pandas as pd

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook

# Параметры подключения
postgres_conn_id = 'postgresql_de'
nickname = 'unbelievable'
cohort = '45'

# Конфигурация HTTP подключений (generate_report, get_report, get_increment, upload_data_to_staging)
HTTP_TIMEOUT = 30
MAX_RETRIES = 3
TIME_TILL_RETRY = 5



# Конфигурация polling для получения отчета (get_report)
MAX_POLLING_TIME = 300
POLLING_INTERVAL = 10

# Расписание выполнения
DAG_START_DATE = datetime(2026, 1, 16)
DAG_END_DATE = datetime(2026, 1, 23)
DAG_CATCHUP = True
DAG_SCHEDULE_INTERVAL = '0 1 * * *'


def generate_report(ti):

    http_conn_id = HttpHook.get_connection('http_conn_id')

    base_url = http_conn_id.host
    api_key = http_conn_id.extra_dejson.get('api_key')
    headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
    }

    print('Making request generate_report')

    for conn_attempt in range(MAX_RETRIES):
        try:
            response = requests.post(f'{base_url}/generate_report', headers=headers, timeout=HTTP_TIMEOUT)
            response.raise_for_status()
            break
        except requests.exceptions.RequestException as e:
            print(f'Attempt {conn_attempt + 1}/{MAX_RETRIES} failed: {e}')
            if conn_attempt == MAX_RETRIES - 1:
                raise
            time.sleep(TIME_TILL_RETRY)
        

    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    print(f'Response is {response.content}')


def get_report(ti):

    http_conn_id = HttpHook.get_connection('http_conn_id')

    base_url = http_conn_id.host
    api_key = http_conn_id.extra_dejson.get('api_key')
    headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
    }

    task_id = ti.xcom_pull(key='task_id', task_ids='generate_report')
    
    start_time = time.time()

    while time.time() - start_time < MAX_POLLING_TIME:

        for attempt in range(MAX_RETRIES):
            try:
                print(f'Making request get_report attempt №{attempt + 1}')
                response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers, timeout=HTTP_TIMEOUT)
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                print(f'Request failed on attempt №{attempt + 1}/{MAX_RETRIES}: {e}')
                if attempt == MAX_RETRIES - 1:
                    raise
                time.sleep(TIME_TILL_RETRY)
                

        print(f'Response is {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            ti.xcom_push(key='report_id', value=report_id)
            print(f'Report_id={report_id}')
            return  
        
        time.sleep(POLLING_INTERVAL)

    raise TimeoutError()


def get_increment(date, ti):

    http_conn_id = HttpHook.get_connection('http_conn_id')

    base_url = http_conn_id.host
    api_key = http_conn_id.extra_dejson.get('api_key')
    headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    print('Making request get_increment')
    report_id = ti.xcom_pull(key='report_id')
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(
                f'{base_url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
                headers=headers,
                timeout=HTTP_TIMEOUT
            )
            response.raise_for_status()
            break
        except requests.exceptions.RequestException as e:
            print(f'Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}')
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(TIME_TILL_RETRY)
    
    print(f'Response is {response.content}')

    increment_id = json.loads(response.content)['data']['increment_id']
    if not increment_id:
        raise ValueError(f'Increment is empty. Most probably due to error in API call.')
    
    ti.xcom_push(key='increment_id', value=increment_id)
    print(f'increment_id={increment_id}')


def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    increment_id = ti.xcom_pull(key='increment_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{increment_id}/{filename}'
    print(s3_filename)
    local_filename = date.replace('-', '') + '_' + filename
    print(local_filename)
    response = requests.get(s3_filename, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    open(f"{local_filename}", "wb").write(response.content)
    print(response.content)

    df = pd.read_csv(local_filename)
    df=df.drop('id', axis=1)
    df=df.drop_duplicates(subset=['uniq_id'])

    if 'status' not in df.columns:
        df['status'] = 'shipped'

    
    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()

    with engine.begin() as transaction:
        print(f"Deleting {date} data")
        delete_sql = f"""
        DELETE FROM {pg_schema}.{pg_table} 
        WHERE date_time::Date = '{date}'
        """

        del_result = transaction.execute(delete_sql)
        deleted_rows = del_result.rowcount
        print(f"Deleted {deleted_rows} rows for date {date} in {pg_schema}.{pg_table}") 

       
        try:
            row_count = df.to_sql(pg_table, transaction, schema=pg_schema, if_exists='append', index=False, method='multi', chunksize=1000)
            print(f'{row_count} rows was inserted')
        except Exception as e:
            print(f'Insert failed: {e}')
            raise



args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

business_dt = '{{ ds }}'

with DAG(
        'sp3_proj_main_DAG_refactored',
        default_args=args,
        description='Provide default dag for sprint3',
        catchup=DAG_CATCHUP,
        start_date=DAG_START_DATE,
        end_date=DAG_END_DATE,
        schedule_interval=DAG_SCHEDULE_INTERVAL,
) as dag:
    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report)

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report)

    get_increment = PythonOperator(
        task_id='get_increment',
        python_callable=get_increment,
        op_kwargs={'date': business_dt})

    upload_user_order_inc = PythonOperator(
        task_id='upload_user_order_inc',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'user_order_log_inc.csv',
                   'pg_table': 'user_order_log',
                   'pg_schema': 'staging'})

    update_d_item_table = PostgresOperator(
        task_id='update_d_item',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_item.sql")

    update_d_customer_table = PostgresOperator(
        task_id='update_d_customer',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_customer.sql")

    update_d_city_table = PostgresOperator(
        task_id='update_d_city',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_city.sql")

    update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_sales_v2.sql",
        parameters={"date": {business_dt}})

    update_f_customer_retention = PostgresOperator(
        task_id='update_f_customer_retention',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_customer_retention.sql")
    

    (
            generate_report
            >> get_report
            >> get_increment
            >> upload_user_order_inc
            >> [update_d_item_table, update_d_city_table, update_d_customer_table]
            >> update_f_sales
            >> update_f_customer_retention
    )
