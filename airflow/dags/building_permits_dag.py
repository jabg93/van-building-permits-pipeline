from __future__ import annotations
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
import boto3
import requests
import json
import io
import pandas as pd
from time import sleep
from datetime import datetime, timedelta
import pendulum
from airflow.exceptions import AirflowSkipException


# AWS credentials and S3 d
AWS_S3_BUCKET = Variable.get("s3_bucket_name_api")
S3_KEY_PREFIX = "van-Issued-building-permits/"

tz = pendulum.timezone("America/Vancouver")
execution_date = (datetime.now(tz) - timedelta(days=1)).strftime('%Y-%m-%d')

def get_api_data(**context):
    api_url = Variable.get("api_endpoint_van_issued_building_permits")
    limit = 100
    offset = 0
    all_results = []

    page_count = 0

    while True:
        params = {
        'where': f"issuedate = date'{execution_date}'",
        'limit': limit, 
        'offset': offset
        }
        response = requests.get(api_url, params=params)
        
        if response.status_code != 200:
            print(f"Request failed at offset: {offset}")
            break
        
        response.raise_for_status()
        data = response.json()
        records = data.get('results', [])
        
        if not records:
            break

        all_results.extend(records)
        offset += limit
        page_count += 1
        print(f"Página {page_count} obtenida, total acumulado: {len(all_results)} registros para el dia {execution_date}")
        sleep(0.2)
    
    if len(all_results) == 0:
         raise AirflowSkipException("No data available for the given execution date")
    
    context['ti'].xcom_push(key='data_array', value=all_results)

def dict_to_json(dict):
    return json.dumps(dict, ensure_ascii=False)


def save_to_s3(**context):
    ti = context['ti']
    data = ti.xcom_pull(key='data_array', task_ids='get_data')
    try:
        df = pd.DataFrame(data)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        aws_conn = BaseHook.get_connection('aws_default')
        session = boto3.session.Session(
            aws_access_key_id=aws_conn.login,
            aws_secret_access_key=aws_conn.password,
            region_name='us-east-1'
        )
        s3_client = session.client('s3')

        s3_key = f"{S3_KEY_PREFIX}daily_{execution_date}.csv"

        s3_client.put_object(
            Bucket=AWS_S3_BUCKET,
            Key=s3_key,
            Body=csv_buffer.getvalue()        
        )
    except Exception as e:
        return e
    return "Successfully uploaded data to AWS S3"


def insert_to_postgres(**context):
    ti = context['ti']
    data = ti.xcom_pull(key='data_array', task_ids='get_data')
    from sqlalchemy import create_engine
    try:
        df = pd.DataFrame(data)
        df.drop_duplicates(subset=["permitnumber"], inplace=True)

        pg_conn = BaseHook.get_connection("postgres_bronze")
        engine = create_engine(
            f"postgresql://{pg_conn.login}:{pg_conn.password}@{pg_conn.host}:{pg_conn.port}/{pg_conn.schema}"
        )

        df['geom'] = df.geom.map(dict_to_json)
        df['geo_point_2d'] = df.geo_point_2d.map(dict_to_json)

        df.to_sql(
            name="building_permits",
            con=engine,
            schema="bronze",
            index=False,
            if_exists="append",
            method="multi"
        )

    except Exception as e:\
        raise


with DAG(
    dag_id='van_data_to_s3_and_postgres',
    start_date=datetime(2024, 1, 1, tzinfo=tz),
    schedule_interval="0 7 * * *",
    catchup=False,
    tags=['elt', 'vancouver', 'building-permts']
) as dag:


    t1 = PythonOperator(
        task_id='get_data',
        python_callable=get_api_data
    )

    t2 = PythonOperator(
        task_id='save_to_s3',
        python_callable=save_to_s3
    )

    t3 = PythonOperator(
        task_id='insert_to_postgres',
        python_callable=insert_to_postgres
    )

    dbt_run = BashOperator(
        task_id='run_dbt_models',
        bash_command='~/.local/bin/dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt'
    )

    t1 >> [t2, t3]
    t3 >> dbt_run
