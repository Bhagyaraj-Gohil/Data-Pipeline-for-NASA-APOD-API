from airflow import DAG
from datetime import timedelta
from datetime import datetime
from airflow.operators.python import PythonOperator
from APOD_ETL import run_APOD_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 25),    #Replace it with your desired start date
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='APOD_dag',
    default_args=default_args,
    description="DAG for running ETL process on NASA's APOD Open API",
    schedule_interval=timedelta(days=1)
)

run_etl = PythonOperator(
    task_id='APOD_etl',
    python_callable=run_APOD_etl,
    dag=dag
)

run_etl