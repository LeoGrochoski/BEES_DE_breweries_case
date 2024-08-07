from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from scripts.busca_dados import main as busca_dados

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 1),
    'retries': 1,
}

dag = DAG(
    'busca_dados_dag',
    default_args=default_args,
    schedule_interval='@daily',
)

busca_dados_task = PythonOperator(
    task_id='busca_dados_task',
    python_callable=busca_dados,
    dag=dag,
)
