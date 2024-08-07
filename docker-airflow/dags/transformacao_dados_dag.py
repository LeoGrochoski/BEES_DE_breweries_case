from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from scripts.transformacao_dados import main as transformacao_dados

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 1),
    'retries': 1,
}

dag = DAG(
    'transformacao_dados_dag',
    default_args=default_args,
    schedule_interval='@daily',
)

transformacao_dados_task = PythonOperator(
    task_id='transformacao_dados_task',
    python_callable=transformacao_dados,
    dag=dag,
)
