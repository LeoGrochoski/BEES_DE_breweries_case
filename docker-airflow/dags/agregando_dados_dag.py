from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import subprocess


def execute_script():
    subprocess.run(["python", "/opt/airflow/scripts/agregando_dados.py"], check=True)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 1),
    'retries': 1,
}

dag = DAG(
    'agregando_dados_dag',
    default_args=default_args,
    schedule_interval='@daily',
)

agregando_dados_task = PythonOperator(
    task_id='agregando_dados_task',
    python_callable=execute_script,
    dag=dag,
)
