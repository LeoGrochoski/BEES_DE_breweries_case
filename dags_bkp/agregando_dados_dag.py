from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import subprocess

"""
agregando_dados_dag.py

Este módulo define uma DAG (Directed Acyclic Graph) do Airflow para executar o script de agregação de dados.

A DAG é configurada para ser executada diariamente e chama o script 'agregando_dados.py'.

Principais componentes:
- execute_script: Função que executa o script Python externo.
- default_args: Argumentos padrão para a DAG.
- dag: Definição da DAG.
- agregando_dados_task: Tarefa que executa o script de agregação.

"""

def execute_script():
    """
    Executa o script 'agregando_dados.py' usando subprocess.

    Esta função é chamada pela tarefa do Airflow para realizar a agregação de dados.
    """
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
