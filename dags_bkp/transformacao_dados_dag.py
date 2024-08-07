from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

"""
transformacao_dados_dag.py

Este módulo define uma DAG (Directed Acyclic Graph) do Airflow para executar o script de transformação de dados.

A DAG é configurada para ser executada diariamente e chama o script 'transformacao_dados.py'.

Principais componentes:
- execute_script: Função que executa o script Python externo.
- default_args: Argumentos padrão para a DAG.
- dag: Definição da DAG.
- transformacao_dados_task: Tarefa que executa o script de transformação de dados.

"""

def execute_script():
    """
    Executa o script 'transformacao_dados.py' usando subprocess.

    Esta função é chamada pela tarefa do Airflow para realizar a transformação de dados.
    """
    subprocess.run(["python", "/opt/airflow/scripts/transformacao_dados.py"], check=True)

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
    python_callable=execute_script,
    dag=dag,
)
