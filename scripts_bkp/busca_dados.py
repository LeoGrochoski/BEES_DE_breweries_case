import requests
import pandas as pd
import boto3
import os
from dotenv import load_dotenv
from io import StringIO
from pandas import DataFrame
from typing import Dict, Any
from datetime import datetime
import logging

# Configuração de logging
logging.basicConfig(
    filename="../logs/pipeline_logs.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Trazendo as variáveis de ambiente do arquivo seguro .env
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_LAND = os.getenv("BUCKET_LAND")

logging.info("Iniciando a pipeline")

# Sessão para trazer os dados da API no formato JSON
API = "https://api.openbrewerydb.org/breweries"

def extracao_api(api_url: str) -> Dict[str, Any]:
    logging.info("Iniciando requisicao da API")
    try:
        req = requests.get(api_url, timeout=10)
        req.raise_for_status()
        logging.info("Requisicao da API realizada com sucesso!")
        return req.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro na requisiaoo da API: {e}")
        raise

# Sessão de conversão do JSON para DataFrame
def cria_dataframe(lista: list) -> DataFrame:
    logging.info("Iniciando conversao para DataFrame")
    try:
        tabela: DataFrame = pd.DataFrame(lista)
        logging.info("Dados convertidos para DataFrame com sucesso!")
        return tabela
    except Exception as e:
        logging.error(f"Erro na conversao para DataFrame: {e}")
        raise

# Sessão de ingestão na land
def salvando_s3(df: pd.DataFrame, bucket: str, key: str):
    logging.info("Convertendo DataFrame para CSV")
    try:
        csv_dados = StringIO()
        df.to_csv(csv_dados, index=False)
        logging.info("CSV criado com sucesso!")
    except Exception as e:
        logging.error(f"Erro na conversao para CSV: {e}")
        raise

    logging.info("Iniciando conexao com S3")
    try:
        conexao_s3 = boto3.resource(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        logging.info("Conexao com S3 realizada com sucesso!")
        
        conexao_s3.Object(bucket, key).put(Body=csv_dados.getvalue())
        logging.info("Dados salvos no S3 com sucesso!")
    except boto3.exceptions.Boto3Error as e:
        logging.error(f"Erro ao salvar dados no S3: {e}")
        raise
    
    logging.info("Extracao dos dados completa")

if __name__ == "__main__":
        dados = extracao_api(API)
        df = cria_dataframe(dados)
        nome_arquivo = f"breweries_data_land_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
        salvando_s3(df, BUCKET_LAND, nome_arquivo)