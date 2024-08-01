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

logging.basicConfig(
    filename = "../log/pipeline_logs.log", 
    level = logging.INFO,
    format = "%(asctime)s - %(levelname)s - %(message)s"
    )

# Trazendo as variaves de ambiente do arquivo seguro .env

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')

logging.info("Iniciando a pipeline")

# Sessão para trazer os dados da API no formato JSON

API = 'https://api.openbrewerydb.org/breweries'

def extracao_api(api_url: str) -> Dict[str, Any]:
    logging.info("Inicio da requisicao na API")
    def requisicao_api(url: str) -> Dict[str, Any]:
        logging.info()
        req = requests.get(url, timeout=10)
        req.raise_for_status()  
        return req.json()
    
    return requisicao_api(api_url)


# Sesão de conversão do JSON para Dataframe

def cria_dataframe(lista: list) -> DataFrame:
    tabela: DataFrame = pd.DataFrame(lista)
    return tabela
        
# Sessão de ingestão na land

def salvando_s3(df: pd.DataFrame, bucket: str, key: str):
    csv_dados = StringIO()
    df.to_csv(csv_dados, index=False)
    
    conexao_s3 = boto3.resource(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    
    conexao_s3.Object(bucket, key).put(Body=csv_dados.getvalue())


if __name__ == "__main__":
    dados = extracao_api(API)
    df = cria_dataframe(dados)
    nome_arquivo = f"breweries_data_land_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    salvando_s3(df, BUCKET_NAME, nome_arquivo)