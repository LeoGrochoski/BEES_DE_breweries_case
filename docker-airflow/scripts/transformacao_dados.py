import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import logging
from dotenv import load_dotenv
import os
from datetime import datetime
from typing import List

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
BUCKET_RAW = os.getenv("BUCKET_RAW")

# Função para listar arquivos CSV no S3
def listar_arquivos_csv(bucket_name: str, prefix: str = "") -> List[str]:
    conexao_s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    resposta = conexao_s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    if "Contents" not in resposta:
        raise FileNotFoundError("Nenhum arquivo encontrado no bucket")

    arquivos = resposta["Contents"]
    arquivos_csv = [arq["Key"] for arq in arquivos if arq["Key"].endswith(".csv")]

    if not arquivos_csv:
        raise FileNotFoundError("Nenhum arquivo CSV encontrado no bucket")

    return arquivos_csv

# Função para baixar o arquivo CSV do S3
def baixar_arquivo_csv(bucket_name: str, arquivo: str) -> str:
    conexao_s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    caminho_temporario = os.path.join("/tmp", os.path.basename(arquivo))
    conexao_s3.download_file(bucket_name, arquivo, caminho_temporario)
    return caminho_temporario

# Função para transformar os dados
def transformar_dados(caminho_csv: str) -> pd.DataFrame:
    df = pd.read_csv(caminho_csv)
    df["state"] = df["state"].str.strip().str.title()
    df = df.dropna(subset=["state"])
    return df

# Função para salvar dados transformados em Parquet
def salvar_dados_parquet(df: pd.DataFrame, state: str, bucket_name: str):
    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    nome_saida_arq = f"breweries_data_raw_{timestamp}_{state.replace(' ', '_')}.parquet"
    caminho_saida = os.path.join("/tmp", nome_saida_arq)
    
    tabela_arrow = pa.Table.from_pandas(df)
    pq.write_table(tabela_arrow, caminho_saida, compression="snappy")
    
    conexao_s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    conexao_s3.upload_file(caminho_saida, bucket_name, nome_saida_arq)

# Função principal de transformação
def transformacao_dados(entrada_bucket: str, saida_bucket: str, prefix: str):
    logging.info("Procurando o último arquivo CSV no S3")
    try:
        arquivos_csv = listar_arquivos_csv(entrada_bucket, prefix)
        ultimo_arquivo = max(arquivos_csv, key=lambda x: x.split('_')[-1])
        logging.info(f"Último arquivo CSV encontrado: {ultimo_arquivo}")
    except Exception as e:
        logging.error(f"Erro ao procurar o último arquivo CSV: {e}")
        return
    
    try:
        logging.info("Baixando arquivo CSV do S3")
        caminho_csv = baixar_arquivo_csv(entrada_bucket, ultimo_arquivo)
    except Exception as e:
        logging.error(f"Erro ao baixar arquivo CSV: {e}")
        return
    
    try:
        logging.info("Transformando dados")
        df = transformar_dados(caminho_csv)
        agrupamento = df.groupby("state")
    except Exception as e:
        logging.error(f"Erro ao transformar dados: {e}")
        return
    
    for state, grupo in agrupamento:
        try:
            logging.info(f"Salvando dados para o estado: {state}")
            salvar_dados_parquet(grupo, state, saida_bucket)
        except Exception as e:
            logging.error(f"Erro ao processar o estado {state}: {e}")
            continue

    logging.info("Transformação dos dados completa")

if __name__ == "__main__":
    transformacao_dados(BUCKET_LAND, BUCKET_RAW, "breweries_data_land_")
