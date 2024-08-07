import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import logging
from dotenv import load_dotenv
import os
from datetime import datetime

"""
agregando_dados.py

Este módulo é responsável por agregar dados de cervejarias armazenados em arquivos Parquet no S3.
Ele lê os arquivos, realiza a agregação por estado e tipo de cervejaria, e salva o resultado
em um novo arquivo Parquet no bucket curado do S3.

Funções principais:
- listar_arquivos_parquet: Lista arquivos Parquet em um bucket S3.
- agregacao_dados: Realiza a agregação dos dados e salva o resultado.

O módulo utiliza as bibliotecas pandas, pyarrow, e boto3 para processamento de dados e interação com o S3.
"""

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
BUCKET_RAW = os.getenv("BUCKET_RAW")
BUCKET_CURATED = os.getenv("BUCKET_CURATED")

def listar_arquivos_parquet(bucket_name, prefix=""):
    """
    Lista todos os arquivos Parquet em um bucket S3 específico.

    Args:
        bucket_name (str): Nome do bucket S3.
        prefix (str, opcional): Prefixo para filtrar os arquivos. Padrão é "".

    Returns:
        list: Lista de chaves (nomes) dos arquivos Parquet encontrados.
    """
    conexao_s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    resposta = conexao_s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    if "Contents" not in resposta:
        return []
    
    arquivos = resposta["Contents"]
    arquivos_parquet = [arq["Key"] for arq in arquivos if arq["Key"].endswith(".parquet")]
    return arquivos_parquet

def agregacao_dados(bucket_name, prefix):
    """
    Agrega dados de cervejarias de múltiplos arquivos Parquet no S3.

    Esta função realiza as seguintes operações:
    1. Lista arquivos Parquet no bucket S3 especificado.
    2. Baixa e lê cada arquivo Parquet.
    3. Agrega os dados por estado e tipo de cervejaria.
    4. Combina os resultados de todos os arquivos.
    5. Salva o resultado agregado em um novo arquivo Parquet no bucket curado.

    Args:
        bucket_name (str): Nome do bucket S3 contendo os arquivos de origem.
        prefix (str): Prefixo para filtrar os arquivos no bucket.

    Raises:
        Exception: Se ocorrer um erro durante o processamento ou upload do arquivo.
    """
    conexao_s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    # Listando parquets
    arquivos_parquet = listar_arquivos_parquet(bucket_name, prefix)
    if not arquivos_parquet:
        logging.error("Nenhum arquivo Parquet encontrado no bucket")
        return

    agregados = []

    for arquivo in arquivos_parquet:
        try:
            logging.info(f"Baixando arquivo Parquet: {arquivo}")
            caminho_temporario = os.path.join("/tmp", os.path.basename(arquivo))
            conexao_s3.download_file(bucket_name, arquivo, caminho_temporario)
            
            logging.info("Lendo arquivo Parquet em forma de DF")
            tabela_arrow = pq.read_table(caminho_temporario)
            df = tabela_arrow.to_pandas()

            logging.info("Agregando dados")
            agrupamento = df.groupby(["state", "brewery_type"]).size().reset_index(name="count")
            agregados.append(agrupamento)
            
        except Exception as e:
            logging.error(f"Erro ao processar o arquivo {arquivo}: {e}")
            continue

    # Concatenando todos os DataFrames após agregação
    try:
        df_agregado = pd.concat(agregados)
    except ValueError as e:
        logging.error(f"Erro ao concatenar DataFrames: {e}")
        return
    
    try:
        logging.info("Salvando resultado agregado em Parquet")
        tabela_arrow_agregada = pa.Table.from_pandas(df_agregado)
        timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        caminho_saida = os.path.join("/tmp", f"breweries_data_raw_{timestamp}.parquet")

        pq.write_table(tabela_arrow_agregada, caminho_saida, compression="snappy")

        logging.info("Subindo arquivo Parquet agregado para S3")
        conexao_s3.upload_file(caminho_saida, BUCKET_CURATED, "aggregated_breweries.parquet")
    
    except Exception as e:
        logging.error(f"Erro ao salvar ou enviar o arquivo Parquet agregado: {e}")
        return

    logging.info("Agregação dos dados completa")

# Chamada da função agregacao_dados
if __name__ == "__main__":
    agregacao_dados(BUCKET_RAW, "")
