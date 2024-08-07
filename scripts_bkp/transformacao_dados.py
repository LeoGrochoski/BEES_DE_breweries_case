import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import logging
from dotenv import load_dotenv
import os
from datetime import datetime
from typing import List

"""
transformacao_dados.py

Este módulo é responsável por transformar dados de cervejarias armazenados em arquivos CSV no S3.
Ele lê o arquivo CSV mais recente, realiza transformações nos dados, e salva os resultados
como arquivos Parquet separados por estado no bucket raw do S3.

Funções principais:
- listar_arquivos_csv: Lista arquivos CSV em um bucket S3.
- baixar_arquivo_csv: Baixa um arquivo CSV do S3.
- transformar_dados: Realiza transformações nos dados.
- salvar_dados_parquet: Salva dados transformados como arquivo Parquet no S3.
- transformacao_dados: Função principal que orquestra o processo de transformação.

O módulo utiliza as bibliotecas pandas e pyarrow para processamento de dados,
e boto3 para interação com o Amazon S3.
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
BUCKET_LAND = os.getenv("BUCKET_LAND")
BUCKET_RAW = os.getenv("BUCKET_RAW")

# Função para listar arquivos CSV no S3
def listar_arquivos_csv(bucket_name: str, prefix: str = "") -> List[str]:
    """
    Lista todos os arquivos CSV em um bucket S3 específico.

    Args:
        bucket_name (str): Nome do bucket S3.
        prefix (str, opcional): Prefixo para filtrar os arquivos. Padrão é "".

    Returns:
        List[str]: Lista de chaves (nomes) dos arquivos CSV encontrados.

    Raises:
        FileNotFoundError: Se nenhum arquivo CSV for encontrado no bucket.
    """
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
    """
    Baixa um arquivo CSV específico de um bucket S3.

    Args:
        bucket_name (str): Nome do bucket S3.
        arquivo (str): Chave (nome) do arquivo a ser baixado.

    Returns:
        str: Caminho local do arquivo baixado.
    """
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
    """
    Lê um arquivo CSV e realiza transformações nos dados.

    Transformações realizadas:
    - Normaliza os nomes dos estados (strip e title case).
    - Remove linhas com valores nulos na coluna 'state'.

    Args:
        caminho_csv (str): Caminho local do arquivo CSV.

    Returns:
        pd.DataFrame: DataFrame com os dados transformados.
    """
    df = pd.read_csv(caminho_csv)
    df["state"] = df["state"].str.strip().str.title()
    df = df.dropna(subset=["state"])
    return df

# Função para salvar dados transformados em Parquet
def salvar_dados_parquet(df: pd.DataFrame, state: str, bucket_name: str):
    """
    Salva um DataFrame como arquivo Parquet no S3, separado por estado.

    Args:
        df (pd.DataFrame): DataFrame a ser salvo.
        state (str): Nome do estado para o qual os dados estão sendo salvos.
        bucket_name (str): Nome do bucket S3 onde o arquivo será salvo.
    """
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
    """
    Função principal que orquestra o processo de transformação dos dados.

    Esta função realiza as seguintes operações:
    1. Lista e seleciona o arquivo CSV mais recente no bucket de entrada.
    2. Baixa o arquivo CSV selecionado.
    3. Transforma os dados.
    4. Agrupa os dados por estado.
    5. Salva os dados transformados como arquivos Parquet separados por estado no bucket de saída.

    Args:
        entrada_bucket (str): Nome do bucket S3 contendo os arquivos CSV de origem.
        saida_bucket (str): Nome do bucket S3 onde os arquivos Parquet serão salvos.
        prefix (str): Prefixo para filtrar os arquivos CSV no bucket de entrada.
    """
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
