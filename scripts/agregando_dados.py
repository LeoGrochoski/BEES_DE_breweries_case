import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import logging
from dotenv import load_dotenv
import os
from datetime import datetime

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

# Função para listar todos os arquivos Parquet no bucket S3
def listar_arquivos_parquet(bucket_name, prefix=""):
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

    # Etapa de agregacao de dados
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

    # Concatenanando todos os DataFrames apos agregacao
    try:
        df_agregado = pd.concat(agregados)
    except ValueError as e:
        logging.error(f"Erro ao concatenar DataFrames: {e}")
        return
    
    # Salvando o resultado agregado em um novo arquivo Parquet
    try:
        logging.info("Salvando resultado agregado em Parquet")
        tabela_arrow_agregada = pa.Table.from_pandas(df_agregado)
        timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        caminho_saida = os.path.join("/tmp", f"breweries_data_raw_{timestamp}_{state.replace(" ", "_")}.parquet")
        pq.write_table(tabela_arrow_agregada, caminho_saida, compression="snappy")

        logging.info("Subindo arquivo Parquet agregado para S3")
        conexao_s3.upload_file(caminho_saida, BUCKET_CURATED, "aggregated_breweries.parquet")
    
    except Exception as e:
        logging.error(f"Erro ao salvar ou enviar o arquivo Parquet agregado: {e}")
        return

    logging.info("Agregacao dos dados completa")

agregacao_dados(BUCKET_RAW, "")
