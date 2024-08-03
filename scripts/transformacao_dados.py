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
BUCKET_LAND = os.getenv("BUCKET_LAND")
BUCKET_RAW = os.getenv("BUCKET_RAW")

# Função de qualidade buscando arquivo no S3
def pegar_ultimo_arquivo_csv(bucket_name, prefix=""):
    conexao_s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    resposta = conexao_s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    if "Contents" not in resposta:
        raise FileNotFoundError("Nenhum arquivo encontrado no bucket")

    arquivos = resposta["Contents"]
    arquivos_csv = [arq for arq in arquivos if arq["Key"].endswith(".csv")]

    if not arquivos_csv:
        raise FileNotFoundError("Nenhum arquivo CSV encontrado no bucket")

    ultimo_arquivo = max(arquivos_csv, key=lambda x: x["LastModified"])
    return ultimo_arquivo["Key"]

def transformacao_dados(entrada_bucket, saida_bucket, prefix):
    conexao_s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    # Garantindo que o diretório /tmp exista
    if not os.path.exists("/tmp"):
        os.makedirs("/tmp")

    try:
        logging.info("Procurando o último arquivo CSV no S3")
        entrada_arq = pegar_ultimo_arquivo_csv(entrada_bucket, prefix)
        logging.info(f"Ultimo arquivo CSV encontrado: {entrada_arq}")
    except Exception as e:
        logging.error(f"Erro ao procurar o ultimo arquivo CSV: {e}")
        return
    
    try:
        logging.info("Baixando arquivo CSV da Land no S3")
        caminho_temporario = os.path.join("/tmp", "breweries.csv")
        conexao_s3.download_file(entrada_bucket, entrada_arq, caminho_temporario)
    except Exception as e:
        logging.error(f"Erro ao baixar arquivo: {e}")
        return
    
    try:
        logging.info("Lendo arquivo CSV em forma de DF")
        df = pd.read_csv(caminho_temporario)

        # Normalizando e removendo espaços em branco na coluna "state", evitando erros humanos
        df["state"] = df["state"].str.strip().str.title()
        df = df.dropna(subset=["state"])
    except Exception as e:
        logging.error(f"Erro ao ler arquivo CSV: {e}")
        return
    
    try:
        logging.info("Agrupando dados por estado")
        agrupamento = df.groupby("state")
    except Exception as e:
        logging.error(f"Erro ao agrupar dados por estado: {e}")
        return
    
    # Ralizando o agrupamento por estado para que o particionamento ocorra sem problemas de duplicidade nem de erros 
    for state, grupo in agrupamento:
        try:
            timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
            nome_saida_arq = f"breweries_data_raw_{timestamp}_{state.replace(" ", "_")}.parquet"
            caminho_saida = os.path.join("/tmp", nome_saida_arq)
            
            logging.info(f"Processando dados para o estado: {state}")
            tabela_arrow = pa.Table.from_pandas(grupo)
            
            logging.info(f"Escrevendo dados para {state} em Parquet")
            pq.write_table(tabela_arrow, caminho_saida, compression="snappy")
            
            logging.info(f"Subindo arquivo Parquet para o estado: {state}")
            s3_path = nome_saida_arq  
            conexao_s3.upload_file(caminho_saida, saida_bucket, s3_path)
        
        except Exception as e:
            logging.error(f"Erro ao processar o estado {state}: {e}")
            continue

    # Limpeza dos arquivos temporários
    try:
        logging.info("Removendo arquivos temporários")
        for root, dirs, files in os.walk("/tmp"):
            for file in files:
                os.remove(os.path.join(root, file))
    except Exception as e:
        logging.error(f"Erro ao remover arquivos temporarios: {e}")
        return

    logging.info("Transformacao dos dados completa")

transformacao_dados(BUCKET_LAND, BUCKET_RAW, "breweries_data_land_")
