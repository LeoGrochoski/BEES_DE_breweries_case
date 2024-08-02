import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import logging
from dotenv import load_dotenv
import os

# Configuração de logging
logging.basicConfig(
    filename="../logs/pipeline_logs.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Trazendo as variáveis de ambiente do arquivo seguro .env
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
BUCKET_LAND = os.getenv('BUCKET_LAND')
BUCKET_RAW = os.getenv('BUCKET_RAW')

# Função de qualidade buscando arquivo no S3
def pegar_ultimo_arquivo_csv(bucket_name, prefix=''):
    conexao_s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    resposta = conexao_s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    if 'Contents' not in resposta:
        raise FileNotFoundError("Nenhum arquivo encontrado no bucket")

    arquivos = resposta['Contents']
    arquivos_csv = [arq for arq in arquivos if arq['Key'].endswith('.csv')]

    if not arquivos_csv:
        raise FileNotFoundError("Nenhum arquivo CSV encontrado no bucket")

    ultimo_arquivo = max(arquivos_csv, key=lambda x: x['LastModified'])
    return ultimo_arquivo['Key']

def transformacao_dados(entrada_bucket, saida_bucket, prefix):
    conexao_s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    # Garantindo que o diretório temporário exista
    if not os.path.exists('/tmp'):
        os.makedirs('/tmp')

    try:
        logging.info("Procurando o último arquivo CSV no S3")
        entrada_arq = pegar_ultimo_arquivo_csv(entrada_bucket, prefix)
        logging.info(f"Ultimo arquivo CSV encontrado: {entrada_arq}")
    except Exception as e:
        logging.error(f"Erro ao procurar o último arquivo CSV: {e}")
        return
    
    try:
        logging.info("Baixando arquivo CSV da Land no S3")
        caminho_temporario = os.path.join('/tmp', 'breweries.csv')
        conexao_s3.download_file(entrada_bucket, entrada_arq, caminho_temporario)
    except Exception as e:
        logging.error(f"Erro ao baixar arquivo: {e}")
        return
    
    try:
        logging.info("Lendo arquivo CSV em forma de DF")
        df = pd.read_csv(caminho_temporario)
    except Exception as e:
        logging.error(f"Erro ao ler arquivo CSV: {e}")
        return
    
    try:
        logging.info("Convertendo DataFrame para uma tabela em formato Apache Arrow")
        tabela_arrow = pa.Table.from_pandas(df)
    except Exception as e:
        logging.error(f"Erro convertendo DataFrame para tabela Arrow: {e}")
        return
    
    try:
        logging.info("Escrevendo Tabela Arrow em Parquet com particionamento")
        nome_saida_arq = entrada_arq.replace('.csv', '')
        caminho_saida = os.path.join('/tmp', nome_saida_arq)
        pq.write_to_dataset(tabela_arrow, root_path=caminho_saida, partition_cols=['state'])
    except Exception as e:
        logging.error(f"Erro ao escrever arquivo Parquet: {e}")
        return
    
    try:
        logging.info("Subindo Parquet particionado para S3")
        for root, _, files in os.walk(caminho_saida):
            for file in files:
                if file.endswith(".parquet"):
                    caminho_completo = os.path.join(root, file)
                    s3_path = os.path.relpath(caminho_completo, '/tmp')
                    conexao_s3.upload_file(caminho_completo, saida_bucket, s3_path)
    except Exception as e:
        logging.error(f"Erro ao enviar Parquet para S3: {e}")
        return

    logging.info("Transformacao dos dados completa")

# Exemplo de chamada da função
transformacao_dados(BUCKET_LAND, BUCKET_RAW, 'breweries_data_land_')
