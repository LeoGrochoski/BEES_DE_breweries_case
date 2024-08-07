import streamlit as st
import pandas as pd
import pyarrow.parquet as pq
import boto3
import os
from dotenv import load_dotenv
import plotly.express as px

# Carregar variáveis de ambiente
load_dotenv()

def baixar_arquivo_parquet(bucket_name: str, file_key: str, local_path: str):
    s3 = boto3.client('s3',
                      aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                      aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))
    s3.download_file(bucket_name, file_key, local_path)

def carregar_dados(local_path: str) -> pd.DataFrame:
    table = pq.read_table(local_path)
    return table.to_pandas()

def main():
    st.title("Dashboard de Cervejarias")

    # Carregar variáveis de ambiente
    BUCKET_CURATED = os.getenv('BUCKET_CURATED')
    TEMP_DIR = os.getenv('TEMP_DIR')
    FILE_KEY = 'aggregated_breweries.parquet'

    # Verificar se as variáveis de ambiente estão configuradas corretamente
    if not BUCKET_CURATED:
        st.error("A variável de ambiente BUCKET_CURATED não está configurada.")
        return
    
    if not TEMP_DIR:
        st.error("A variável de ambiente TEMP_DIR não está configurada.")
        return

    # Definir o LOCAL_PATH para o diretório temporário
    LOCAL_PATH = os.path.join(TEMP_DIR, 'aggregated_breweries.parquet')

    st.write(f"BUCKET_CURATED: {BUCKET_CURATED}")
    st.write(f"TEMP_DIR: {TEMP_DIR}")

    # Baixar o arquivo Parquet do S3
    baixar_arquivo_parquet(BUCKET_CURATED, FILE_KEY, LOCAL_PATH)

    # Carregar dados
    df = carregar_dados(LOCAL_PATH)

    # Mostrar dataframe
    st.dataframe(df)

    # Criar gráfico de barras com Plotly Express
    st.subheader("Quantidade de Cervejarias por Tipo e Localização")
    grafico = px.bar(df, 
                     x='state', 
                     y='count', 
                     color='brewery_type', 
                     barmode='group', 
                     labels={'state': 'Estado', 'count': 'Quantidade', 'brewery_type': 'Tipo de Cervejaria'})
    st.plotly_chart(grafico)

if __name__ == "__main__":
    main()
