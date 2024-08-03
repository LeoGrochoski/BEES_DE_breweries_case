import streamlit as st
import pandas as pd
import pyarrow.parquet as pq
import boto3
import os
from dotenv import load_dotenv
import plotly.express as px

load_dotenv()

def baixar_arquivo_parquet(bucket_name, file_key, local_path):
    s3 = boto3.client('s3',
                      aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                      aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))
    s3.download_file(bucket_name, file_key, local_path)

def carregar_dados(local_path):
    table = pq.read_table(local_path)
    return table.to_pandas()

# Função principal do Streamlit
def main():
    st.title("Dashboard de Cervejarias")

    # Configurações de ambiente
    BUCKET_CURATED = os.getenv('BUCKET_CURATED')
    FILE_KEY = 'aggregated_breweries.parquet'
    LOCAL_PATH = '/tmp/aggregated_breweries.parquet'

    # Baixar o arquivo Parquet do S3
    baixar_arquivo_parquet(BUCKET_CURATED, FILE_KEY, LOCAL_PATH)

    # Carregar dados
    df = carregar_dados(LOCAL_PATH)

    # Mostrar dataframe
    st.dataframe(df)

    # Criar mapa com Plotly Express
    st.subheader("Mapa de Localização das Cervejarias")
    mapa = px.scatter_geo(df, 
                          locations="state", 
                          locationmode="USA-states", 
                          hover_name="state", 
                          size="count", 
                          projection="albers usa")
    st.plotly_chart(mapa)

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