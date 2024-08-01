import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from io import StringIO
from scripts.busca_dados import extracao_api, cria_dataframe, salvando_s3

@patch('scripts.busca_dados.requests.get')
def test_extracao_api(retorno_mock):
    # Preparação
    resposta_mock = MagicMock()
    resposta_mock.json.return_value = [{'name': 'Brewery A'}, {'name': 'Brewery B'}]
    retorno_mock.return_value = resposta_mock
    
    api_url = 'https://api.openbrewerydb.org/breweries'
    experado = pd.DataFrame([{'name': 'Brewery A'}, {'name': 'Brewery B'}])
    
    # Ação
    resultado = extracao_api(api_url)
    resultado_df = pd.DataFrame(resultado)
    
    # Verificação
    pd.testing.assert_frame_equal(resultado_df, experado)

def test_cria_dataframe():
    # Preparação
    lista = [{'name': 'Brewery A'}, {'name': 'Brewery B'}]
    lista_esperado = pd.DataFrame(lista)
    
    # Ação
    resultado_df = cria_dataframe(lista)
    
    # Verificação
    pd.testing.assert_frame_equal(resultado_df, lista_esperado)

@patch('scripts.busca_dados.boto3.resource')
def test_salvando_s3(conexao_mock_boto):
    # Preparação
    mock_s3 = MagicMock()
    conexao_mock_boto.return_value = mock_s3
    
    df = pd.DataFrame({'name': ['Brewery A', 'Brewery B']})
    bucket = 'test-bucket'
    chave = 'test-file.csv'
    
    # Ação
    salvando_s3(df, bucket, chave)
    
    # Verificação
    csv_dados = StringIO()
    df.to_csv(csv_dados, index=False)
    
    mock_s3.Object.assert_called_with(bucket, chave)
    mock_s3.Object().put.assert_called_with(Body=csv_dados.getvalue())
