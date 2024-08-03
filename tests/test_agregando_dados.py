import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from scripts.agregando_dados import agregacao_dados

@patch('scripts.agregando_dados.boto3.client')
@patch('scripts.agregando_dados.pq.read_table')
@patch('scripts.agregando_dados.pq.write_table')
@patch('scripts.agregando_dados.pd.concat')
def test_agregacao_dados(mock_concat, mock_write_table, mock_read_table, mock_boto_client):
    # Configurando mocks
    mock_s3_client = MagicMock()
    mock_boto_client.return_value = mock_s3_client
    
    # Mock de resposta da função list_objects_v2
    mock_s3_client.list_objects_v2.return_value = {
        'Contents': [
            {'Key': 'file1.parquet'},
            {'Key': 'file2.parquet'}
        ]
    }
    
    # Mock da leitura de arquivo Parquet
    df_mock = pd.DataFrame({
        'state': ['CA', 'NY'],
        'brewery_type': ['Micro', 'Nano']
    })
    mock_read_table.return_value.to_pandas.return_value = df_mock
    
    # Mock da concatenação dos DataFrames
    mock_concat.return_value = df_mock
    
    # Mock da escrita de arquivo Parquet
    mock_write_table.return_value = None
    
    # Chama a função a ser testada
    agregacao_dados('bucket_name', 'prefix')

    # Verifica se a função de escrita foi chamada
    mock_write_table.assert_called_once()
