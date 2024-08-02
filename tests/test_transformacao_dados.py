import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from scripts.transformacao_dados import transformacao_dados

@patch('scripts.transformacao_dados.boto3.client')
@patch('scripts.transformacao_dados.pd.read_csv')
@patch('scripts.transformacao_dados.os.makedirs')
@patch('scripts.transformacao_dados.os.path.exists')
@patch('scripts.transformacao_dados.os.path.join')
def test_transformacao_dados(
        mock_os_path_join, mock_exists, mock_makedirs, 
        mock_read_csv, mock_boto_client):
    
    # Preparação dos mocks
    mock_exists.return_value = False
    mock_makedirs.return_value = None
    
    mock_read_csv.return_value = pd.DataFrame({'state': ['CA', 'NY'], 'name': ['Brewery A', 'Brewery B']})
    
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    mock_s3.download_file = MagicMock()
    mock_s3.upload_file = MagicMock()
    
    # Ajuste o caminho temporário para o teste
    def mock_join(*args):
        return '/'.join(args)
    
    mock_os_path_join.side_effect = mock_join
    
    entrada_bucket = 'breweries-lake-land'
    saida_bucket = 'breweries-lake-raw'
    prefix = 'breweries_data_land_'
    
    # Mock para garantir que o arquivo CSV seja encontrado
    mock_s3.list_objects_v2.return_value = {
        'Contents': [
            {'Key': 'breweries_data_land_2024-08-01_20-14-54.csv', 'LastModified': pd.Timestamp('2024-08-01 20:14:54')},
            {'Key': 'breweries_data_land_2024-08-01_20-15-00.csv', 'LastModified': pd.Timestamp('2024-08-01 20:15:00')}
        ]
    }
    
    # Ação
    transformacao_dados(entrada_bucket, saida_bucket, prefix)
    
    # Verificação dos mocks
    assert mock_s3.download_file.call_count == 1, "O método download_file não foi chamado exatamente uma vez."
    
    expected_args = (entrada_bucket, 'breweries_data_land_2024-08-01_20-15-00.csv', '/tmp/breweries.csv')
    actual_args = mock_s3.download_file.call_args[0]
    assert actual_args == expected_args, f"Esperado {expected_args}, mas foi {actual_args}"
    
    chamadas_upload = mock_s3.upload_file.call_args_list
    assert len(chamadas_upload) > 0, "O método upload_file não foi chamado."
    
    for call in chamadas_upload:
        args = call[0]
        assert args[0].endswith('.parquet'), f"O arquivo enviado não é parquet: {args[0]}"
        assert args[1] == saida_bucket, f"Bucket incorreto para upload: {args[1]}"
        assert args[2]  # O caminho S3 deve ser especificado
    
    # Verificar se o diretório temporário foi criado
    mock_makedirs.assert_called_once_with('/tmp')

if __name__ == "__main__":
    pytest.main()
