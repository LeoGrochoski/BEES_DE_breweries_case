# busca_dados.py

Este módulo é responsável por extrair dados de cervejarias da API OpenBreweryDB, transformá-los em um DataFrame pandas, e salvar os resultados como um arquivo CSV no S3.

## Funções Principais

### extracao_api(api_url: str) -> Dict[str, Any]

Realiza uma requisição GET para a API especificada e retorna os dados JSON.

**Argumentos:**
- `api_url` (str): URL da API para fazer a requisição.

**Retorna:**
- `Dict[str, Any]`: Dados JSON retornados pela API.

**Erros:**
- `requests.exceptions.RequestException`: Se ocorrer um erro na requisição à API.

### cria_dataframe(lista: list) -> DataFrame

Converte uma lista de dicionários em um DataFrame pandas.

**Argumentos:**
- `lista` (list): Lista de dicionários contendo os dados das cervejarias.

**Retorna:**
- `DataFrame`: DataFrame pandas criado a partir da lista de dados.

**Erros:**
- `Exception`: Se ocorrer um erro durante a conversão para DataFrame.

### salvando_s3(df: pd.DataFrame, bucket: str, key: str)

Converte um DataFrame para CSV e o salva em um bucket S3.

**Argumentos:**
- `df` (pd.DataFrame): DataFrame a ser salvo.
- `bucket` (str): Nome do bucket S3 onde o arquivo será salvo.
- `key` (str): Chave (nome do arquivo) para o arquivo no S3.

**Erros:**
- `Exception`: Se ocorrer um erro durante a conversão para CSV ou upload para S3.

## Dependências

- requests
- pandas
- boto3
- dotenv
- StringIO (from io)
- DataFrame (from pandas)
- Dict, Any (from typing)
- datetime (from datetime)
- logging

## Configuração

O módulo utiliza variáveis de ambiente carregadas de um arquivo `.env` para configurações sensíveis como chaves de acesso AWS e nomes de buckets.

## Logging

O módulo utiliza logging para registrar informações e erros durante a execução do processo de extração.

## Execução

Quando executado como script principal, o módulo chama a função `extracao_api` com a URL da API OpenBreweryDB, converte os dados em um DataFrame, e salva o resultado em um arquivo CSV no bucket S3 configurado.
