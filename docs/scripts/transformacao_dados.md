# transformacao_dados.py

Este módulo é responsável por transformar dados de cervejarias armazenados em arquivos CSV no S3. Ele lê o arquivo CSV mais recente, realiza transformações nos dados, e salva os resultados como arquivos Parquet separados por estado no bucket raw do S3.

## Funções Principais

### listar_arquivos_csv(bucket_name: str, prefix: str = "") -> List[str]

Lista todos os arquivos CSV em um bucket S3 específico.

**Argumentos:**
- `bucket_name` (str): Nome do bucket S3.
- `prefix` (str, opcional): Prefixo para filtrar os arquivos. Padrão é "".

**Retorna:**
- `List[str]`: Lista de chaves (nomes) dos arquivos CSV encontrados.

**Erros:**
- `FileNotFoundError`: Se nenhum arquivo CSV for encontrado no bucket.

### baixar_arquivo_csv(bucket_name: str, arquivo: str) -> str

Baixa um arquivo CSV específico de um bucket S3.

**Argumentos:**
- `bucket_name` (str): Nome do bucket S3.
- `arquivo` (str): Chave (nome) do arquivo a ser baixado.

**Retorna:**
- `str`: Caminho local do arquivo baixado.

### transformar_dados(caminho_csv: str) -> pd.DataFrame

Lê um arquivo CSV e realiza transformações nos dados.

Transformações realizadas:
- Normaliza os nomes dos estados (strip e title case).
- Remove linhas com valores nulos na coluna 'state'.

**Argumentos:**
- `caminho_csv` (str): Caminho local do arquivo CSV.

**Retorna:**
- `pd.DataFrame`: DataFrame com os dados transformados.

### salvar_dados_parquet(df: pd.DataFrame, state: str, bucket_name: str)

Salva um DataFrame como arquivo Parquet no S3, separado por estado.

**Argumentos:**
- `df` (pd.DataFrame): DataFrame a ser salvo.
- `state` (str): Nome do estado para o qual os dados estão sendo salvos.
- `bucket_name` (str): Nome do bucket S3 onde o arquivo será salvo.

### transformacao_dados(entrada_bucket: str, saida_bucket: str, prefix: str)

Função principal que orquestra o processo de transformação dos dados.

Esta função realiza as seguintes operações:
1. Lista e seleciona o arquivo CSV mais recente no bucket de entrada.
2. Baixa o arquivo CSV selecionado.
3. Transforma os dados.
4. Agrupa os dados por estado.
5. Salva os dados transformados como arquivos Parquet separados por estado no bucket de saída.

**Argumentos:**
- `entrada_bucket` (str): Nome do bucket S3 contendo os arquivos CSV de origem.
- `saida_bucket` (str): Nome do bucket S3 onde os arquivos Parquet serão salvos.
- `prefix` (str): Prefixo para filtrar os arquivos CSV no bucket de entrada.

## Dependências

- pandas
- pyarrow
- boto3
- dotenv
- os
- datetime
- logging

## Configuração

O módulo utiliza variáveis de ambiente carregadas de um arquivo `.env` para configurações sensíveis como chaves de acesso AWS e nomes de buckets.

## Logging

O módulo utiliza logging para registrar informações e erros durante a execução do processo de transformação.

## Execução

Quando executado como script principal, o módulo chama a função `transformacao_dados` com os buckets de entrada e saída configurados, e o prefixo para filtrar os arquivos CSV no bucket de entrada.
