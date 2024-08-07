# agregando_dados.py

Este módulo é responsável por agregar dados de cervejarias armazenados em arquivos Parquet no S3. Ele lê os arquivos, realiza a agregação por estado e tipo de cervejaria, e salva o resultado em um novo arquivo Parquet no bucket curado do S3.

## Funções Principais

### listar_arquivos_parquet(bucket_name, prefix="")

Lista todos os arquivos Parquet em um bucket S3 específico.

**Argumentos:**
- `bucket_name` (str): Nome do bucket S3.
- `prefix` (str, opcional): Prefixo para filtrar os arquivos. Padrão é "".

**Retorna:**
- Lista de chaves (nomes) dos arquivos Parquet encontrados.

### agregacao_dados(bucket_name, prefix)

Agrega dados de cervejarias de múltiplos arquivos Parquet no S3.

**Operações:**
1. Lista arquivos Parquet no bucket S3 especificado.
2. Baixa e lê cada arquivo Parquet.
3. Agrega os dados por estado e tipo de cervejaria.
4. Combina os resultados de todos os arquivos.
5. Salva o resultado agregado em um novo arquivo Parquet no bucket curado.

**Argumentos:**
- `bucket_name` (str): Nome do bucket S3 contendo os arquivos de origem.
- `prefix` (str): Prefixo para filtrar os arquivos no bucket.

## Dependências

- pandas
- pyarrow
- boto3
- dotenv

## Configuração

O módulo utiliza variáveis de ambiente carregadas de um arquivo `.env` para configurações sensíveis como chaves de acesso AWS e nomes de buckets.

## Logging

O módulo utiliza logging para registrar informações e erros durante a execução do processo de agregação.

## Execução

Quando executado como script principal, o módulo chama a função `agregacao_dados` com o bucket raw e sem prefixo.