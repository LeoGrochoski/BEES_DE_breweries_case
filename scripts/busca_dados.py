import requests
import pandas as pd
from pandas import DataFrame
from typing import Dict, Any

# Sessão para trazer os dados da API no formato JSON

API = 'https://api.openbrewerydb.org/breweries'

def extracao_api(api_url: str) -> Dict[str, Any]:
    def requisicao_api(url: str) -> Dict[str, Any]:
        req = requests.get(url, timeout=10)
        req.raise_for_status()  
        return req.json()
    
    return requisicao_api(api_url)


# Sesão de conversão do JSON para Dataframe

def cria_dataframe(lista: list) -> DataFrame:
    df: DataFrame = pd.DataFrame(lista)
    return df
        
dados = extracao_api(API)
tabela = cria_dataframe(dados)
print(tabela)