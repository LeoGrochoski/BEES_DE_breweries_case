# BEES Data Engineering – Breweries Case

<img src="https://miro.medium.com/v2/resize:fit:2400/1*6Eg35s47bNX7BcPzM0Fllg.png" alt="BEES" width="500" height="300">

## Objetivo

O objetivo deste projeto é demonstrar minhas habilidades em consumir dados de uma API, transformando-os e persistindo-os
em um data lake seguindo a arquitetura medalhão com três camadas: dados brutos, dados selecionados
particionado por localização e uma camada analítica agregada.

## Configuração de ambiente

1 - Criar o repositório do projeto no GitHub (para quem for utilizar clonar)

2 - Setar a versão do Python para o projeto

```bash
pyenv local 3.11.5
```

3 - Iniciar o Poetry. 

```bash
poetry init
```
**OBS**: Para o projeto iniciei de forma basica sem customizar o poetry, somente seguindo com enter

4 - Vincular a versão do python com o poetry

```bash
poetry env use 3.11.5
```
**OBS**: Airflow roda somente no python 3.11 até este momento.

5 -  Ativar ambiente virtual criado pelo Poetry
```bash
poetry shell
```

6 - Instalar as bibliotecas pertinentes ao projeto.


```bash
poetry add <biblioteca>
```

**OBS**: Como estamos utilizando o poetry como gerenciador do ambiente, usar o comando para cada biblioteca. Para facilitar criei um requirements.txt com as bibliotecas do projeto