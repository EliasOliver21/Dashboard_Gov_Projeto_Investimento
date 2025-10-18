# Dashboard Projeto Investimento

Este repositório tem como objetivo documentar a criação de um dashboard, para analisar os dados contidos na [API: Consulta Cadastro Integrado de Projetos de Investimentos](https://www.gov.br/conecta/catalogo/apis/consulta-cadastro-integrado-de-projetos-de-investimentos-2013-obrasgov.br).

# Ferramentas

Para a criação deste projeto, foram utilizadas as seguintes ferramentas e tecnologias:

- Python 3.12
- Streamlit
- Pandas
- Docker
- MySQL

## Estrutura do Projeto

- `/scripts`: Contém o script principal de ETL (`processa_dados.py`).
- `/notebooks`: Contém o progresso inicial e formato inicial do script ETL(`processa_dados.py`).
- `/dashboard`: Contém o código da aplicação Streamlit (`app.py`).
- `requirements.txt`: Lista de dependências Python.
- `README.md`: Este documento.


## Como Executar

Para executar o projeto, inicialmente, garanta que o [python 3.12.3](https://www.python.org/downloads/) esteja instalado e atualizado.

```bash
# Clone o repositório

git clone https://github.com/EliasOliver21/Dashboard_Gov_Projeto_Investimento.git

# Entre no diretório do projeto

cd Dashboard_Gov_Projeto_Investimento

# Crie um ambiente virtual

python3 -m venv venv

# Use o ambiente

source venv/bin/activate

# Instale as dependências

pip install -r requirements.txt

```

## Docker

Para executar o projeto você precisa ter instalado o docker em sua máquina, siga o guia de [instalação oficial do docker](https://docs.docker.com/desktop/setup/install/windows-install/).

```bash
# Verifique o status do docker

sudo systemctl status docker

# Execute o o teste "hello-world" para ver se está tudo funcionando

docker run hello-world

```

```bash
# Agora execute o comando para construir os conteineres e ativá-los

docker compose up --build

```