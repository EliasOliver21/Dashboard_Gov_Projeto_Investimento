import pandas as pd
from sqlalchemy import create_engine, text, Table, Column, Integer, String, MetaData, ForeignKey, DECIMAL, BIGINT
import requests
import json
import pandas as pd
import time
import os
from sqlalchemy.exc import OperationalError

## Trazendo as informações da api com o método get
CACHE_FILE = 'api_cache.json'

def fetch_data(filters={}):
    resultado = []
    count = 0
    while(True):
        url = (f'https://api.obrasgov.gestao.gov.br/obrasgov/api/projeto-investimento?uf=DF&pagina={count}')
        
        try:
            response = requests.get(url, params=filters)

            if response.status_code == 200:
                data = response.json()
                if data and data['content']:
                    print(f"Adicionando dados da página nº: {count}")
                    resultado.extend(data['content'])
                else:
                    print("Parando: Nenhum conteúdo na página. Extração concluída.")
                    break
            
            elif response.status_code == 429:
                print(f"Erro 429 (Too Many Requests) na página {count}. Aguardando 60 segundos para tentar novamente...")
                time.sleep(60)
                continue
            
            else:
                print(f"Erro na página nº: {count} com status code: {response.status_code}. Parando extração.")
                break

            count += 1
            time.sleep(1)

        except requests.exceptions.RequestException as e:
            print(f"Erro de conexão (ex: timeout): {e}. Aguardando 30 segundos...")
            time.sleep(30)
            continue 

    return resultado



def get_data_from_api_or_cache(cache_file=CACHE_FILE):
    """
    Verifica se o cache (arquivo JSON) existe.
    Se existir, lê os dados do arquivo.
    Se não existir, chama a API (fetch_data) e salva os resultados no arquivo.
    """
    if os.path.exists(cache_file):
        print(f"Lendo dados do cache local: {cache_file}")
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                dados = json.load(f)
            return dados
        except json.JSONDecodeError:
            print("Erro ao ler o arquivo JSON. O arquivo pode estar corrompido.")
            pass

    print("Cache não encontrado. Buscando dados da API... (Isso pode demorar)")
    dados = fetch_data()
    
    if dados:
        print(f"Salvando dados da API no cache: {cache_file}")
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(dados, f, ensure_ascii=False, indent=4)
    else:
        print("Nenhum dado foi retornado da API. O cache não foi criado.")
    
    return dados

dados = get_data_from_api_or_cache()

df = pd.DataFrame(dados)

df[['idUnico','nome','subTipos', 'tipos']]

df['tomadorNome'] = df['tomadores'].apply(lambda x: x[0]['nome'] if len(x) > 0 else None)
df['tomadorCodigo'] = df['tomadores'].apply(lambda x: x[0]['codigo'] if len(x) > 0 else None)

df['executorNome'] = df['executores'].apply(lambda x: x[0]['nome'] if len(x) > 0 else None)
df['executorCodigo'] = df['executores'].apply(lambda x: x[0]['codigo'] if len(x) > 0 else None)

df['repassadorNome'] = df['repassadores'].apply(lambda x: x[0]['nome'] if len(x) > 0 else None)
df['repassadorCodigo'] = df['repassadores'].apply(lambda x: x[0]['codigo'] if len(x) > 0 else None)

df['origemFontesDeRecurso'] = df['fontesDeRecurso'].apply(lambda x: x[0]['origem'] if len(x) > 0 else None)
df['valorInvestimentoPrevisto'] = df['fontesDeRecurso'].apply(lambda x: x[0]['valorInvestimentoPrevisto'] if len(x) > 0 else None)

df_principal = df.drop(columns=['tomadores','executores','repassadores','fontesDeRecurso'])

# Listas para armazenar os dados extraídos
eixosData = []
tiposData = []
subtiposData = []

# Listas para as tabelas de ligação
operacaoEixoRel = []
operacaoTipoRel = []
operacaoSubtipoRel = []

# Iterar sobre o DataFrame original para extrair os dados aninhados
for index, row in df_principal.iterrows():
    id_op = row['idUnico']

    for eixo in row['eixos']:
        eixosData.append(eixo)
        operacaoEixoRel.append({'idUnico': id_op, 'id_eixo': eixo['id']})

    for tipo in row['tipos']:
        tiposData.append(tipo)
        operacaoTipoRel.append({'idUnico': id_op, 'id_tipo': tipo['id']})

    for subtipo in row['subTipos']:
        subtiposData.append(subtipo)
        operacaoSubtipoRel.append({'idUnico': id_op, 'id_subtipo': subtipo['id']})

# --- Criar os novos DataFrames (Tabelas de Dimensão) ---

# DataFrame de Eixos
df_eixos = pd.DataFrame(eixosData).drop_duplicates(subset=['id']).reset_index(drop=True)

# DataFrame Tabela de ligação de Eixos
df_operacaoEixoRel = pd.DataFrame(operacaoEixoRel).drop_duplicates().reset_index(drop=True)

# DataFrame de Tipos
df_tipos = pd.DataFrame(tiposData).drop_duplicates(subset=['id']).reset_index(drop=True)

# DataFrame Tabewla de ligação de Tipos
df_operacaoTipoRel = pd.DataFrame(operacaoTipoRel).drop_duplicates().reset_index(drop=True)

# DataFrame de SubTipos
df_subtipos = pd.DataFrame(subtiposData).drop_duplicates(subset=['id']).reset_index(drop=True)

# DataFrame Tabela de ligação de Subtipos
df_operacaoSubtipoRel = pd.DataFrame(operacaoSubtipoRel).drop_duplicates().reset_index(drop=True)

# DataFrame Principal dos Dados
df_principal_final = df_principal.drop(columns=['tipos','subTipos','eixos'])

df_principal_final = df_principal_final.drop_duplicates(subset=['idUnico'], keep='first')

# Renomeia as colunas
df_eixos.rename(columns={'id': 'id_eixo', 'descricao': 'descricao_eixo'}, inplace=True)
df_tipos.rename(columns={'id': 'id_tipo', 'descricao': 'descricao_tipo', 'idEixo': 'id_eixo'}, inplace=True)
df_subtipos.rename(columns={'id': 'id_subtipo', 'descricao': 'descricao_subtipo', 'idTipo': 'id_tipo'}, inplace=True)

df_operacaoEixoRel.rename(columns={'idUnico':'id_operacao','id_eixo': 'id_eixo'}, inplace=True)
df_operacaoTipoRel.rename(columns={'idUnico':'id_operacao', 'id_tipo': 'id_tipo'}, inplace=True)
df_operacaoSubtipoRel.rename(columns={'idUnico':'id_operacao', 'id_subtipo': 'id_subtipo'}, inplace=True)
df_principal_final.rename(columns={
    'idUnico': 'id_operacao',
    'valorInvestimentoPrevisto': 'valor_investimento_previsto',
    'tomadorNome': 'tomador_nome',
    'tomadorCodigo': 'tomador_codigo',
    'executorNome': 'executor_nome',
    'executorCodigo': 'executor_codigo',
    'repassadorNome': 'repassador_nome',
    'repassadorCodigo': 'repassador_codigo',
    'origemFontesDeRecurso': 'origem_fontes_de_recurso'
}, inplace=True)


colunas_para_operacoes = [
    'id_operacao', 
    'valor_investimento_previsto',
    'tomador_nome', 
    'tomador_codigo',
    'executor_nome',
    'executor_codigo',
    'repassador_nome',
    'repassador_codigo',
    'origem_fontes_de_recurso'
]

#Adicionando apenas os atributos a serem utilizados no dashboard
df_principal_final_filtrado = df_principal_final[colunas_para_operacoes]


print("Iniciando verificação do banco de dados...")

db_user = "root"
db_password = "root" # Senha do docker-compose
db_host = "db"
db_name = "dados_governo"
db_port = 3306

retries = 10
engine = None

# URL para conectar ao SERVIDOR (sem DB específico)
server_engine_url = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}"

for i in range(retries):
    try:

        temp_engine = create_engine(server_engine_url)
        with temp_engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name}"))
            conn.commit()
        
        temp_engine.dispose()
        print(f"Banco de dados '{db_name}' verificado/criado com sucesso.")
        break

    except OperationalError:
        print(f"Servidor MySQL ainda não está pronto... tentativa {i+1} de {retries}.")
        if i < retries - 1:
            time.sleep(5)
        else:
            print("Não foi possível conectar ao servidor MySQL após várias tentativas.")
            exit(1)
    except Exception as e:
        print(f"Erro inesperado ao criar/verificar o DB: {e}")
        exit(1)

# 3. conecta DIRETAMENTE AO BANCO
try:
    engine_url = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(engine_url)
    with engine.connect() as conn:
        pass 
    print(f"Conexão com o banco de dados '{db_name}' estabelecida!")
except Exception as e:
    print(f"Erro ao conectar ao banco de dados '{db_name}': {e}")
    exit(1)


# Definição da estrutura (Schema) das tabelas
metadata = MetaData()

# Tabela de Dimensão: Eixos
eixos_table = Table('eixos', metadata,
    Column('id_eixo', Integer, primary_key=True),
    Column('descricao_eixo', String(255))
)

# Tabela de Dimensão: Tipos
tipos_table = Table('tipos', metadata,
    Column('id_tipo', Integer, primary_key=True),
    Column('descricao_tipo', String(255)),
    Column('id_eixo', Integer, ForeignKey('eixos.id_eixo'))
)

# Tabela de Dimensão: Subtipos
subtipos_table = Table('subtipos', metadata,
    Column('id_subtipo', Integer, primary_key=True),
    Column('descricao_subtipo', String(255)),
    Column('id_tipo', Integer, ForeignKey('tipos.id_tipo'))
)

# Tabela de Fato: Operacoes
operacoes_table = Table('operacoes', metadata,
    Column('id_operacao', String(100), primary_key=True), 
    Column('valor_investimento_previsto', DECIMAL(15,2)), 
    Column('tomador_nome', String(255)),
    Column('tomador_codigo', BIGINT),
    Column('executor_nome', String(255)),
    Column('executor_codigo', BIGINT),
    Column('repassador_nome', String(255)),
    Column('repassador_codigo', BIGINT),
    Column('origem_fontes_de_recurso', String(255))
)

# Tabelas de Ligação Eixo
operacao_eixo_rel_table = Table('operacao_eixo_rel', metadata,
    Column('id_operacao', String(100), ForeignKey('operacoes.id_operacao'), primary_key=True),
    Column('id_eixo', Integer, ForeignKey('eixos.id_eixo'), primary_key=True),
)

# Tabelas de Ligação Tipo
operacao_tipo_rel_table = Table('operacao_tipo_rel', metadata,
    Column('id_operacao', String(100), ForeignKey('operacoes.id_operacao'), primary_key=True),                                
    Column('id_tipo', Integer, ForeignKey('tipos.id_tipo'), primary_key=True),
)

# Tabelas de Ligação Subtipo
operacao_subtipo_rel_table = Table('operacao_subtipo_rel', metadata,
    Column('id_operacao', String(100), ForeignKey('operacoes.id_operacao'), primary_key=True),
    Column('id_subtipo', Integer, ForeignKey('subtipos.id_subtipo'), primary_key=True)
)


print("Criando/Verificando tabelas no banco de dados...")
metadata.create_all(engine)

with engine.connect() as conn:
    conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
    conn.execute(text("TRUNCATE TABLE operacao_subtipo_rel;"))
    conn.execute(text("TRUNCATE TABLE operacao_tipo_rel;"))
    conn.execute(text("TRUNCATE TABLE operacao_eixo_rel;"))
    conn.execute(text("TRUNCATE TABLE operacoes;"))
    conn.execute(text("TRUNCATE TABLE subtipos;"))
    conn.execute(text("TRUNCATE TABLE tipos;"))
    conn.execute(text("TRUNCATE TABLE eixos;"))
    conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
    conn.commit()

# Populando as tabelas
print("\nIniciando carga dos dados no MySQL...")
df_principal_final_filtrado.to_sql('operacoes', con=engine, if_exists='append', index=False)
df_eixos.to_sql('eixos', con=engine, if_exists='append', index=False)
df_tipos.to_sql('tipos', con=engine, if_exists='append', index=False)
df_subtipos.to_sql('subtipos', con=engine, if_exists='append', index=False)
df_operacaoEixoRel.to_sql('operacao_eixo_rel', con=engine, if_exists='append', index=False)
df_operacaoTipoRel.to_sql('operacao_tipo_rel', con=engine, if_exists='append', index=False)
df_operacaoSubtipoRel.to_sql('operacao_subtipo_rel', con=engine, if_exists='append', index=False)

print("Carga de dados concluída com sucesso!")