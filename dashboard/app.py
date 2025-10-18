import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError
import time
import altair as alt

# --- Configurações da Página ---
st.set_page_config(
    page_title="Dashboard ObrasGov",
    page_icon="🇧🇷",
    layout="wide"
)

# --- Conexão com o Banco de Dados ---
@st.cache_resource
def get_connection():
    retries = 10
    
    db_user = "root"
    db_password = "root" # Sua senha do docker-compose
    db_host = "db"
    db_name = "dados_governo"
    db_port = 3306
    
    server_engine_url = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}"
    
    for i in range(retries):
        try:
            temp_engine = create_engine(server_engine_url)
            with temp_engine.connect() as conn:
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name}"))
                conn.commit()
            temp_engine.dispose()
            
            engine_url = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            engine = create_engine(engine_url)
            
            with engine.connect() as conn:
                pass
            
            return engine, "success"

        except OperationalError:
            if i < retries - 1:
                time.sleep(3) 
            else:
                return None, "connection_error"
        except Exception as e:
            st.error(f"Erro inesperado na conexão: {e}")
            return None, "connection_error"

# --- Carregamento dos Dados ---
@st.cache_data(ttl=600)
def load_data():
    try:
        engine, conn_status = get_connection()

        if conn_status != "success":
            return None, "connection_error_from_load"

        query = """
        SELECT 
            op.id_operacao,
            op.valor_investimento_previsto,
            op.tomador_nome,
            op.origem_fontes_de_recurso,
            GROUP_CONCAT(DISTINCT ex.descricao_eixo SEPARATOR ', ') AS eixo_descricao,
            GROUP_CONCAT(DISTINCT tp.descricao_tipo SEPARATOR ', ') AS tipo_descricao
        FROM operacoes op
        LEFT JOIN operacao_eixo_rel oe ON op.id_operacao = oe.id_operacao
        LEFT JOIN eixos ex ON oe.id_eixo = ex.id_eixo
        LEFT JOIN operacao_tipo_rel otr ON op.id_operacao = otr.id_operacao
        LEFT JOIN tipos tp ON otr.id_tipo = tp.id_tipo
        GROUP BY 
            op.id_operacao,
            op.valor_investimento_previsto,
            op.tomador_nome,
            op.origem_fontes_de_recurso;
        """
        df = pd.read_sql(query, engine)
        
        # Limpeza pós-query: preencher valores nulos para gráficos
        df['eixo_descricao'] = df['eixo_descricao'].fillna('Não Categorizado')
        df['tipo_descricao'] = df['tipo_descricao'].fillna('Não Categorizado')
        df['origem_fontes_de_recurso'] = df['origem_fontes_de_recurso'].fillna('Não Informada')
        
        if df.empty:
            return None, "no_data"
            
        return df, "success"
        
    except ProgrammingError:
        return None, "table_not_found"
    except Exception as e:
        st.error(f"Erro inesperado ao carregar dados: {e}")
        return None, "other_error"

# --- FUNÇÃO PRINCIPAL DO DASHBOARD (COM NOVOS GRÁFICOS) ---

# 1. Título e Descrição
st.title("🇧🇷 Dashboard de Análise de Obras")
st.markdown("Análise de dados extraídos da **API ObrasGov (Projeto Investimento)**.")
st.markdown("---")

# 2. Inicializar o estado do botão
if 'show_data' not in st.session_state:
    st.session_state.show_data = False

if st.button("Iniciar Análise (Carregar Dados)"):
    st.session_state.show_data = True

# 3. Bloco de Lógica Principal
if st.session_state.show_data:
    
    with st.spinner("Conectando ao banco de dados e carregando dados..."):
        
        engine, conn_status = get_connection()

        if conn_status == "connection_error":
            st.error("Falha ao conectar ao banco de dados. Verifique os contêineres.", icon="🔥")
            st.stop() 

        df, data_status = load_data()

        if data_status == "success":
            st.success("Dados carregados com sucesso!")
            
            # --- VISUALIZAÇÃO 1: MÉTRICAS (KPIs) ---
            st.subheader("Métricas Principais")
            total_valor = df['valor_investimento_previsto'].sum()
            num_operacoes = len(df)
            media_valor = df['valor_investimento_previsto'].mean()
            num_eixos = df['eixo_descricao'].nunique()
            num_tipos = df['tipo_descricao'].nunique()
            num_origens = df['origem_fontes_de_recurso'].nunique()

            col1, col2, col3, col4, col5= st.columns(5)
            col1.metric("Valor Total Previsto", f"R$ {total_valor:,.2f}")
            col2.metric("Nº de Operações", f"{num_operacoes}")
            col3.metric("Valor Médio/Operação", f"R$ {media_valor:,.2f}")
            col4.metric("Nº de Eixos", f"{num_eixos}")
            col5.metric("Nº de Origens de Recurso", f"{num_origens}")
            

            st.markdown("---")
            
            # --- GRÁFICOS (Layout 2x2) ---
            st.subheader("Análise dos Investimentos")
            col_graf_1, col_graf_2 = st.columns(2)

            with col_graf_1:
                # --- VISUALIZAÇÃO 2: Valor por Tipo/Espécie (Gráfico de Pizza) ---
                st.markdown("#### Valor por Tipo (Espécie)")
                df_tipo = df.groupby('tipo_descricao')['valor_investimento_previsto'].sum().reset_index()
                
                # Gráfico de pizza
                base = alt.Chart(df_tipo).encode(
                   theta=alt.Theta("valor_investimento_previsto", stack=True)
                )
                pie = base.mark_arc(outerRadius=120).encode(
                    color=alt.Color("tipo_descricao", title="Tipo"), 
                    order=alt.Order("valor_investimento_previsto", sort="descending"),
                    tooltip=["tipo_descricao", "valor_investimento_previsto"]
                )
                st.altair_chart(pie, use_container_width=True)

            with col_graf_2:
                # --- VISUALIZAÇÃO 3: Valor por Eixo (Gráfico de Barras) ---
                st.markdown("#### Valor por Eixo")
                df_eixo_valor = df.groupby('eixo_descricao')['valor_investimento_previsto'].sum().sort_values(ascending=False)
                st.bar_chart(df_eixo_valor)

            
            st.markdown("<br>", unsafe_allow_html=True) # Adiciona um espaço
            col_graf_3, col_graf_4 = st.columns(2)

            with col_graf_3:
                # --- VISUALIZAÇÃO 4: Top 10 Tomadores por Valor (Barras Horizontais) ---
                st.markdown("#### Top 10 Tomadores de Recurso")
                df_top_tomadores = df.groupby('tomador_nome')['valor_investimento_previsto'].sum().nlargest(10).sort_values(ascending=True)
                st.bar_chart(df_top_tomadores, horizontal=True)

            with col_graf_4:
                # --- VISUALIZAÇÃO 5: Contagem de Operações por Eixo (Barras) ---
                st.markdown("#### Contagem de Operações por Eixo")
                df_eixo_contagem = df['eixo_descricao'].value_counts().sort_values(ascending=False)
                st.bar_chart(df_eixo_contagem)

            st.markdown("---")

            # --- VISUALIZAÇÃO 6: Tabela de Dados Interativa ---
            st.subheader("Explore os Dados Completos")
            st.dataframe(df)

        # --- Lógica de Erro (sem alterações) ---
        elif data_status == "no_data":
            st.warning("Banco de dados conectado, mas as tabelas estão vazias.", icon="📊")
            st.info("Execute o script de ETL para popular o banco de dados:")
            st.code("docker compose exec app python scripts/processa_dados.py", language="bash")
            st.info("Após executar o comando, atualize esta página.")

        elif data_status == "table_not_found":
            st.error("Tabelas não encontradas no banco de dados.", icon="🔍")
            st.info("Execute o script de ETL para criar e popular o banco de dados:")
            st.code("docker compose exec app python scripts/processa_dados.py", language="bash")
            st.info("Após executar o comando, atualize esta página.")

        elif data_status == "connection_error_from_load":
             st.error("Erro na conexão ao tentar carregar os dados. Verifique os logs.")