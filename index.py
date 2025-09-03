import streamlit as st
import pandas as pd
import time
from datetime import datetime
import io
import tempfile
import os
import logging
from sql_converter import SQLConverter
from sql_generator import SQLGenerator

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuração da página
st.set_page_config(
    page_title="Firebird to PostgreSQL Converter",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personalizado (mantenha seu CSS existente)
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #1E88E5;
        text-align: center;
        margin-bottom: 2rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #0D47A1;
        margin-bottom: 1rem;
    }
    .info-box {
        background-color: #E3F2FD;
        padding: 1.5rem;
        border-radius: 0.5rem;
        margin-bottom: 1.5rem;
    }
    .success-box {
        background-color: #E8F5E9;
        padding: 1.5rem;
        border-radius: 0.5rem;
        margin-bottom: 1.5rem;
    }
    .warning-box {
        background-color: #FFF8E1;
        padding: 1.5rem;
        border-radius: 0.5rem;
        margin-bottom: 1.5rem;
    }
    .progress-bar {
        height: 25px;
        border-radius: 5px;
        background-color: #E0E0E0;
        margin-bottom: 1rem;
    }
    .progress-fill {
        height: 100%;
        border-radius: 5px;
        background-color: #43A047;
        text-align: center;
        color: white;
        line-height: 25px;
    }
    .metric-box {
        background-color: #F5F5F5;
        padding: 1rem;
        border-radius: 0.5rem;
        text-align: center;
        margin: 0.5rem;
    }
    .stButton>button {
        width: 100%;
        background-color: #1E88E5;
        color: white;
    }
    .file-type-tabs {
        display: flex;
        margin-bottom: 1rem;
    }
    .file-type-tab {
        padding: 0.5rem 1rem;
        cursor: pointer;
        border-radius: 0.5rem 0.5rem 0 0;
        margin-right: 0.5rem;
        background-color: #E0E0E0;
    }
    .file-type-tab.active {
        background-color: #1E88E5;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

# Cabeçalho da aplicação
st.markdown('<h1 class="main-header">🔄 Firebird to PostgreSQL Converter</h1>', unsafe_allow_html=True)
st.markdown("### Converta seus bancos Firebird (.FDB) ou scripts SQL para PostgreSQL")

# Barra lateral para configurações
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/2/29/Postgresql_elephant.svg/1200px-Postgresql_elephant.svg.png", width=100)
    st.markdown("### Configurações")
    
    # Opções de conversão
    st.markdown("#### Opções de Conversão")
    batch_size = st.slider("Tamanho do lote para processamento", 1000, 50000, 10000, help="Número de registros processados por vez")
    
    # Configurações do PostgreSQL
    st.markdown("#### Configurações do PostgreSQL")
    pg_version = st.selectbox("Versão do PostgreSQL", ["13", "14", "15", "16"])
    encoding = st.selectbox("Codificação", ["UTF8", "LATIN1", "WIN1252"])
    
    # Opções avançadas
    with st.expander("Opções Avançadas"):
        include_indexes = st.checkbox("Incluir índices", value=True)
        include_constraints = st.checkbox("Incluir constraints", value=True)
        include_triggers = st.checkbox("Incluir triggers", value=False)
        data_only = st.checkbox("Apenas dados (sem schema)", value=False)
    
 
  
# Layout principal com abas
tab1, tab2, tab3 = st.tabs(["Conversão", "Monitoramento", "Histórico"])

with tab1:
    # Seletor de tipo de arquivo
    st.markdown("### Selecione o Tipo de Arquivo")
    file_type = st.radio(
        "Tipo de arquivo de entrada:",
        ["Arquivo Firebird (.FDB)", "Script SQL Firebird (.SQL)"],
        horizontal=True
    )
    
    # Área de upload baseada no tipo selecionado
    if file_type == "Arquivo Firebird (.FDB)":
        st.markdown("### Upload do Arquivo Firebird")
        uploaded_file = st.file_uploader(
            "Selecione o arquivo .FDB", 
            type=["fdb"],
            help="Arquivo de banco de dados Firebird"
        )
        file_extension = "fdb"
    else:
        st.markdown("### Upload do Script SQL Firebird")
        uploaded_file = st.file_uploader(
            "Selecione o arquivo .SQL", 
            type=["sql"],
            help="Script SQL de dump do Firebird"
        )
        file_extension = "sql"
    
    if uploaded_file is not None:
        # Informações do arquivo
        file_size = uploaded_file.size / (1024 * 1024)  # Tamanho em MB
        file_details = {
            "Nome": uploaded_file.name,
            "Tamanho": f"{file_size:.2f} MB",
            "Tipo": uploaded_file.type,
            "Última modificação": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        st.markdown("#### Detalhes do Arquivo")
        st.json(file_details)
        
        # Análise preliminar
        if file_size > 100:
            st.warning("⚠️ Arquivo grande detectado. O processamento pode demorar vários minutos.")
        elif file_size > 500:
            st.error("🚨 Arquivo muito grande. Recomenda-se processamento em lote especial.")
        
        # Botão de conversão
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            convert_btn = st.button(
                "Iniciar Conversão", 
                type="primary", 
                disabled=(uploaded_file is None),
                use_container_width=True
            )
        
        if convert_btn:
            with tempfile.NamedTemporaryFile(delete=False, suffix=f".{file_extension}") as tmp_file:
                tmp_file.write(uploaded_file.read())
                file_path = tmp_file.name
            
            try:
                # Área de progresso
                st.markdown("---")
                st.markdown("#### Progresso da Conversão")
                
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # Processamento baseado no tipo de arquivo
                if file_extension == "fdb":
                    steps = [
                        "Validando arquivo .FDB...",
                        "Conectando ao banco Firebird...",
                        "Extraindo metadados...",
                        "Convertendo schema...",
                        "Processando dados...",
                        "Gerando script SQL...",
                        "Validando sintaxe PostgreSQL...",
                        "Finalizando..."
                    ]
                else:
                    steps = [
                        "Validando arquivo .SQL...",
                        "Analisando script Firebird...",
                        "Convertendo sintaxe...",
                        "Ajustando tipos de dados...",
                        "Adaptando funções...",
                        "Validando sintaxe PostgreSQL...",
                        "Finalizando..."
                    ]
                
                # Simular progresso (em produção, isso seria substituído por progresso real)
                for i, step in enumerate(steps):
                    status_text.text(f"{step} ({i+1}/{len(steps)})")
                    progress_bar.progress((i + 1) / len(steps))
                    time.sleep(0.5)  # Simula processamento
                
                # Realizar a conversão
                if file_extension == "fdb":
                    # Converter arquivo .FDB
                    sql_generator = SQLGenerator(file_path, batch_size)
                    sql_content, stats = sql_generator.generate_complete_sql()
                else:
                    # Converter arquivo .SQL
                    sql_converter = SQLConverter()
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        sql_content = f.read()
                    sql_content, stats = sql_converter.convert_sql_script(sql_content)
                
                # Mensagem de conclusão
                st.markdown("---")
                
                if not stats.get('errors'):
                    st.markdown('<div class="success-box">✅ Conversão concluída com sucesso!</div>', unsafe_allow_html=True)
                else:
                    st.markdown('<div class="warning-box">⚠️ Conversão concluída com erros. Verifique abaixo.</div>', unsafe_allow_html=True)
                
                # Estatísticas
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    if file_extension == "fdb":
                        st.metric("Tabelas", f"{stats.get('processed_tables', 0)}/{stats.get('total_tables', 0)}")
                    else:
                        st.metric("Linhas processadas", stats.get('total_lines', 0))
                with col2:
                    if file_extension == "fdb":
                        st.metric("Registros", f"{stats.get('total_rows', 0):,}")
                    else:
                        st.metric("Linhas convertidas", stats.get('converted_lines', 0))
                with col3:
                    error_count = len(stats.get('errors', []))
                    st.metric("Erros", error_count)
                with col4:
                    warning_count = len(stats.get('warnings', []))
                    st.metric("Avisos", warning_count)
                
                # Exibir erros e avisos se houver
                if stats.get('errors'):
                    with st.expander("Ver erros de conversão"):
                        for error in stats['errors']:
                            st.error(error)
                
                if stats.get('warnings'):
                    with st.expander("Ver avisos de conversão"):
                        for warning in stats['warnings']:
                            st.warning(warning)
                
                # Download do resultado
                st.markdown("---")
                st.markdown("#### Download do Script SQL PostgreSQL")
                
                st.download_button(
                    label="📥 Baixar Script PostgreSQL",
                    data=sql_content,
                    file_name="converted_database.sql",
                    mime="application/sql",
                    help="Download do script SQL compatível com PostgreSQL"
                )
                
            except Exception as e:
                st.error(f"Erro durante a conversão: {str(e)}")
                logger.exception("Erro durante a conversão")
            finally:
                os.unlink(file_path)

with tab2:
    st.markdown("### Monitoramento de Processamento")
    
    # Estatísticas em tempo real (simuladas)
    st.markdown("#### Estatísticas em Tempo Real")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### Progresso Atual")
        st.markdown("""
        <div class="progress-bar">
            <div class="progress-fill" style="width: 65%;">65%</div>
        </div>
        """, unsafe_allow_html=True)
        
        # Métricas de desempenho
        st.metric("Tabelas Processadas", "27/42", "5")
        st.metric("Registros Convertidos", "824,156", "12,387")
        st.metric("Tempo Decorrido", "1:45", "0:15")
    
    with col2:
        st.markdown("##### Utilização de Recursos")
        # Gráfico de utilização de CPU (simulado)
        cpu_data = pd.DataFrame({
            'Tempo': [f'{i}:00' for i in range(10)],
            'CPU (%)': [25, 32, 45, 60, 75, 68, 52, 45, 38, 30]
        })
        st.line_chart(cpu_data, x='Tempo', y='CPU (%)')
        
        # Gráfico de utilização de memória (simulado)
        memory_data = pd.DataFrame({
            'Tempo': [f'{i}:00' for i in range(10)],
            'Memória (MB)': [512, 550, 610, 720, 850, 820, 750, 680, 620, 580]
        })
        st.line_chart(memory_data, x='Tempo', y='Memória (MB)')
    
    # Log de atividades (simulado)
    st.markdown("#### Log de Atividades")
    log_data = pd.DataFrame({
        'Timestamp': [
            '2023-11-15 14:12:01', '2023-11-15 14:12:15', '2023-11-15 14:13:02', 
            '2023-11-15 14:13:45', '2023-11-15 14:14:30', '2023-11-15 14:15:10'
        ],
        'Nível': ['INFO', 'INFO', 'WARNING', 'INFO', 'INFO', 'ERROR'],
        'Mensagem': [
            'Iniciando processo de conversão',
            'Arquivo validado com sucesso',
            'Tipo de dados BLOB detectado - conversão limitada',
            'Tabela CLIENTES convertida (15,342 registros)',
            'Tabela PEDIDOS convertida (8,765 registros)',
            'Erro na tabela LOG: campo inválido'
        ]
    })
    st.dataframe(log_data, hide_index=True, use_container_width=True)

with tab3:
    st.markdown("### Histórico de Conversões")
    
    # Histórico simulado
    history_data = pd.DataFrame({
        'Data': ['2023-11-15', '2023-11-14', '2023-11-12', '2023-11-10', '2023-11-08'],
        'Arquivo': ['vendas.fdb', 'clientes.sql', 'produtos.fdb', 'estoque.sql', 'rh.fdb'],
        'Tipo': ['.FDB', '.SQL', '.FDB', '.SQL', '.FDB'],
        'Tamanho': ['245 MB', '128 MB', '89 MB', '512 MB', '76 MB'],
        'Tabelas': [42, 28, 19, 54, 22],
        'Registros': ['1.2M', '856K', '421K', '2.4M', '305K'],
        'Status': ['✅ Concluído', '✅ Concluído', '⚠️ Parcial', '✅ Concluído', '✅ Concluído'],
        'Duração': ['2:45', '1:20', '0:45', '4:10', '0:55']
    })
    
    st.dataframe(history_data, hide_index=True, use_container_width=True)
    
    # Opções de filtro
    col1, col2, col3 = st.columns(3)
    with col1:
        st.selectbox("Filtrar por status", ["Todos", "Concluído", "Parcial", "Falha"])
    with col2:
        st.date_input("Data inicial", value=pd.to_datetime("2023-11-01"))
    with col3:
        st.date_input("Data final", value=pd.to_datetime("2023-11-15"))
    
    # Botão para exportar histórico
    st.download_button(
        label="Exportar Histórico como CSV",
        data=history_data.to_csv(index=False).encode('utf-8'),
        file_name="historico_conversoes.csv",
        mime="text/csv"
    )

# Rodapé
st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: #666;'>Firebird to PostgreSQL Converter • v1.0 • Desenvolvido com Streamlit</div>", 
    unsafe_allow_html=True
)