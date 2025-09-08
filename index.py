import streamlit as st
import pandas as pd
import time
import json
from datetime import datetime
import io
import tempfile
import os
import logging
import traceback
from pathlib import Path

# Importações dos módulos de conversão
try:
    from sql_converter import SQLConverter
    from sql_generator import SQLGenerator
    from firebird_client import FirebirdClient
    from progress_manager import ProgressManager
    from error_handler import setup_logging, ErrorCollector
except ImportError as e:
    st.error(f"Erro ao importar módulos: {e}")
    st.stop()

# Configuração de logging para Streamlit
@st.cache_resource
def setup_streamlit_logging():
    return setup_logging(log_level=logging.INFO, log_file='streamlit_migration.log')

logger = setup_streamlit_logging()

# Configuração da página
st.set_page_config(
    page_title="Firebird to PostgreSQL Converter",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personalizado melhorado
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #1E88E5;
        text-align: center;
        margin-bottom: 2rem;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
    }
    .sub-header {
        font-size: 1.5rem;
        color: #0D47A1;
        margin-bottom: 1rem;
        border-bottom: 2px solid #E3F2FD;
        padding-bottom: 0.5rem;
    }
    .info-box {
        background: linear-gradient(135deg, #E3F2FD, #BBDEFB);
        padding: 1.5rem;
        border-radius: 10px;
        margin-bottom: 1.5rem;
        border-left: 5px solid #2196F3;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
    }
    .success-box {
        background: linear-gradient(135deg, #E8F5E9, #C8E6C9);
        padding: 1.5rem;
        border-radius: 10px;
        margin-bottom: 1.5rem;
        border-left: 5px solid #4CAF50;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
    }
    .warning-box {
        background: linear-gradient(135deg, #FFF8E1, #FFECB3);
        padding: 1.5rem;
        border-radius: 10px;
        margin-bottom: 1.5rem;
        border-left: 5px solid #FF9800;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
    }
    .error-box {
        background: linear-gradient(135deg, #FFEBEE, #FFCDD2);
        padding: 1.5rem;
        border-radius: 10px;
        margin-bottom: 1.5rem;
        border-left: 5px solid #F44336;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
    }
    .progress-container {
        background-color: #F5F5F5;
        border-radius: 10px;
        padding: 1rem;
        margin: 1rem 0;
        box-shadow: inset 0 2px 5px rgba(0,0,0,0.1);
    }
    .progress-bar {
        height: 30px;
        border-radius: 15px;
        background-color: #E0E0E0;
        overflow: hidden;
        position: relative;
    }
    .progress-fill {
        height: 100%;
        border-radius: 15px;
        background: linear-gradient(90deg, #4CAF50, #66BB6A);
        text-align: center;
        color: white;
        line-height: 30px;
        font-weight: bold;
        transition: width 0.5s ease-in-out;
        box-shadow: inset 0 2px 5px rgba(0,0,0,0.2);
    }
    .metric-card {
        background: linear-gradient(135deg, #FFFFFF, #F8F9FA);
        padding: 1.5rem;
        border-radius: 10px;
        text-align: center;
        margin: 0.5rem;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
        border: 1px solid #E0E0E0;
        transition: transform 0.2s ease;
    }
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(0,0,0,0.15);
    }
    .metric-number {
        font-size: 2rem;
        font-weight: bold;
        color: #1E88E5;
        margin-bottom: 0.5rem;
    }
    .metric-label {
        color: #666;
        font-size: 0.9rem;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    .stButton>button {
        width: 100%;
        background: linear-gradient(45deg, #1E88E5, #42A5F5);
        color: white;
        border: none;
        border-radius: 10px;
        padding: 0.75rem 1.5rem;
        font-weight: bold;
        box-shadow: 0 4px 15px rgba(30, 136, 229, 0.3);
        transition: all 0.3s ease;
    }
    .stButton>button:hover {
        background: linear-gradient(45deg, #1565C0, #1E88E5);
        box-shadow: 0 6px 20px rgba(30, 136, 229, 0.4);
        transform: translateY(-2px);
    }
    .upload-area {
        border: 2px dashed #1E88E5;
        border-radius: 10px;
        padding: 2rem;
        text-align: center;
        background: linear-gradient(135deg, #F8F9FA, #FFFFFF);
        margin: 1rem 0;
        transition: all 0.3s ease;
    }
    .upload-area:hover {
        border-color: #1565C0;
        background: linear-gradient(135deg, #E3F2FD, #F8F9FA);
    }
    .step-indicator {
        display: flex;
        justify-content: space-between;
        margin: 2rem 0;
        padding: 0 1rem;
    }
    .step {
        flex: 1;
        text-align: center;
        position: relative;
    }
    .step-number {
        width: 40px;
        height: 40px;
        border-radius: 50%;
        background: #E0E0E0;
        color: #666;
        display: flex;
        align-items: center;
        justify-content: center;
        margin: 0 auto 0.5rem;
        font-weight: bold;
    }
    .step.active .step-number {
        background: linear-gradient(45deg, #1E88E5, #42A5F5);
        color: white;
    }
    .step.completed .step-number {
        background: linear-gradient(45deg, #4CAF50, #66BB6A);
        color: white;
    }
    .log-container {
        background-color: #263238;
        color: #E0E0E0;
        border-radius: 10px;
        padding: 1rem;
        max-height: 300px;
        overflow-y: auto;
        font-family: 'Courier New', monospace;
        margin: 1rem 0;
    }
    .sidebar-logo {
        text-align: center;
        padding: 1rem;
        border-radius: 10px;
        background: linear-gradient(135deg, #E3F2FD, #BBDEFB);
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

# Estado da sessão para controle de estado
if 'conversion_state' not in st.session_state:
    st.session_state.conversion_state = 'idle'
if 'conversion_progress' not in st.session_state:
    st.session_state.conversion_progress = 0
if 'conversion_results' not in st.session_state:
    st.session_state.conversion_results = None
if 'error_collector' not in st.session_state:
    st.session_state.error_collector = ErrorCollector()

# Funções auxiliares
def format_file_size(size_bytes):
    """Formata tamanho do arquivo em formato legível"""
    if size_bytes == 0:
        return "0B"
    size_names = ["B", "KB", "MB", "GB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    return f"{size_bytes:.2f}{size_names[i]}"

def validate_firebird_file(file_path):
    """Valida se o arquivo Firebird é válido"""
    try:
        with FirebirdClient(file_path) as client:
            connection_info = client.test_connection()
            return connection_info['status'] == 'success', connection_info
    except Exception as e:
        return False, {'status': 'error', 'message': str(e)}

def create_progress_callback():
    """Cria callback para atualização de progresso"""
    def progress_callback(progress_info):
        st.session_state.conversion_progress = progress_info.get('overall_percentage', 0)
        return True
    return progress_callback

# Cabeçalho da aplicação
st.markdown('<h1 class="main-header">🔄 Firebird to PostgreSQL Converter</h1>', unsafe_allow_html=True)
st.markdown("### Converta seus bancos Firebird (.FDB) ou scripts SQL para PostgreSQL com facilidade e precisão")

# Barra lateral para configurações
with st.sidebar:
    st.markdown("""
    <div class="sidebar-logo">
        <h3 style="color: #1E88E5; margin: 0;">⚙️ Configurações</h3>
    </div>
    """, unsafe_allow_html=True)
    
    # Opções de conversão
    st.markdown("#### 🔧 Opções de Conversão")
    batch_size = st.slider(
        "Tamanho do lote para processamento", 
        min_value=100, 
        max_value=10000, 
        value=1000, 
        step=100,
        help="Número de registros processados por vez. Valores menores usam menos memória."
    )
    
    include_data = st.checkbox(
        "Incluir dados das tabelas", 
        value=True, 
        help="Incluir os dados além do schema"
    )
    
    # Configurações do PostgreSQL
    st.markdown("#### 🐘 Configurações do PostgreSQL")
    pg_version = st.selectbox("Versão do PostgreSQL", ["16", "15", "14", "13", "12"], index=0)
    encoding = st.selectbox("Codificação", ["UTF8", "LATIN1", "WIN1252"])
    
    # Opções avançadas
    with st.expander("🔍 Opções Avançadas"):
        include_indexes = st.checkbox("Incluir índices", value=True)
        include_constraints = st.checkbox("Incluir constraints", value=True)
        include_triggers = st.checkbox("Incluir triggers (requer revisão)", value=False)
        use_transactions = st.checkbox("Usar transações", value=True)
        validate_data = st.checkbox("Validar dados", value=True)
    
    # Informações do sistema
    st.markdown("---")
    st.markdown("#### ℹ️ Informações")
    st.info("💡 **Dica:** Para arquivos grandes (>100MB), considere usar lotes menores.")
    
    if st.button("🔄 Limpar Cache"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.success("Cache limpo!")

# Layout principal com abas melhoradas
tab1, tab2, tab3, tab4 = st.tabs(["🔄 Conversão", "📊 Monitoramento", "📋 Histórico", "❓ Ajuda"])

with tab1:
    # Indicador de passos
    st.markdown("""
    <div class="step-indicator">
        <div class="step active">
            <div class="step-number">1</div>
            <span>Upload</span>
        </div>
        <div class="step">
            <div class="step-number">2</div>
            <span>Validação</span>
        </div>
        <div class="step">
            <div class="step-number">3</div>
            <span>Conversão</span>
        </div>
        <div class="step">
            <div class="step-number">4</div>
            <span>Download</span>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Seletor de tipo de arquivo
    st.markdown('<h2 class="sub-header">📁 Selecione o Tipo de Arquivo</h2>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    with col1:
        file_type_fdb = st.button(
            "🗄️ Arquivo Firebird (.FDB)",
            help="Arquivo de banco de dados Firebird completo",
            use_container_width=True
        )
    with col2:
        file_type_sql = st.button(
            "📜 Script SQL Firebird (.SQL)",
            help="Script SQL exportado do Firebird",
            use_container_width=True
        )
    
    # Controle do tipo de arquivo selecionado
    if file_type_fdb:
        st.session_state.selected_file_type = "fdb"
    elif file_type_sql:
        st.session_state.selected_file_type = "sql"
    
    if 'selected_file_type' in st.session_state:
        file_type = st.session_state.selected_file_type
        
        # Área de upload personalizada
        st.markdown("---")
        if file_type == "fdb":
            st.markdown('<h2 class="sub-header">🗄️ Upload do Arquivo Firebird</h2>', unsafe_allow_html=True)
            uploaded_file = st.file_uploader(
                "Selecione o arquivo .FDB", 
                type=["fdb"],
                help="Arquivo de banco de dados Firebird (.fdb)",
                label_visibility="collapsed"
            )
        else:
            st.markdown('<h2 class="sub-header">📜 Upload do Script SQL Firebird</h2>', unsafe_allow_html=True)
            uploaded_file = st.file_uploader(
                "Selecione o arquivo .SQL", 
                type=["sql", "txt"],
                help="Script SQL exportado do Firebird (.sql)",
                label_visibility="collapsed"
            )
        
        if uploaded_file is not None:
            # Análise do arquivo
            file_size = uploaded_file.size
            file_size_mb = file_size / (1024 * 1024)
            
            st.markdown('<div class="info-box">', unsafe_allow_html=True)
            st.markdown("#### 📊 Informações do Arquivo")
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-number">{uploaded_file.name}</div>
                    <div class="metric-label">Nome do Arquivo</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-number">{format_file_size(file_size)}</div>
                    <div class="metric-label">Tamanho</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col3:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-number">{uploaded_file.type or 'N/A'}</div>
                    <div class="metric-label">Tipo MIME</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col4:
                estimated_time = max(1, int(file_size_mb / 10))  # Estimativa: 10MB por minuto
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-number">~{estimated_time}min</div>
                    <div class="metric-label">Tempo Estimado</div>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Alertas baseados no tamanho
            if file_size_mb > 500:
                st.markdown('<div class="error-box">🚨 <strong>Arquivo muito grande!</strong> Arquivos acima de 500MB podem causar problemas de memória. Considere processar em partes menores.</div>', unsafe_allow_html=True)
            elif file_size_mb > 100:
                st.markdown('<div class="warning-box">⚠️ <strong>Arquivo grande detectado.</strong> O processamento pode demorar vários minutos. Certifique-se de ter uma conexão estável.</div>', unsafe_allow_html=True)
            elif file_size_mb < 0.1:
                st.markdown('<div class="info-box">ℹ️ <strong>Arquivo pequeno.</strong> O processamento será rápido!</div>', unsafe_allow_html=True)
            
            # Validação prévia para arquivos .FDB
            if file_type == "fdb":
                if st.button("🔍 Validar Arquivo Firebird", help="Verifica se o arquivo pode ser aberto"):
                    with tempfile.NamedTemporaryFile(delete=False, suffix=".fdb") as tmp_file:
                        tmp_file.write(uploaded_file.read())
                        tmp_file_path = tmp_file.name
                    
                    try:
                        with st.spinner("Validando arquivo Firebird..."):
                            is_valid, validation_info = validate_firebird_file(tmp_file_path)
                        
                        if is_valid:
                            st.success("✅ Arquivo Firebird válido!")
                            st.json(validation_info)
                        else:
                            st.error(f"❌ Arquivo inválido: {validation_info.get('message', 'Erro desconhecido')}")
                    finally:
                        os.unlink(tmp_file_path)
            
            # Botão de conversão principal
            st.markdown("---")
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                if st.button(
                    "🚀 Iniciar Conversão", 
                    type="primary", 
                    use_container_width=True,
                    disabled=(st.session_state.conversion_state == 'running')
                ):
                    st.session_state.conversion_state = 'running'
                    st.session_state.conversion_progress = 0
                    st.session_state.error_collector.clear()
                    st.rerun()
            
            # Processamento da conversão
            if st.session_state.conversion_state == 'running':
                # Criar arquivo temporário
                with tempfile.NamedTemporaryFile(delete=False, suffix=f".{file_type}") as tmp_file:
                    uploaded_file.seek(0)  # Reset file pointer
                    tmp_file.write(uploaded_file.read())
                    file_path = tmp_file.name
                
                try:
                    # Container para progresso
                    progress_container = st.empty()
                    log_container = st.empty()
                    
                    def update_progress(step, progress, message):
                        with progress_container.container():
                            st.markdown(f'<div class="progress-container">', unsafe_allow_html=True)
                            st.markdown(f"**{step}**")
                            st.markdown(f'''
                            <div class="progress-bar">
                                <div class="progress-fill" style="width: {progress}%;">{progress:.1f}%</div>
                            </div>
                            ''', unsafe_allow_html=True)
                            st.markdown(f"*{message}*")
                            st.markdown('</div>', unsafe_allow_html=True)
                    
                    # Executar conversão baseada no tipo
                    if file_type == "fdb":
                        # Conversão de arquivo .FDB
                        update_progress("Iniciando...", 0, "Preparando conversão do banco Firebird")
                        time.sleep(0.5)
                        
                        generator = SQLGenerator(
                            db_path=file_path, 
                            batch_size=batch_size,
                            include_data=include_data
                        )
                        
                        update_progress("Conectando...", 10, "Estabelecendo conexão com o banco Firebird")
                        
                        # Executar conversão com callback de progresso
                        progress_manager = ProgressManager(create_progress_callback())
                        
                        sql_content, stats = generator.generate_complete_sql()
                        
                        update_progress("Finalizando...", 100, "Conversão concluída!")
                        
                    else:
                        # Conversão de arquivo .SQL
                        update_progress("Lendo arquivo...", 20, "Carregando script SQL Firebird")
                        
                        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                            sql_content = f.read()
                        
                        update_progress("Convertendo...", 60, "Aplicando regras de conversão")
                        
                        sql_converter = SQLConverter()
                        sql_content, stats = sql_converter.convert_sql_script(sql_content)
                        
                        update_progress("Finalizando...", 100, "Conversão concluída!")
                    
                    # Armazenar resultados
                    st.session_state.conversion_results = {
                        'sql_content': sql_content,
                        'stats': stats,
                        'file_name': uploaded_file.name
                    }
                    st.session_state.conversion_state = 'completed'
                    
                    time.sleep(1)  # Pequena pausa para visualizar 100%
                    st.rerun()
                    
                except Exception as e:
                    st.session_state.conversion_state = 'error'
                    st.session_state.error_collector.add_error("Conversão", e)
                    st.error(f"❌ Erro durante a conversão: {str(e)}")
                    st.expander("Detalhes do erro").code(traceback.format_exc())
                finally:
                    if os.path.exists(file_path):
                        os.unlink(file_path)
            
            # Exibir resultados
            elif st.session_state.conversion_state == 'completed' and st.session_state.conversion_results:
                results = st.session_state.conversion_results
                stats = results['stats']
                
                st.markdown("---")
                
                # Status da conversão
                error_count = len(stats.get('errors', []))
                warning_count = len(stats.get('warnings', []))
                
                if error_count == 0:
                    st.markdown('<div class="success-box">✅ <strong>Conversão concluída com sucesso!</strong> O script PostgreSQL está pronto para uso.</div>', unsafe_allow_html=True)
                else:
                    st.markdown('<div class="warning-box">⚠️ <strong>Conversão concluída com erros.</strong> Revise os problemas encontrados abaixo.</div>', unsafe_allow_html=True)
                
                # Estatísticas detalhadas
                st.markdown('<h2 class="sub-header">📈 Estatísticas da Conversão</h2>', unsafe_allow_html=True)
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    if file_type == "fdb":
                        tables_processed = stats.get('processed_tables_schema', 0)
                        total_tables = stats.get('total_tables', 0)
                        st.metric("Tabelas Processadas", f"{tables_processed}/{total_tables}")
                    else:
                        st.metric("Linhas Processadas", stats.get('total_lines', 0))
                
                with col2:
                    if file_type == "fdb":
                        rows_migrated = stats.get('processed_rows', 0)
                        st.metric("Registros Migrados", f"{rows_migrated:,}")
                    else:
                        st.metric("Linhas Convertidas", stats.get('converted_lines', 0))
                
                with col3:
                    st.metric("Erros", error_count, delta=None if error_count == 0 else f"-{error_count}")
                
                with col4:
                    st.metric("Avisos", warning_count, delta=None if warning_count == 0 else f"-{warning_count}")
                
                # Informações adicionais para .FDB
                if file_type == "fdb":
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Sequences", stats.get('processed_sequences', 0))
                    with col2:
                        st.metric("Views", stats.get('processed_views', 0))
                    with col3:
                        st.metric("Constraints", stats.get('processed_constraints', 0))
                    with col4:
                        duration = stats.get('duration')
                        if duration:
                            st.metric("Duração", str(duration).split('.')[0])
                
                # Exibir problemas encontrados
                if stats.get('errors'):
                    with st.expander(f"❌ Ver {len(stats['errors'])} erro(s) de conversão"):
                        for i, error in enumerate(stats['errors'], 1):
                            st.error(f"**Erro {i}:** {error}")
                
                if stats.get('warnings'):
                    with st.expander(f"⚠️ Ver {len(stats['warnings'])} aviso(s) de conversão"):
                        for i, warning in enumerate(stats['warnings'], 1):
                            st.warning(f"**Aviso {i}:** {warning}")
                
                # Prévia do SQL gerado
                st.markdown('<h2 class="sub-header">👀 Prévia do SQL Gerado</h2>', unsafe_allow_html=True)
                sql_preview = results['sql_content'][:2000]
                st.code(sql_preview + ("..." if len(results['sql_content']) > 2000 else ""), language="sql")
                
                # Downloads
                st.markdown('<h2 class="sub-header">📥 Downloads</h2>', unsafe_allow_html=True)
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Download do script SQL principal
                    output_filename = f"{Path(results['file_name']).stem}_postgresql.sql"
                    st.download_button(
                        label="📥 Baixar Script PostgreSQL",
                        data=results['sql_content'],
                        file_name=output_filename,
                        mime="application/sql",
                        help="Script SQL compatível com PostgreSQL",
                        use_container_width=True
                    )
                
                with col2:
                    # Download do relatório
                    if file_type == "fdb":
                        try:
                            generator = SQLGenerator(file_path="", batch_size=batch_size)
                            generator.stats = stats
                            report = generator.generate_migration_report()
                        except:
                            report = f"Relatório de Conversão\n\nArquivo: {results['file_name']}\nErros: {error_count}\nAvisos: {warning_count}"
                    else:
                        report = f"Relatório de Conversão SQL\n\nArquivo: {results['file_name']}\nLinhas: {stats.get('total_lines', 0)}\nErros: {error_count}\nAvisos: {warning_count}"
                    
                    report_filename = f"{Path(results['file_name']).stem}_report.txt"
                    st.download_button(
                        label="📋 Baixar Relatório",
                        data=report,
                        file_name=report_filename,
                        mime="text/plain",
                        help="Relatório detalhado da conversão",
                        use_container_width=True
                    )
                
                # Botão para nova conversão
                st.markdown("---")
                if st.button("🔄 Nova Conversão", use_container_width=True):
                    st.session_state.conversion_state = 'idle'
                    st.session_state.conversion_results = None
                    if 'selected_file_type' in st.session_state:
                        del st.session_state.selected_file_type
                    st.rerun()

with tab2:
    st.markdown('<h2 class="sub-header">📊 Monitoramento do Sistema</h2>', unsafe_allow_html=True)
    
    # Status atual
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 🔄 Status Atual")
        status_map = {
            'idle': ('⏸️ Aguardando', 'info'),
            'running': ('🔄 Processando', 'success'),
            'completed': ('✅ Concluído', 'success'),
            'error': ('❌ Erro', 'error')
        }
        
        status_text, status_icon = status_map.get(st.session_state.conversion_state, ('❓ Desconhecido', 'warning'))
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-number">{status_text}</div>
            <div class="metric-label">Status da Conversão</div>
        </div>
        """, unsafe_allow_html=True)
        
        # Barra de progresso
        if st.session_state.conversion_state == 'running':
            st.markdown('<div class="progress-container">', unsafe_allow_html=True)
            st.markdown(f'''
            <div class="progress-bar">
                <div class="progress-fill" style="width: {st.session_state.conversion_progress}%;">
                    {st.session_state.conversion_progress:.1f}%
                </div>
            </div>
            ''', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown("#### 📈 Estatísticas do Sistema")
        
        # Simulação de métricas do sistema (em produção, usar psutil)
        import psutil
        try:
            cpu_percent = psutil.cpu_percent()
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-number">{cpu_percent}%</div>
                <div class="metric-label">Uso de CPU</div>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-number">{memory.percent}%</div>
                <div class="metric-label">Uso de Memória</div>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-number">{disk.percent}%</div>
                <div class="metric-label">Uso de Disco</div>
            </div>
            """, unsafe_allow_html=True)
            
        except ImportError:
            st.warning("Instale a biblioteca psutil para monitoramento do sistema: pip install psutil")
    
    # Logs em tempo real
    st.markdown("#### 📝 Logs de Execução")
    
    # Container para logs
    log_container = st.container()
    
    # Simulação de leitura de logs (em produção, ler do arquivo de log real)
    try:
        with open('streamlit_migration.log', 'r') as log_file:
            logs = log_file.readlines()[-100:]  # Últimas 100 linhas
    except FileNotFoundError:
        logs = ["Arquivo de log não encontrado. Os logs serão exibidos aqui."]
    
    with log_container:
        st.markdown('<div class="log-container">', unsafe_allow_html=True)
        for log in logs:
            st.text(log.strip())
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Botão para atualizar logs
    if st.button("🔄 Atualizar Logs"):
        st.rerun()

with tab3:
    st.markdown('<h2 class="sub-header">📋 Histórico de Conversões</h2>', unsafe_allow_html=True)
    
    # Simulação de histórico (em produção, usar banco de dados)
    history_data = [
        {"data": "2024-01-15 10:30", "arquivo": "backup.fdb", "tamanho": "250MB", "status": "✅ Concluído"},
        {"data": "2024-01-14 15:22", "arquivo": "vendas.sql", "tamanho": "15MB", "status": "⚠️ Com avisos"},
        {"data": "2024-01-13 09:45", "arquivo": "clientes.fdb", "tamanho": "120MB", "status": "✅ Concluído"},
    ]
    
    if history_data:
        df = pd.DataFrame(history_data)
        st.dataframe(
            df,
            column_config={
                "data": "Data/Hora",
                "arquivo": "Arquivo",
                "tamanho": "Tamanho",
                "status": "Status"
            },
            hide_index=True,
            use_container_width=True
        )
        
        # Estatísticas do histórico
        st.markdown("#### 📊 Estatísticas do Histórico")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total de Conversões", len(history_data))
        with col2:
            success_count = sum(1 for item in history_data if "✅" in item["status"])
            st.metric("Conversões Bem-sucedidas", success_count)
        with col3:
            total_size = sum(float(item["tamanho"].replace("MB", "")) for item in history_data)
            st.metric("Total Processado", f"{total_size}MB")
    else:
        st.info("📝 Nenhum histórico de conversão disponível.")

with tab4:
    st.markdown('<h2 class="sub-header">❓ Ajuda e Documentação</h2>', unsafe_allow_html=True)
    
    st.markdown("""
    <div class="info-box">
        <h4>📚 Guia de Uso</h4>
        <p>Esta ferramenta converte bancos de dados Firebird (.FDB) ou scripts SQL Firebird para PostgreSQL.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # FAQ
    with st.expander("🔍 Como usar a ferramenta?"):
        st.markdown("""
        1. Selecione o tipo de arquivo (FDB ou SQL)
        2. Faça o upload do arquivo
        3. Configure as opções de conversão na barra lateral
        4. Clique em "Iniciar Conversão"
        5. Aguarde o processamento e faça o download do resultado
        """)
    
    with st.expander("⚠️ Quais são as limitações conhecidas?"):
        st.markdown("""
        - Alguns tipos de dados do Firebird podem não ter equivalência exata no PostgreSQL
        - Triggers complexos podem requerer ajustes manuais
        - Blobs muito grandes podem causar problemas de performance
        - A codificação de caracteres deve ser verificada após a conversão
        """)
    
    with st.expander("🔧 Quais opções de configuração estão disponíveis?"):
        st.markdown("""
        - **Tamanho do lote**: Controla quantos registros são processados por vez
        - **Incluir dados**: Se desmarcado, apenas o schema será convertido
        - **Versão do PostgreSQL**: Seleciona a sintaxe SQL adequada
        - **Codificação**: Define a codificação de caracteres do output
        """)
    
    with st.expander("❓ Onde obter suporte?"):
        st.markdown("""
        Para problemas técnicos ou dúvidas:
        - Consulte a documentação oficial
        - Verifique os logs de execução na aba de Monitoramento
        - Entre em contato com a equipe de suporte
        """)
    
    # Informações da versão
    st.markdown("---")
    st.markdown("#### ℹ️ Informações da Versão")
    st.info("""
    - **Versão:** 2.1.0
    - **Última atualização:** 15/01/2024
    - **Compatibilidade:** Firebird 2.5+, PostgreSQL 12+
    """)

# Rodapé
st.markdown("---")
st.markdown(
    '<div style="text-align: center; color: #666; padding: 1rem;">'
    'Firebird to PostgreSQL Converter © 2024 | Desenvolvido com Streamlit'
    '</div>',
    unsafe_allow_html=True
)
