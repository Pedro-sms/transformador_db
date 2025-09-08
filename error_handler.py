import logging
import sys
from datetime import datetime
from pathlib import Path

def setup_logging(log_level=logging.INFO, log_file='conversion.log'):
    """
    Configura sistema de logging com arquivo e console
    """
    # Criar diretório de logs se não existir
    log_path = Path(log_file)
    log_path.parent.mkdir(exist_ok=True)
    
    # Configurar formatação
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Handler para arquivo
    file_handler = logging.FileHandler(log_file, encoding='utf-8', mode='w')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    # Handler para console (apenas INFO e acima)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(logging.Formatter(
        '%(levelname)s: %(message)s'
    ))
    
    # Configurar logger raiz
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.handlers.clear()  # Limpar handlers existentes
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Reduzir verbosidade de bibliotecas externas
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    
    # Log inicial
    logging.info(f"Logging configurado - arquivo: {log_file}")
    logging.info(f"Sessão iniciada em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    return root_logger

def log_error(context: str, error: Exception, extra_info: str = None):
    """
    Registra erro com contexto adicional
    """
    logger = logging.getLogger(__name__)
    
    error_msg = f"{context}: {str(error)}"
    if extra_info:
        error_msg += f" | Info adicional: {extra_info}"
    
    logger.error(error_msg, exc_info=True)
    
    return error_msg

def log_warning(context: str, warning: str, extra_info: str = None):
    """
    Registra aviso com contexto
    """
    logger = logging.getLogger(__name__)
    
    warning_msg = f"{context}: {warning}"
    if extra_info:
        warning_msg += f" | Info adicional: {extra_info}"
    
    logger.warning(warning_msg)
    
    return warning_msg

def log_info(context: str, info: str, extra_info: str = None):
    """
    Registra informação com contexto
    """
    logger = logging.getLogger(__name__)
    
    info_msg = f"{context}: {info}"
    if extra_info:
        info_msg += f" | Info adicional: {extra_info}"
    
    logger.info(info_msg)
    
    return info_msg

def log_debug(context: str, debug_info: str, data: any = None):
    """
    Registra informação de debug
    """
    logger = logging.getLogger(__name__)
    
    debug_msg = f"{context}: {debug_info}"
    if data is not None:
        debug_msg += f" | Dados: {str(data)[:200]}..."  # Limitar tamanho
    
    logger.debug(debug_msg)
    
    return debug_msg

class ProgressTracker:
    """
    Classe para rastrear progresso e logging contextual
    """
    
    def __init__(self, operation_name: str):
        self.operation_name = operation_name
        self.logger = logging.getLogger(f"{__name__}.{operation_name}")
        self.start_time = datetime.now()
        self.current_step = None
        self.total_steps = 0
        self.completed_steps = 0
        
        self.logger.info(f"Iniciando operação: {operation_name}")
    
    def set_total_steps(self, total: int):
        """Define número total de etapas"""
        self.total_steps = total
        self.logger.info(f"Total de etapas definido: {total}")
    
    def start_step(self, step_name: str):
        """Inicia uma nova etapa"""
        if self.current_step:
            self.logger.warning(f"Etapa anterior '{self.current_step}' não foi finalizada")
        
        self.current_step = step_name
        self.logger.info(f"Iniciando etapa: {step_name}")
    
    def complete_step(self):
        """Completa a etapa atual"""
        if not self.current_step:
            self.logger.warning("Tentativa de completar etapa sem ter iniciado uma")
            return
        
        self.completed_steps += 1
        progress = (self.completed_steps / self.total_steps * 100) if self.total_steps > 0 else 0
        
        self.logger.info(f"Etapa concluída: {self.current_step} - Progresso: {progress:.1f}%")
        self.current_step = None
    
    def log_step_error(self, error: Exception, extra_info: str = None):
        """Registra erro na etapa atual"""
        step_context = f"{self.operation_name}.{self.current_step or 'unknown_step'}"
        return log_error(step_context, error, extra_info)
    
    def log_step_warning(self, warning: str, extra_info: str = None):
        """Registra aviso na etapa atual"""
        step_context = f"{self.operation_name}.{self.current_step or 'unknown_step'}"
        return log_warning(step_context, warning, extra_info)
    
    def log_step_info(self, info: str, extra_info: str = None):
        """Registra informação na etapa atual"""
        step_context = f"{self.operation_name}.{self.current_step or 'unknown_step'}"
        return log_info(step_context, info, extra_info)
    
    def finish_operation(self, success: bool = True):
        """Finaliza a operação"""
        duration = datetime.now() - self.start_time
        
        if success:
            self.logger.info(f"Operação '{self.operation_name}' concluída com sucesso em {duration}")
        else:
            self.logger.error(f"Operação '{self.operation_name}' falhou após {duration}")
        
        return duration

class ErrorCollector:
    """
    Coleta e organiza erros durante a migração
    """
    
    def __init__(self):
        self.errors = []
        self.warnings = []
        self.info_messages = []
        self.logger = logging.getLogger(f"{__name__}.ErrorCollector")
    
    def add_error(self, context: str, error: Exception, extra_info: str = None):
        """Adiciona erro à coleção"""
        error_msg = log_error(context, error, extra_info)
        self.errors.append({
            'context': context,
            'message': str(error),
            'extra_info': extra_info,
            'timestamp': datetime.now(),
            'full_message': error_msg
        })
        return error_msg
    
    def add_warning(self, context: str, warning: str, extra_info: str = None):
        """Adiciona aviso à coleção"""
        warning_msg = log_warning(context, warning, extra_info)
        self.warnings.append({
            'context': context,
            'message': warning,
            'extra_info': extra_info,
            'timestamp': datetime.now(),
            'full_message': warning_msg
        })
        return warning_msg
    
    def add_info(self, context: str, info: str, extra_info: str = None):
        """Adiciona informação à coleção"""
        info_msg = log_info(context, info, extra_info)
        self.info_messages.append({
            'context': context,
            'message': info,
            'extra_info': extra_info,
            'timestamp': datetime.now(),
            'full_message': info_msg
        })
        return info_msg
    
    def get_summary(self) -> dict:
        """Retorna resumo dos erros/avisos coletados"""
        return {
            'total_errors': len(self.errors),
            'total_warnings': len(self.warnings),
            'total_info': len(self.info_messages),
            'has_critical_errors': len(self.errors) > 0
        }
    
    def get_report(self) -> str:
        """Gera relatório textual dos problemas encontrados"""
        report = []
        
        if self.errors:
            report.append("ERROS ENCONTRADOS:")
            report.append("=" * 50)
            for i, error in enumerate(self.errors, 1):
                report.append(f"{i}. {error['context']}: {error['message']}")
                if error['extra_info']:
                    report.append(f"   Info: {error['extra_info']}")
                report.append(f"   Timestamp: {error['timestamp'].strftime('%H:%M:%S')}")
                report.append("")
        
        if self.warnings:
            report.append("AVISOS:")
            report.append("=" * 50)
            for i, warning in enumerate(self.warnings, 1):
                report.append(f"{i}. {warning['context']}: {warning['message']}")
                if warning['extra_info']:
                    report.append(f"   Info: {warning['extra_info']}")
                report.append("")
        
        summary = self.get_summary()
        report.append("RESUMO:")
        report.append("=" * 50)
        report.append(f"Erros: {summary['total_errors']}")
        report.append(f"Avisos: {summary['total_warnings']}")
        report.append(f"Informações: {summary['total_info']}")
        
        return "\n".join(report)
    
    def clear(self):
        """Limpa todas as mensagens coletadas"""
        self.errors.clear()
        self.warnings.clear()
        self.info_messages.clear()
        self.logger.info("ErrorCollector limpo")
