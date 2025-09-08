import time
import logging
from typing import Optional, Callable, Dict, Any

class ProgressManager:
    def __init__(self, update_callback: Optional[Callable] = None):
        self.current_table = None
        self.processed_tables = 0
        self.total_tables = 0
        self.processed_rows = 0
        self.total_rows = 0
        self.table_rows = 0
        self.processed_table_rows = 0
        self.start_time = None
        self.elapsed_time = 0
        self.update_callback = update_callback
        self.logger = logging.getLogger(__name__)
    
    def start_conversion(self, total_tables: int, total_rows: int):
        """Inicia o processo de conversão"""
        self.total_tables = total_tables
        self.total_rows = total_rows
        self.processed_tables = 0
        self.processed_rows = 0
        self.start_time = time.time()
        self.logger.info(f"Iniciando conversão de {total_tables} tabelas com {total_rows:,} linhas totais")
        self._trigger_callback()
    
    def start_table(self, table_name: str, row_count: int):
        """Inicia processamento de uma tabela"""
        self.current_table = table_name
        self.table_rows = row_count
        self.processed_table_rows = 0
        self.logger.info(f"Iniciando tabela {table_name} com {row_count:,} linhas")
        self._trigger_callback()
    
    def update_table_progress(self, processed_rows: int):
        """Atualiza progresso da tabela atual"""
        if processed_rows > self.table_rows:
            self.logger.warning(f"Linhas processadas ({processed_rows:,}) excedem o total da tabela ({self.table_rows:,})")
            processed_rows = self.table_rows
        
        # Calcular diferença para atualizar total
        rows_added = processed_rows - self.processed_table_rows
        self.processed_table_rows = processed_rows
        self.processed_rows += rows_added
        
        # Garantir que não ultrapasse o total
        if self.processed_rows > self.total_rows:
            self.processed_rows = self.total_rows
        
        self._trigger_callback()
    
    def finish_table(self):
        """Finaliza processamento da tabela atual"""
        self.processed_tables += 1
        self.logger.info(f"Tabela {self.current_table} concluída. {self.processed_tables}/{self.total_tables} tabelas processadas")
        self.current_table = None
        self.table_rows = 0
        self.processed_table_rows = 0
        self._trigger_callback()
    
    def get_overall_progress(self) -> float:
        """Calcula progresso geral (0.0 a 1.0)"""
        if self.total_tables == 0 or self.total_rows == 0:
            return 0.0
        
        # Progresso baseado principalmente em dados (70%) e tabelas (30%)
        table_progress = self.processed_tables / self.total_tables * 0.3
        
        if self.total_rows > 0:
            data_progress = self.processed_rows / self.total_rows * 0.7
        else:
            data_progress = 0.0
        
        return min(table_progress + data_progress, 1.0)  # Garante que não ultrapasse 100%
    
    def get_detailed_progress(self) -> Dict[str, Any]:
        """Retorna informações detalhadas do progresso"""
        overall_percentage = self.get_overall_progress() * 100
        
        return {
            'current_table': self.current_table or 'Nenhuma',
            'processed_tables': self.processed_tables,
            'total_tables': self.total_tables,
            'processed_rows': self.processed_rows,
            'total_rows': self.total_rows,
            'table_progress': f"{self.processed_table_rows:,}/{self.table_rows:,}" if self.current_table else "0/0",
            'overall_percentage': f"{overall_percentage:.2f}%",
            'elapsed_time': time.time() - self.start_time if self.start_time else 0,
            'estimated_remaining': self.get_estimated_time_remaining()
        }
    
    def get_estimated_time_remaining(self) -> str:
        """Estima tempo restante baseado no progresso atual"""
        if not self.start_time or self.processed_rows == 0:
            return "Estimativa indisponível"
        
        elapsed = time.time() - self.start_time
        
        # Calcular taxa baseada em dados processados
        if self.processed_rows > 0:
            rows_per_second = self.processed_rows / elapsed
            remaining_rows = self.total_rows - self.processed_rows
            
            if rows_per_second <= 0:
                return "Estimativa indisponível"
            
            seconds_remaining = remaining_rows / rows_per_second
        else:
            # Fallback baseado em tabelas
            if self.processed_tables > 0:
                tables_per_second = self.processed_tables / elapsed
                remaining_tables = self.total_tables - self.processed_tables
                
                if tables_per_second <= 0:
                    return "Estimativa indisponível"
                
                seconds_remaining = remaining_tables / tables_per_second
            else:
                return "Estimativa indisponível"
        
        # Formatar tempo para legibilidade
        return self._format_duration(seconds_remaining)
    
    def _format_duration(self, seconds: float) -> str:
        """Formata duração em segundos para formato legível"""
        if seconds < 0:
            return "Estimativa indisponível"
        
        hours, remainder = divmod(int(seconds), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        if hours > 24:
            days = hours // 24
            hours = hours % 24
            return f"{days}d {hours}h {minutes}m"
        elif hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Retorna estatísticas de performance"""
        if not self.start_time:
            return {}
        
        elapsed = time.time() - self.start_time
        
        stats = {
            'elapsed_time': self._format_duration(elapsed),
            'elapsed_seconds': elapsed,
            'rows_per_second': self.processed_rows / elapsed if elapsed > 0 else 0,
            'tables_per_second': self.processed_tables / elapsed if elapsed > 0 else 0,
        }
        
        # Adicionar estimativas
        if stats['rows_per_second'] > 0:
            remaining_rows = self.total_rows - self.processed_rows
            stats['estimated_remaining_seconds'] = remaining_rows / stats['rows_per_second']
            stats['estimated_total_time'] = elapsed + stats['estimated_remaining_seconds']
        
        return stats
    
    def _trigger_callback(self):
        """Dispara callback de atualização se definido"""
        if self.update_callback:
            try:
                self.update_callback(self.get_detailed_progress())
            except Exception as e:
                self.logger.error(f"Erro no callback de progresso: {e}")
    
    def log_summary(self):
        """Registra resumo final no log"""
        if not self.start_time:
            return
        
        total_time = time.time() - self.start_time
        stats = self.get_performance_stats()
        
        self.logger.info("="*60)
        self.logger.info("RESUMO DA MIGRAÇÃO")
        self.logger.info("="*60)
        self.logger.info(f"Tabelas processadas: {self.processed_tables}/{self.total_tables}")
        self.logger.info(f"Registros migrados: {self.processed_rows:,}/{self.total_rows:,}")
        self.logger.info(f"Tempo total: {self._format_duration(total_time)}")
        self.logger.info(f"Velocidade média: {stats.get('rows_per_second', 0):.2f} registros/seg")
        self.logger.info(f"Progresso final: {self.get_overall_progress()*100:.2f}%")
        self.logger.info("="*60)
