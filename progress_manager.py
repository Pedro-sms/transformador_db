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
        self.total_tables = total_tables
        self.total_rows = total_rows
        self.processed_tables = 0
        self.processed_rows = 0
        self.start_time = time.time()
        self.logger.info(f"Iniciando conversão de {total_tables} tabelas com {total_rows} linhas totais")
    
    def start_table(self, table_name: str, row_count: int):
        self.current_table = table_name
        self.table_rows = row_count
        self.processed_table_rows = 0
        self.logger.info(f"Iniciando tabela {table_name} com {row_count} linhas")
        self._trigger_callback()
    
    def update_table_progress(self, processed_rows: int):
        if processed_rows > self.table_rows:
            self.logger.warning(f"Linhas processadas ({processed_rows}) excedem o total da tabela ({self.table_rows})")
            processed_rows = self.table_rows
        
        self.processed_table_rows = processed_rows
        self.processed_rows += processed_rows
        self._trigger_callback()
    
    def finish_table(self):
        self.processed_tables += 1
        self.logger.info(f"Tabela {self.current_table} concluída. {self.processed_tables}/{self.total_tables} tabelas processadas")
        self.current_table = None
        self._trigger_callback()
    
    def get_overall_progress(self) -> float:
        if self.total_tables == 0 or self.total_rows == 0:
            return 0
        
        table_progress = self.processed_tables / self.total_tables * 0.3
        data_progress = self.processed_rows / self.total_rows * 0.7
        
        return min(table_progress + data_progress, 1.0)  # Garante que não ultrapasse 100%
    
    def get_detailed_progress(self) -> Dict[str, Any]:
        return {
            'current_table': self.current_table,
            'processed_tables': self.processed_tables,
            'total_tables': self.total_tables,
            'processed_rows': self.processed_rows,
            'total_rows': self.total_rows,
            'table_progress': f"{self.processed_table_rows}/{self.table_rows}",
            'overall_percentage': f"{self.get_overall_progress()*100:.2f}%",
            'elapsed_time': time.time() - self.start_time if self.start_time else 0,
            'estimated_remaining': self.get_estimated_time_remaining()
        }
    
    def get_estimated_time_remaining(self) -> str:
        if not self.start_time or self.processed_rows == 0:
            return "Estimativa indisponível"
        
        elapsed = time.time() - self.start_time
        rows_per_second = self.processed_rows / elapsed
        remaining_rows = self.total_rows - self.processed_rows
        
        if rows_per_second <= 0:
            return "Estimativa indisponível"
        
        seconds_remaining = remaining_rows / rows_per_second
        
        # Formata para legibilidade
        hours, remainder = divmod(seconds_remaining, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        if hours > 0:
            return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
        elif minutes > 0:
            return f"{int(minutes)}m {int(seconds)}s"
        else:
            return f"{int(seconds)}s"
    
    def _trigger_callback(self):
        if self.update_callback:
            try:
                self.update_callback(self.get_detailed_progress())
            except Exception as e:
                self.logger.error(f"Erro no callback de progresso: {e}")