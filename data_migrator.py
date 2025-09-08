from typing import List, Dict, Any, Optional, Set
from firebird_client import FirebirdClient
from schema_converter import SchemaConverter
import re
import datetime
import decimal
import logging

logger = logging.getLogger(__name__)

class DataMigrator:
    def __init__(self, firebird_client: FirebirdClient, batch_size: int = 10000):
        self.firebird_client = firebird_client
        self.batch_size = batch_size
        self.schema_converter = SchemaConverter()
        self.postgres_reserved_words = self.get_postgres_reserved_words()

    @staticmethod
    def get_postgres_reserved_words() -> Set[str]:
        """Retorna conjunto completo de palavras reservadas do PostgreSQL"""
        return {
            'all', 'analyse', 'analyze', 'and', 'any', 'array', 'as', 'asc',
            'asymmetric', 'authorization', 'binary', 'both', 'case', 'cast',
            'check', 'collate', 'column', 'constraint', 'create', 'cross',
            'current_catalog', 'current_date', 'current_role', 'current_schema',
            'current_time', 'current_timestamp', 'current_user', 'default',
            'deferrable', 'desc', 'distinct', 'do', 'else', 'end', 'except',
            'false', 'fetch', 'for', 'foreign', 'from', 'grant', 'group', 'having',
            'in', 'initially', 'intersect', 'into', 'join', 'leading', 'left',
            'like', 'limit', 'localtime', 'localtimestamp', 'not', 'null', 'offset',
            'on', 'only', 'or', 'order', 'outer', 'over', 'overlaps', 'placing',
            'primary', 'references', 'returning', 'right', 'select', 'session_user',
            'similar', 'some', 'symmetric', 'table', 'then', 'to', 'trailing',
            'true', 'union', 'unique', 'user', 'using', 'variadic', 'verbose',
            'when', 'where', 'window', 'with', 'ilike', 'is', 'isnull', 'notnull'
        }

    def format_column_name(self, column_name: str) -> str:
        """Coloca aspas em nomes de colunas que são palavras reservadas do PostgreSQL"""
        if column_name.lower() in self.postgres_reserved_words:
            return f'"{column_name.lower()}"'
        return f'"{column_name.lower()}"'  # Sempre usar aspas para consistência

    def format_value_for_postgres(self, value: Any, column_type: int = None) -> str:
        """
        Formatação robusta de valores para PostgreSQL com tratamento específico por tipo
        """
        if value is None:
            return "NULL"
        
        elif isinstance(value, str):
            # Verificar se é uma string vazia
            if value == '':
                return "''"
            
            # Escape completo para strings
            escaped = value.replace('\\', '\\\\')  # Escape backslashes primeiro
            escaped = escaped.replace("'", "''")   # Escape aspas simples
            escaped = escaped.replace('\0', '')    # Remove null bytes
            escaped = escaped.replace('\r\n', '\\n')  # Windows line endings
            escaped = escaped.replace('\r', '\\n')    # Mac line endings
            escaped = escaped.replace('\n', '\\n')    # Unix line endings
            escaped = escaped.replace('\t', '\\t')    # Escape tab
            
            # Para strings com caracteres especiais, use E-string notation
            if any(char in escaped for char in ['\\n', '\\t', '\\\\']) or any(ord(c) < 32 for c in escaped if c):
                return f"E'{escaped}'"
            return f"'{escaped}'"
        
        elif isinstance(value, (int, float)):
            # Verificar valores especiais
            if isinstance(value, float):
                if value != value:  # NaN check
                    return "NULL"
                if value == float('inf'):
                    return "'infinity'"
                if value == float('-inf'):
                    return "'-infinity'"
            
            # Para campos NUMERIC/DECIMAL, manter precisão
            if column_type in [64]:  # NUMERIC/DECIMAL
                return str(decimal.Decimal(str(value)))
            
            return str(value)
        
        elif isinstance(value, datetime.datetime):
            # Formatação ISO completa com microsegundos se disponível
            if value.microsecond:
                formatted = value.strftime('%Y-%m-%d %H:%M:%S.%f')
            else:
                formatted = value.strftime('%Y-%m-%d %H:%M:%S')
            
            if value.tzinfo:
                return f"'{formatted}{value.strftime('%z')}'"
            return f"'{formatted}'"
        
        elif isinstance(value, datetime.date):
            return f"'{value.isoformat()}'"
        
        elif isinstance(value, datetime.time):
            if value.microsecond:
                return f"'{value.strftime('%H:%M:%S.%f')}'"
            return f"'{value.strftime('%H:%M:%S')}'"
        
        elif isinstance(value, (bytes, bytearray)):
            # Formato bytea hex para PostgreSQL (mais eficiente que escape)
            hex_value = value.hex()
            return f"'\\x{hex_value}'"
        
        elif isinstance(value, bool):
            return 'TRUE' if value else 'FALSE'
        
        elif isinstance(value, decimal.Decimal):
            return str(value)
        
        elif hasattr(value, '__iter__') and not isinstance(value, (str, bytes)):
            # Arrays/listas - PostgreSQL suporta arrays nativamente
            try:
                formatted_items = [self.format_value_for_postgres(item) for item in value]
                return f"ARRAY[{', '.join(formatted_items)}]"
            except Exception:
                logger.warning(f"Erro ao formatar array: {value}")
                return "NULL"
        
        else:
            # Fallback para outros tipos
            try:
                str_value = str(value)
                if str_value.lower() in ('none', 'null', ''):
                    return "NULL"
                
                # Verificar se é numérico
                try:
                    float(str_value)
                    return str_value
                except ValueError:
                    # Tratar como string
                    escaped = str_value.replace("'", "''").replace('\\', '\\\\')
                    return f"'{escaped}'"
            except Exception:
                logger.warning(f"Erro ao formatar valor: {value} (tipo: {type(value)})")
                return "NULL"

    def generate_insert_statements(self, table_name: str, data_batch: List[Dict], 
                                 table_schema: Any = None) -> str:
        """
        Gera INSERT statements otimizados com múltiplos valores
        """
        if not data_batch:
            return ""
        
        # Obter informações das colunas se schema disponível
        column_types = {}
        if table_schema:
            for col in table_schema.columns:
                column_types[col['name']] = col['type']
        
        # Preparar nomes das colunas
        columns = list(data_batch[0].keys())
        formatted_columns = [self.format_column_name(col) for col in columns]
        table_name_formatted = f'"{table_name.lower()}"'
        
        # Dividir em chunks para evitar queries muito grandes
        chunk_size = min(500, self.batch_size // 4)  # Ajustar baseado no batch_size
        insert_statements = []
        
        for i in range(0, len(data_batch), chunk_size):
            chunk = data_batch[i:i + chunk_size]
            values_lines = []
            
            for row in chunk:
                values = []
                for col in columns:
                    col_type = column_types.get(col)
                    formatted_value = self.format_value_for_postgres(
                        row.get(col), col_type
                    )
                    values.append(formatted_value)
                
                values_lines.append(f"({', '.join(values)})")
            
            # Construir INSERT simples sem ON CONFLICT para debug
            insert_sql = f"""INSERT INTO {table_name_formatted} ({', '.join(formatted_columns)}) 
VALUES 
  {',\n  '.join(values_lines)};"""
            
            insert_statements.append(insert_sql)
        
        return "\n\n".join(insert_statements)

    def generate_copy_statements(self, table_name: str, data_batch: List[Dict], 
                               table_schema: Any = None) -> str:
        """
        Gera statements COPY FROM para volumes grandes de dados (mais eficiente)
        """
        if not data_batch:
            return ""
        
        columns = list(data_batch[0].keys())
        formatted_columns = [self.format_column_name(col) for col in columns]
        table_name_formatted = f'"{table_name.lower()}"'
        
        # Header do COPY com configurações robustas
        copy_lines = [
            f"COPY {table_name_formatted} ({', '.join(formatted_columns)}) FROM STDIN WITH (",
            "  FORMAT csv,",
            "  DELIMITER ',',",
            "  QUOTE '\"',",
            "  ESCAPE '\"',",
            "  NULL '\\N',",
            "  ENCODING 'UTF8'",
            ");"
        ]
        
        # Obter informações das colunas se schema disponível
        column_types = {}
        if table_schema:
            for col in table_schema.columns:
                column_types[col['name']] = col['type']
        
        # Dados em formato CSV
        for row in data_batch:
            csv_values = []
            for col in columns:
                value = row.get(col)
                col_type = column_types.get(col)
                
                if value is None:
                    csv_values.append('\\N')  # NULL em COPY
                elif isinstance(value, str):
                    # Escape específico para CSV
                    if value == '':
                        csv_values.append('""')  # String vazia
                    else:
                        escaped = value.replace('"', '""')  # Escape aspas duplas
                        escaped = escaped.replace('\n', '\\n')  # Escape newlines
                        escaped = escaped.replace('\r', '\\r')  # Escape carriage returns
                        csv_values.append(f'"{escaped}"')
                elif isinstance(value, (bytes, bytearray)):
                    # Bytea em formato hex
                    hex_value = value.hex()
                    csv_values.append(f'"\\x{hex_value}"')
                elif isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
                    formatted = self.format_value_for_postgres(value, col_type)
                    csv_values.append(formatted.strip("'"))  # Remove aspas externas
                elif isinstance(value, bool):
                    csv_values.append('t' if value else 'f')  # Formato boolean do PostgreSQL
                else:
                    formatted = self.format_value_for_postgres(value, col_type)
                    if formatted == 'NULL':
                        csv_values.append('\\N')
                    else:
                        csv_values.append(formatted.strip("'"))  # Remove aspas externas
            
            copy_lines.append(','.join(csv_values))
        
        # Finalizar COPY
        copy_lines.append('\\.')
        copy_lines.append('')  # Linha vazia para separação
        
        return '\n'.join(copy_lines)

    def validate_data_integrity(self, table_name: str, data_batch: List[Dict], 
                              table_schema: Any) -> List[str]:
        """
        Valida integridade dos dados antes da migração
        """
        warnings = []
        
        if not table_schema or not data_batch:
            return warnings
        
        # Criar mapeamento de colunas por nome
        column_map = {col['name']: col for col in table_schema.columns}
        
        for i, row in enumerate(data_batch):
            for col_name, value in row.items():
                column = column_map.get(col_name)
                if not column:
                    warnings.append(f"Linha {i+1}: coluna '{col_name}' não encontrada no schema")
                    continue
                
                # Verificar NOT NULL
                if not column['nullable'] and value is None:
                    warnings.append(f"Linha {i+1}, coluna {col_name}: valor NULL em coluna NOT NULL")
                
                # Verificar tipos específicos
                if value is not None:
                    if column['type'] == 7:  # SMALLINT
                        try:
                            val = int(value)
                            if val < -32768 or val > 32767:
                                warnings.append(f"Linha {i+1}, coluna {col_name}: valor {val} fora do range SMALLINT")
                        except (ValueError, TypeError):
                            warnings.append(f"Linha {i+1}, coluna {col_name}: valor inválido para SMALLINT: {value}")
                    
                    elif column['type'] == 8:  # INTEGER
                        try:
                            val = int(value)
                            if val < -2147483648 or val > 2147483647:
                                warnings.append(f"Linha {i+1}, coluna {col_name}: valor {val} fora do range INTEGER")
                        except (ValueError, TypeError):
                            warnings.append(f"Linha {i+1}, coluna {col_name}: valor inválido para INTEGER: {value}")
                    
                    elif column['type'] in [14, 37] and isinstance(value, str):  # CHAR/VARCHAR
                        max_length = column.get('length', 0)
                        if max_length > 0 and len(value) > max_length:
                            warnings.append(f"Linha {i+1}, coluna {col_name}: string muito longa ({len(value)} > {max_length})")
                    
                    elif column['type'] == 64:  # NUMERIC/DECIMAL
                        try:
                            decimal.Decimal(str(value))
                        except (ValueError, decimal.InvalidOperation):
                            warnings.append(f"Linha {i+1}, coluna {col_name}: valor inválido para NUMERIC: {value}")
        
        return warnings

    def migrate_table_data(self, table_name: str, total_rows: int, 
                          table_schema: Any = None, 
                          use_copy: bool = False,
                          use_transactions: bool = True) -> str:
        """
        Migra dados de tabela com opções flexíveis
        CORRIGIDO: Forçar uso de INSERT ao invés de COPY para debug
        """
        if total_rows == 0:
            return f"-- Tabela {table_name} está vazia\n"
        
        sql_output = []
        table_name_formatted = f'"{table_name.lower()}"'
        
        # Comentário inicial
        sql_output.append(f"-- Migração de dados da tabela {table_name} ({total_rows:,} registros)")
        
        if use_transactions:
            sql_output.append("BEGIN;")
        
        # Não desabilitar triggers para debug - pode estar causando problemas
        # sql_output.append(f"ALTER TABLE {table_name_formatted} DISABLE TRIGGER ALL;")
        
        try:
            processed_rows = 0
            batch_count = 0
            
            for offset in range(0, total_rows, self.batch_size):
                batch = self.firebird_client.get_table_data_batch(table_name, offset, self.batch_size)
                if not batch:
                    break
                
                batch_count += 1
                current_batch_size = len(batch)
                processed_rows += current_batch_size
                
                # Validar dados se schema disponível
                if table_schema:
                    warnings = self.validate_data_integrity(table_name, batch, table_schema)
                    if warnings:
                        sql_output.append(f"-- Avisos do lote {batch_count}:")
                        for warning in warnings[:5]:  # Limitar avisos
                            sql_output.append(f"-- {warning}")
                        if len(warnings) > 5:
                            sql_output.append(f"-- ... e mais {len(warnings) - 5} avisos")
                
                # SEMPRE usar INSERT para garantir compatibilidade
                insert_sql = self.generate_insert_statements(table_name, batch, table_schema)
                
                sql_output.append(f"-- Lote {batch_count}: registros {offset + 1} a {offset + current_batch_size}")
                sql_output.append(insert_sql)
                
                # Logging para acompanhamento
                logger.info(f"Processado lote {batch_count} da tabela {table_name}: "
                          f"{current_batch_size} registros ({processed_rows}/{total_rows})")
        
        except Exception as e:
            logger.error(f"Erro durante migração da tabela {table_name}: {str(e)}")
            if use_transactions:
                sql_output.append("ROLLBACK;")
                sql_output.append(f"-- ERRO: {str(e)}")
            raise
        
        finally:
            # Não reabilitar triggers se não foram desabilitados
            pass
        
        if use_transactions:
            sql_output.append("COMMIT;")
        
        sql_output.append(f"-- Migração da tabela {table_name} concluída: {processed_rows:,} registros processados")
        sql_output.append("")  # Linha vazia para separação
        
        return "\n".join(sql_output)
