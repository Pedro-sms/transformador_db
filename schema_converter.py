from typing import Dict, Any, List, Optional
import re
from firebird_client import TableSchema

class SchemaConverter:
    @staticmethod
    def map_data_type(firebird_type: int, length: int, precision: int, scale: int, 
                     subtype: Optional[int] = None) -> str:
        """
        Mapeamento completo e corrigido de tipos de dados do Firebird para PostgreSQL
        """
        # Mapeamento baseado nos códigos internos do Firebird
        type_mapping = {
            # Tipos numéricos
            7: lambda: 'smallint' if scale == 0 else f'numeric({precision}, {abs(scale)})',  # SMALLINT
            8: lambda: 'integer' if scale == 0 else f'numeric({precision}, {abs(scale)})',   # INTEGER  
            9: 'bigint',     # QUAD - mapeado para BIGINT
            10: 'real',      # FLOAT
            11: 'double precision',  # D_FLOAT
            16: lambda: 'bigint' if scale == 0 else f'numeric({precision}, {abs(scale)})',  # INT64
            27: 'double precision',  # DOUBLE PRECISION
            
            # Tipos numéricos com precisão
            64: lambda: f'numeric({precision}, {abs(scale)})' if precision > 0 else 'numeric',  # NUMERIC/DECIMAL
            
            # Tipos de texto
            14: lambda: f'char({length})' if length and length > 0 else 'char(1)',    # CHAR
            37: lambda: f'varchar({length})' if length and length > 0 else 'text',    # VARCHAR
            40: lambda: f'varchar({length})' if length and length > 0 else 'text',    # CSTRING
            
            # Tipos de data/hora
            12: 'date',      # DATE
            13: 'time',      # TIME  
            35: 'timestamp', # TIMESTAMP
            
            # Tipos booleanos (Firebird 3.0+)
            23: 'boolean',   # BOOLEAN
            
            # BLOBs
            261: lambda: SchemaConverter._handle_blob_type(subtype),  # BLOB
            
            # Arrays - PostgreSQL suporta arrays nativamente
            80: lambda: 'text[]',  # ARRAY (estimativa)
            
            # Outros tipos específicos
            17: 'numeric',   # DECIMAL_FIXED
            18: 'numeric',   # DECIMAL_TEXT
            45: 'bytea',     # BLOB alternativo
        }
        
        # Processar o tipo
        type_handler = type_mapping.get(firebird_type, lambda: 'text')  # Default: text
        
        # Se é uma função lambda, executá-la
        if callable(type_handler):
            try:
                return type_handler()
            except Exception:
                return 'text'  # Fallback em caso de erro
        
        return type_handler

    @staticmethod
    def _handle_blob_type(subtype: Optional[int]) -> str:
        """Manuseia diferentes subtipos de BLOB"""
        if subtype is None:
            return 'bytea'  # Default para binário
        elif subtype == 0:    # BLOB SUB_TYPE BINARY
            return 'bytea'
        elif subtype == 1:    # BLOB SUB_TYPE TEXT  
            return 'text'
        else:
            return 'bytea'  # Default para outros subtipos

    @staticmethod  
    def convert_default_value(default_value: str) -> str:
        """
        Conversão robusta de valores padrão do Firebird para PostgreSQL
        """
        if not default_value:
            return ""
            
        default_value = default_value.strip()
        
        # Remove DEFAULT se presente no início
        if default_value.upper().startswith('DEFAULT'):
            default_value = default_value[7:].strip()
        
        # Mapeamento de funções e valores especiais
        conversions = {
            'CURRENT_TIMESTAMP': 'CURRENT_TIMESTAMP',
            'CURRENT_DATE': 'CURRENT_DATE', 
            'CURRENT_TIME': 'CURRENT_TIME',
            'NOW': 'NOW()',
            'TODAY': 'CURRENT_DATE',
            'YESTERDAY': 'CURRENT_DATE - INTERVAL \'1 day\'',
            'TOMORROW': 'CURRENT_DATE + INTERVAL \'1 day\'',
            'NULL': 'NULL',
            'USER': 'CURRENT_USER',
            'CURRENT_USER': 'CURRENT_USER',
            'CURRENT_ROLE': 'CURRENT_ROLE',
            'CURRENT_CONNECTION': 'inet_client_addr()',  # Aproximação
            'CURRENT_TRANSACTION': 'txid_current()',     # Aproximação
        }
        
        upper_value = default_value.upper()
        
        # Verificar conversões diretas
        if upper_value in conversions:
            return conversions[upper_value]
        
        # Generators/Sequences - conversão corrigida
        gen_pattern = re.match(r'GEN_ID\s*\(\s*(\w+)\s*,\s*(\d+)\s*\)', default_value, re.IGNORECASE)
        if gen_pattern:
            gen_name, increment = gen_pattern.groups()
            return f"nextval('{gen_name.lower()}')"
        
        # Funções de data com parâmetros
        if 'DATEADD(' in upper_value:
            # DATEADD precisa ser convertido para sintaxe PostgreSQL
            dateadd_match = re.match(r'DATEADD\s*\(\s*(\w+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)', default_value, re.IGNORECASE)
            if dateadd_match:
                unit, amount, date_expr = dateadd_match.groups()
                # Mapear unidades do Firebird para PostgreSQL
                unit_map = {
                    'DAY': 'day', 'MONTH': 'month', 'YEAR': 'year',
                    'HOUR': 'hour', 'MINUTE': 'minute', 'SECOND': 'second'
                }
                pg_unit = unit_map.get(unit.upper(), unit.lower())
                return f"({date_expr} + INTERVAL '{amount} {pg_unit}')"
            return f"-- MANUAL CONVERSION NEEDED: {default_value}"
        
        # Strings literais
        if default_value.startswith("'") and default_value.endswith("'"):
            # Verificar se há escape necessário
            content = default_value[1:-1]
            if '\\' in content or '\n' in content or '\r' in content or '\t' in content:
                escaped = content.replace('\\', '\\\\').replace("'", "''")
                return f"E'{escaped}'"
            return default_value
            
        # Números
        try:
            float(default_value)
            return default_value
        except ValueError:
            pass
        
        # Expressões matemáticas simples
        if re.match(r'^[\d\s+\-*/().]+$', default_value):
            return default_value
        
        # Funções ou expressões que não precisam de aspas
        if any(char in default_value for char in ['(', ')', '+', '-', '*', '/']):
            return default_value
        
        # Default: tratar como string literal
        escaped = default_value.replace("'", "''")
        return f"'{escaped}'"

    def convert_table_schema(self, table_schema: Any) -> str:
        """
        Converte o esquema completo de uma tabela do Firebird para PostgreSQL
        """
        sql_lines = []
        
        # Nome da tabela em minúsculo
        table_name = table_schema.name.lower()
        sql_lines.append(f'CREATE TABLE "{table_name}" (')
        
        # Definições de colunas
        column_defs = []
        for column in table_schema.columns:
            column_name = f'"{column["name"].lower()}"'
            
            # Obtém o subtipo para BLOBs (se disponível)
            subtype = column.get('subtype', 0) if column['type'] == 261 else None
            
            pg_type = self.map_data_type(
                column['type'],
                column['length'],
                column['precision'],
                column['scale'],
                subtype
            )
            
            col_def = f"    {column_name} {pg_type}"
            
            # NULL/NOT NULL
            if not column['nullable']:
                col_def += " NOT NULL"
                
            # Valor padrão
            if column.get('default'):
                default_value = self.convert_default_value(column['default'])
                if default_value and default_value != "NULL":
                    col_def += f" DEFAULT {default_value}"
                    
            column_defs.append(col_def)
        
        # Chave primária
        if table_schema.primary_key:
            pk_columns = ', '.join([f'"{col.lower()}"' for col in table_schema.primary_key])
            column_defs.append(f"    PRIMARY KEY ({pk_columns})")
        
        sql_lines.append(',\n'.join(column_defs))
        sql_lines.append(');')
        
        return '\n'.join(sql_lines)
    
    def generate_constraints(self, table_schema: Any) -> List[str]:
        """
        Gera constraints separadamente para melhor organização
        """
        constraints = []
        table_name = f'"{table_schema.name.lower()}"'
        
        # Índices únicos (excluindo chave primária)
        unique_indexes = [idx for idx in table_schema.indexes 
                         if idx.get('unique') and idx.get('name') and
                         not (table_schema.primary_key and 
                              set([idx.get('field', '')]) == set(table_schema.primary_key))]
        
        for index in unique_indexes:
            index_name = f'"{index["name"].lower()}"'
            if isinstance(index.get('fields'), list):
                fields = ', '.join([f'"{field.lower()}"' for field in index['fields']])
            else:
                field_name = index.get('field', index.get('fields', ''))
                fields = f'"{field_name.lower()}"'
            
            constraints.append(f"CREATE UNIQUE INDEX {index_name} ON {table_name} ({fields});")
        
        # Índices não-únicos
        regular_indexes = [idx for idx in table_schema.indexes 
                          if not idx.get('unique') and idx.get('name')]
        
        for index in regular_indexes:
            index_name = f'"{index["name"].lower()}"'
            if isinstance(index.get('fields'), list):
                fields = ', '.join([f'"{field.lower()}"' for field in index['fields']])
            else:
                field_name = index.get('field', index.get('fields', ''))
                fields = f'"{field_name.lower()}"'
            
            constraints.append(f"CREATE INDEX {index_name} ON {table_name} ({fields});")
        
        # Chaves estrangeiras
        for fk in table_schema.foreign_keys:
            fk_name = f'"{fk["name"].lower()}"'
            source_field = f'"{fk["source_field"].lower()}"'
            target_table = f'"{fk["target_table"].lower()}"'
            target_field = f'"{fk["target_field"].lower()}"'
            
            delete_rule = fk.get('delete_rule', 'NO ACTION').upper()
            update_rule = fk.get('update_rule', 'NO ACTION').upper()
            
            # Mapear regras do Firebird para PostgreSQL
            rule_mapping = {
                'CASCADE': 'CASCADE',
                'SET NULL': 'SET NULL',
                'SET DEFAULT': 'SET DEFAULT',
                'NO ACTION': 'NO ACTION',
                'RESTRICT': 'RESTRICT'
            }
            
            pg_delete_rule = rule_mapping.get(delete_rule, 'NO ACTION')
            pg_update_rule = rule_mapping.get(update_rule, 'NO ACTION')
            
            fk_sql = f"ALTER TABLE {table_name} ADD CONSTRAINT {fk_name} "
            fk_sql += f"FOREIGN KEY ({source_field}) REFERENCES {target_table} ({target_field})"
            
            if pg_delete_rule != 'NO ACTION':
                fk_sql += f" ON DELETE {pg_delete_rule}"
            if pg_update_rule != 'NO ACTION':
                fk_sql += f" ON UPDATE {pg_update_rule}"
            
            fk_sql += ";"
            constraints.append(fk_sql)
        
        # Check constraints
        for check in table_schema.check_constraints:
            if check.get('condition'):
                check_name = f'"{check["name"].lower()}"'
                # Converter condição básica (pode precisar de ajustes manuais)
                condition = check['condition']
                condition = re.sub(r'\bNEW\.(\w+)', r'"\1"', condition, flags=re.IGNORECASE)
                
                constraints.append(
                    f"ALTER TABLE {table_name} ADD CONSTRAINT {check_name} CHECK ({condition});"
                )
        
        return constraints
    
    def generate_sequences(self, generators: List[Dict[str, Any]]) -> List[str]:
        """
        Converte generators do Firebird para sequences do PostgreSQL
        """
        sequences = []
        
        for gen in generators:
            gen_name = gen['name'].lower()
            current_value = gen.get('current_value', 0)
            
            sequences.append(f'CREATE SEQUENCE "{gen_name}";')
            
            if current_value > 0:
                sequences.append(f"SELECT setval('\"{gen_name}\"', {current_value});")
        
        return sequences
    
    def convert_view_schema(self, view_name: str, view_definition: str) -> str:
        """
        Converte definição de view do Firebird para PostgreSQL
        """
        view_name_lower = view_name.lower()
        
        # Limpeza básica da definição
        definition = view_definition.strip()
        if definition.upper().startswith('SELECT'):
            definition = definition
        else:
            # Pode ter outras palavras antes do SELECT
            select_match = re.search(r'\bSELECT\b', definition, re.IGNORECASE)
            if select_match:
                definition = definition[select_match.start():]
        
        # Conversões básicas
        definition = re.sub(r'\bGEN_ID\s*\(\s*(\w+)\s*,\s*\d+\s*\)', 
                           r"nextval('\1')", definition, flags=re.IGNORECASE)
        
        return f'CREATE OR REPLACE VIEW "{view_name_lower}" AS\n{definition};'