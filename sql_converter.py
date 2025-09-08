import re
import logging
from typing import List, Tuple, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

# Função para converter nomes para snake_case
def to_snake_case(name: str) -> str:
    """Converte nomes para snake_case preservando acrônimos curtos"""
    if name.isupper() and len(name) <= 5:
        return name.lower()
    
    # Inserir underscores antes de letras maiúsculas
    name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
    name = re.sub(r'([A-Z])([A-Z][a-z])', r'\1_\2', name)
    
    # Converter para minúsculo
    name = name.lower()
    
    # Substituir caracteres especiais por underscore
    name = re.sub(r'[^a-z0-9_]', '_', name)
    
    # Remover underscores múltiplos
    name = re.sub(r'_+', '_', name)
    
    # Remover underscores no início e fim
    return name.strip('_')

class SQLConverter:
    def __init__(self):
        self.rules = self._init_rules()
        self.postgres_keywords = self._get_postgres_keywords()

    def _get_postgres_keywords(self) -> set:
        """Retorna conjunto de palavras-chave do PostgreSQL que precisam ser quotadas"""
        return {
            'user', 'order', 'group', 'limit', 'offset', 'select', 'from', 'where',
            'join', 'inner', 'outer', 'left', 'right', 'full', 'cross', 'union',
            'intersect', 'except', 'all', 'distinct', 'case', 'when', 'then',
            'else', 'end', 'if', 'exists', 'in', 'like', 'between', 'null',
            'true', 'false', 'and', 'or', 'not', 'is', 'as', 'table', 'column',
            'constraint', 'primary', 'foreign', 'key', 'references', 'check',
            'unique', 'index', 'create', 'drop', 'alter', 'insert', 'update',
            'delete', 'grant', 'revoke', 'commit', 'rollback', 'transaction',
            'schema', 'view', 'function', 'procedure', 'trigger', 'sequence'
        }

    def _init_rules(self) -> List[Tuple[str, str, str]]:
        """Define regras de conversão do Firebird para PostgreSQL"""
        return [
            # Tipos de dados básicos
            (r'\bBLOB\s+SUB_TYPE\s+TEXT\b', 'TEXT', 'BLOB TEXT → TEXT'),
            (r'\bBLOB\s+SUB_TYPE\s+BINARY\b', 'BYTEA', 'BLOB BINARY → BYTEA'),
            (r'\bBLOB\s+SUB_TYPE\s+0\b', 'BYTEA', 'BLOB BINARY → BYTEA'),
            (r'\bBLOB\s+SUB_TYPE\s+1\b', 'TEXT', 'BLOB TEXT → TEXT'),
            (r'\bBLOB\b(?!\s+SUB_TYPE)', 'BYTEA', 'BLOB → BYTEA'),
            (r'\bFLOAT\b', 'DOUBLE PRECISION', 'FLOAT → DOUBLE PRECISION'),
            (r'\bDOUBLE\s+PRECISION\b', 'DOUBLE PRECISION', 'DOUBLE PRECISION mantido'),
            (r'\bINTEGER\b', 'INTEGER', 'INTEGER → INTEGER'),
            (r'\bSMALLINT\b', 'SMALLINT', 'SMALLINT → SMALLINT'),
            (r'\bBIGINT\b', 'BIGINT', 'BIGINT → BIGINT'),
            
            # Tipos numéricos com precisão
            (r'\bNUMERIC\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)', r'NUMERIC(\1,\2)', 'NUMERIC → NUMERIC'),
            (r'\bDECIMAL\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)', r'DECIMAL(\1,\2)', 'DECIMAL → DECIMAL'),
            (r'\bNUMERIC\s*\(\s*(\d+)\s*\)', r'NUMERIC(\1,0)', 'NUMERIC sem escala'),
            (r'\bDECIMAL\s*\(\s*(\d+)\s*\)', r'DECIMAL(\1,0)', 'DECIMAL sem escala'),
            
            # Tipos de texto
            (r'\bCHAR\s*\(\s*(\d+)\s*\)', r'CHAR(\1)', 'CHAR → CHAR'),
            (r'\bVARCHAR\s*\(\s*(\d+)\s*\)', r'VARCHAR(\1)', 'VARCHAR → VARCHAR'),
            (r'\bCSTRING\s*\(\s*(\d+)\s*\)', r'VARCHAR(\1)', 'CSTRING → VARCHAR'),
            
            # Tipos de data/hora
            (r'\bDATE\b', 'DATE', 'DATE → DATE'),
            (r'\bTIME\b', 'TIME', 'TIME → TIME'),
            (r'\bTIMESTAMP\b', 'TIMESTAMP', 'TIMESTAMP → TIMESTAMP'),
            
            # Funções de data/hora
            (r'\bCURRENT_TIMESTAMP\b', 'CURRENT_TIMESTAMP', 'CURRENT_TIMESTAMP mantido'),
            (r'\bCURRENT_DATE\b', 'CURRENT_DATE', 'CURRENT_DATE mantido'),
            (r'\bCURRENT_TIME\b', 'CURRENT_TIME', 'CURRENT_TIME mantido'),
            (r'\b\'NOW\'\b', 'CURRENT_TIMESTAMP', 'NOW → CURRENT_TIMESTAMP'),
            (r'\b\'TODAY\'\b', 'CURRENT_DATE', 'TODAY → CURRENT_DATE'),
            
            # Generators/Sequences
            (r'\bCREATE\s+GENERATOR\s+(\w+)\s*;', r'CREATE SEQUENCE "\1";', 'GENERATOR → SEQUENCE'),
            (r'\bSET\s+GENERATOR\s+(\w+)\s+TO\s+(\d+)\s*;', r"SELECT setval('\"\1\"', \2);", 'SET GENERATOR → setval'),
            (r'\bGEN_ID\s*\(\s*(\w+)\s*,\s*(\d+)\s*\)', r"nextval('\"\1\"')", 'GEN_ID → nextval'),
            
            # Funções específicas do Firebird
            (r'\bCAST\s*\(\s*(.+?)\s+AS\s+(\w+(?:\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\))?)\s*\)', 
             r'CAST(\1 AS \2)', 'CAST corrigido'),
            (r'\bEXTRACT\s*\(\s*(\w+)\s+FROM\s+(.+?)\s*\)', r'EXTRACT(\1 FROM \2)', 'EXTRACT mantido'),
            
            # Conversão de SUBSTRING
            (r'\bSUBSTRING\s*\(\s*(.+?)\s+FROM\s+(\d+)\s+FOR\s+(\d+)\s*\)', 
             r'SUBSTRING(\1 FROM \2 FOR \3)', 'SUBSTRING FROM/FOR mantido'),
            (r'\bSUBSTRING\s*\(\s*(.+?)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)', 
             r'SUBSTRING(\1, \2, \3)', 'SUBSTRING com vírgulas'),
            
            # Concatenação de strings
            (r'\|\|', '||', 'Concatenação mantida'),
            
            # Remoção de elementos específicos do Firebird
            (r'\bCHARACTER\s+SET\s+\w+', '', 'Remove CHARACTER SET'),
            (r'\bCOLLATE\s+\w+', '', 'Remove COLLATE'),
            (r'\bCOMPUTED\s+BY\s*\([^)]+\)', '', 'Remove COMPUTED BY'),
            (r'\bSET\s+TERM\s+[^;]+;', '', 'Remove SET TERM'),
            (r'\bCOMMIT\s+WORK\s*;', '', 'Remove COMMIT WORK'),
            (r'\bSET\s+AUTODDL\s+(ON|OFF)\s*;', '', 'Remove SET AUTODDL'),
            (r'\bSET\s+NAMES\s+\w+\s*;', '', 'Remove SET NAMES'),
            
            # Triggers - marcar para revisão manual
            (r'\bFOR\s+EACH\s+ROW\b', 'FOR EACH ROW', 'FOR EACH ROW mantido'),
            (r'\bBEFORE\s+(INSERT|UPDATE|DELETE)\b', r'BEFORE \1', 'BEFORE trigger'),
            (r'\bAFTER\s+(INSERT|UPDATE|DELETE)\b', r'AFTER \1', 'AFTER trigger'),
            
            # Procedures e Functions
            (r'\bRETURNS\s*\(\s*([^)]+)\s*\)', r'RETURNS TABLE(\1)', 'RETURNS TABLE para procedures'),
            (r'\bSUSPEND\s*;', 'RETURN NEXT;', 'SUSPEND → RETURN NEXT'),
            (r'\bEXIT\s*;', 'RETURN;', 'EXIT → RETURN'),
            
            # Limpeza de comentários preservando estrutura SQL
            (r'--.*$', '', 'Remove comentários de linha'),
            
            # Limpeza
            (r'\bWITH\s+CHECK\s+OPTION\b', '', 'Remove WITH CHECK OPTION'),
        ]

    def convert_sql_script(self, sql_content: str) -> Tuple[str, Dict[str, Any]]:
        """Converte script SQL do Firebird para PostgreSQL"""
        stats = {
            'total_lines': sql_content.count('\n') + 1,
            'converted_lines': 0,
            'warnings': [],
            'errors': [],
            'converted_objects': {
                'tables': 0, 
                'sequences': 0, 
                'views': 0, 
                'triggers': 0, 
                'procedures': 0,
                'functions': 0,
                'indexes': 0
            }
        }

        try:
            logger.info("Iniciando conversão SQL")
            
            # Pré-processamento
            sql_content = self._pre_process(sql_content)
            
            # Aplicar regras de conversão
            sql_content = self._handle_domains(sql_content, stats)
            sql_content = self._apply_rules(sql_content, stats)
            sql_content = self._convert_identifiers(sql_content)
            sql_content = self._convert_triggers(sql_content, stats)
            sql_content = self._convert_procedures(sql_content, stats)
            sql_content = self._convert_functions(sql_content, stats)
            sql_content = self._convert_views(sql_content, stats)
            sql_content = self._convert_indexes(sql_content, stats)
            sql_content = self._fix_data_types(sql_content, stats)
            
            # Pós-processamento
            sql_content = self._post_process_sql(sql_content, stats)
            
            logger.info("Conversão SQL concluída com sucesso")
            return sql_content, stats
            
        except Exception as e:
            error_msg = f"Erro na conversão SQL: {str(e)}"
            stats['errors'].append(error_msg)
            logger.error(error_msg, exc_info=True)
            return sql_content, stats

    def _pre_process(self, sql: str) -> str:
        """Pré-processamento do SQL"""
        # Normalizar quebras de linha
        sql = re.sub(r'\r\n', '\n', sql)
        sql = re.sub(r'\r', '\n', sql)
        
        # Remover comentários preservando strings
        sql = self._remove_comments_preserve_strings(sql)
        
        # Normalizar espaços
        sql = re.sub(r'[ \t]+', ' ', sql)
        sql = re.sub(r'\n+', '\n', sql)
        
        return sql.strip()

    def _remove_comments_preserve_strings(self, sql: str) -> str:
        """Remove comentários preservando strings literais"""
        result = []
        in_string = False
        in_block_comment = False
        in_line_comment = False
        string_char = None
        i = 0
        
        while i < len(sql):
            char = sql[i]
            next_char = sql[i + 1] if i + 1 < len(sql) else ''
            
            if in_line_comment:
                if char == '\n':
                    in_line_comment = False
                    result.append(char)
                i += 1
                continue
            
            if in_block_comment:
                if char == '*' and next_char == '/':
                    in_block_comment = False
                    i += 2
                    continue
                i += 1
                continue
            
            if in_string:
                result.append(char)
                if char == string_char:
                    if next_char == string_char:  # Escaped quote
                        result.append(next_char)
                        i += 2
                        continue
                    else:
                        in_string = False
                        string_char = None
                i += 1
                continue
            
            if char in ["'", '"']:
                in_string = True
                string_char = char
                result.append(char)
            elif char == '-' and next_char == '-':
                in_line_comment = True
                i += 2
                continue
            elif char == '/' and next_char == '*':
                in_block_comment = True
                i += 2
                continue
            else:
                result.append(char)
            
            i += 1
        
        return ''.join(result)

    def _handle_domains(self, sql: str, stats: Dict[str, Any]) -> str:
        """Converte DOMAINs do Firebird para tipos básicos do PostgreSQL"""
        domain_pattern = re.compile(
            r'CREATE\s+DOMAIN\s+(\w+)\s+(.*?)(?=CREATE|ALTER|DROP|INSERT|UPDATE|DELETE|\Z)',
            flags=re.IGNORECASE | re.DOTALL
        )
        
        domains = {}
        
        def extract_domain(match):
            domain_name = match.group(1)
            domain_def = match.group(2).strip()
            
            # Extrair o tipo base
            type_match = re.search(
                r'(VARCHAR\s*\(\s*\d+\s*\)|CHAR\s*\(\s*\d+\s*\)|INTEGER|SMALLINT|BIGINT|NUMERIC\s*\(\s*\d+\s*,\s*\d+\s*\)|DATE|TIME|TIMESTAMP|BLOB)', 
                domain_def, re.IGNORECASE
            )
            
            if type_match:
                base_type = type_match.group(1)
                domains[domain_name.upper()] = base_type
                stats['warnings'].append(f"DOMAIN {domain_name} convertido para tipo base {base_type}")
            
            return ""  # Remove a definição de DOMAIN
        
        sql = domain_pattern.sub(extract_domain, sql)
        
        # Substituir usos de DOMAINs pelos tipos base
        for domain_name, base_type in domains.items():
            sql = re.sub(rf'\b{domain_name}\b', base_type, sql, flags=re.IGNORECASE)
        
        return sql

    def _apply_rules(self, sql: str, stats: Dict[str, Any]) -> str:
        """Aplica regras de conversão definidas"""
        original_line_count = sql.count('\n')
        
        for pattern, replacement, desc in self.rules:
            sql_new = re.sub(pattern, replacement, sql, flags=re.IGNORECASE | re.MULTILINE)
            if sql != sql_new:
                logger.debug(f"Aplicada regra: {desc}")
                sql = sql_new
        
        new_line_count = sql.count('\n')
        stats['converted_lines'] = abs(new_line_count - original_line_count)
        return sql

    def _convert_identifiers(self, sql: str) -> str:
        """Converte identificadores para padrão PostgreSQL"""
        # Divide para não alterar strings literais
        parts = re.split(r"('.*?'|\".*?\")", sql, flags=re.DOTALL)
        
        for i in range(len(parts)):
            if i % 2 == 0:  # Não é uma string literal
                # Converter nomes de tabelas e colunas para minúsculo e snake_case
                parts[i] = re.sub(
                    r'\b([A-Z][A-Z0-9_]*[a-z][A-Za-z0-9_]*)\b',
                    lambda m: to_snake_case(m.group(1)),
                    parts[i]
                )
                
                # Quotar palavras-chave do PostgreSQL
                words = re.findall(r'\b\w+\b', parts[i])
                for word in words:
                    if word.lower() in self.postgres_keywords:
                        parts[i] = re.sub(
                            rf'\b{re.escape(word)}\b', 
                            f'"{word.lower()}"', 
                            parts[i], 
                            flags=re.IGNORECASE
                        )
        
        return ''.join(parts)

    def _convert_triggers(self, sql: str, stats: Dict[str, Any]) -> str:
        """Converte triggers do Firebird para PostgreSQL"""
        pattern = re.compile(
            r'CREATE\s+(OR\s+ALTER\s+)?TRIGGER\s+(\w+)\s+(ACTIVE|INACTIVE)?\s*(BEFORE|AFTER)\s+(INSERT|UPDATE|DELETE|OR)+.*?AS\s*(DECLARE.*?)?(BEGIN.*?END)(?:\s*\^)?',
            flags=re.IGNORECASE | re.DOTALL
        )

        def replace_trigger(match):
            trigger_name = match.group(2)
            timing = match.group(4) or 'BEFORE'
            events = match.group(5)
            declarations = match.group(6) or ''
            body = match.group(7)
            
            stats['warnings'].append(f"Trigger {trigger_name} requer revisão manual - sintaxe pode diferir")
            stats['converted_objects']['triggers'] += 1
            
            # Conversões básicas no corpo do trigger
            if body:
                body = re.sub(r'\bNEW\.', 'NEW.', body)
                body = re.sub(r'\bOLD\.', 'OLD.', body)
                body = re.sub(r'\bEXCEPTION\s+(\w+)', r'RAISE EXCEPTION \1', body, flags=re.IGNORECASE)
                body = re.sub(r'\bEXIT\s*;', 'RETURN NULL;', body, flags=re.IGNORECASE)
            
            converted = f"""
-- TRIGGER {trigger_name} - CONVERTED (requires manual review)
-- Original Firebird trigger converted to PostgreSQL
CREATE OR REPLACE FUNCTION {trigger_name.lower()}_func() 
RETURNS TRIGGER AS $
{declarations}
{body}
$ LANGUAGE plpgsql;

-- Note: Verify table name and adjust timing/events as needed
-- CREATE TRIGGER {trigger_name.lower()} {timing} {events} ON your_table_name
-- FOR EACH ROW EXECUTE FUNCTION {trigger_name.lower()}_func();
"""
            return converted
        
        return pattern.sub(replace_trigger, sql)

    def _convert_procedures(self, sql: str, stats: Dict[str, Any]) -> str:
        """Converte stored procedures do Firebird para PostgreSQL"""
        pattern = re.compile(
            r'CREATE\s+(OR\s+ALTER\s+)?PROCEDURE\s+(\w+)\s*(\([^)]*\))?\s*(RETURNS\s*\([^)]*\))?\s*AS\s*(DECLARE.*?)?(BEGIN.*?END)(?:\s*\^)?',
            flags=re.IGNORECASE | re.DOTALL
        )

        def replace_procedure(match):
            proc_name = match.group(2)
            params = match.group(3) or '()'
            returns = match.group(4) or ''
            declarations = match.group(5) or ''
            body = match.group(6)
            
            stats['warnings'].append(f"Procedure {proc_name} requer revisão manual")
            stats['converted_objects']['procedures'] += 1
            
            # Ajustar RETURNS para PostgreSQL
            if returns:
                returns = re.sub(r'RETURNS\s*\(([^)]+)\)', r'RETURNS TABLE(\1)', returns, flags=re.IGNORECASE)
            else:
                returns = 'RETURNS VOID'
            
            # Conversões no corpo
            if body:
                body = re.sub(r'\bSUSPEND\s*;', 'RETURN NEXT;', body, flags=re.IGNORECASE)
                body = re.sub(r'\bEXIT\s*;', 'RETURN;', body, flags=re.IGNORECASE)
            
            converted = f"""
-- PROCEDURE {proc_name} - CONVERTED (requires manual review)
CREATE OR REPLACE FUNCTION {proc_name.lower()}{params} 
{returns} AS $
{declarations}
{body}
$ LANGUAGE plpgsql;
"""
            return converted
        
        return pattern.sub(replace_procedure, sql)

    def _convert_functions(self, sql: str, stats: Dict[str, Any]) -> str:
        """Converte UDFs externas do Firebird"""
        pattern = re.compile(
            r'DECLARE\s+EXTERNAL\s+FUNCTION\s+(\w+).*?;',
            flags=re.IGNORECASE | re.DOTALL
        )
        
        def replace_udf(match):
            func_name = match.group(1)
            stats['warnings'].append(f"UDF externa {func_name} removida - implementar em PostgreSQL")
            stats['converted_objects']['functions'] += 1
            return f"-- UDF {func_name} removed - implement in PostgreSQL if needed\n"
        
        return pattern.sub(replace_udf, sql)

    def _convert_views(self, sql: str, stats: Dict[str, Any]) -> str:
        """Conta e registra views convertidas"""
        pattern = re.compile(r'CREATE\s+(OR\s+ALTER\s+)?VIEW\s+(\w+)', flags=re.IGNORECASE)
        
        def count_view(match):
            stats['converted_objects']['views'] += 1
            return match.group(0)
        
        return pattern.sub(count_view, sql)

    def _convert_indexes(self, sql: str, stats: Dict[str, Any]) -> str:
        """Converte índices do Firebird para PostgreSQL"""
        pattern = re.compile(
            r'CREATE\s+(UNIQUE\s+)?(?:ASC|DESC)?\s*INDEX\s+(\w+)\s+ON\s+(\w+)\s*\(([^)]+)\)(?:\s*COMPUTED\s+BY\s*\([^)]+\))?', 
            flags=re.IGNORECASE
        )
        
        def replace_index(match):
            unique = match.group(1) or ''
            index_name = match.group(2)
            table_name = match.group(3)
            columns = match.group(4)
            
            stats['converted_objects']['indexes'] += 1
            
            # Remover COMPUTED BY se existir
            return f'CREATE {unique}INDEX "{index_name.lower()}" ON "{table_name.lower()}" ({columns});'
        
        return pattern.sub(replace_index, sql)

    def _fix_data_types(self, sql: str, stats: Dict[str, Any]) -> str:
        """Correções adicionais de tipos de dados"""
        fixes = [
            (r'\bD_FLOAT\b', 'DOUBLE PRECISION'),
            (r'\bQUAD\b', 'BIGINT'),
            (r'\bINT64\b', 'BIGINT'),
            (r'\bCSTRING\b', 'VARCHAR'),
        ]
        
        for pattern, replacement in fixes:
            sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)
        
        return sql

    def _post_process_sql(self, sql: str, stats: Dict[str, Any]) -> str:
        """Pós-processamento do SQL"""
        # Remover múltiplas linhas em branco
        sql = re.sub(r'\n\s*\n\s*\n+', '\n\n', sql)
        
        # Corrigir terminadores de statement
        sql = re.sub(r';\s*\^', ';', sql)
        sql = re.sub(r'\^\s*', '', sql)
        
        # Cabeçalho do PostgreSQL
        header = f"""-- SQL Script converted from Firebird to PostgreSQL
-- Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- 
-- IMPORTANT: This script requires manual review before execution
-- Review all triggers, procedures, and custom functions
--

-- PostgreSQL configuration
SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

"""
        
        sql_content = header + sql.strip() + '\n'
        
        # Contagem final de objetos
        stats['converted_objects']['tables'] = len(re.findall(r'CREATE\s+TABLE', sql_content, re.IGNORECASE))
        stats['converted_objects']['sequences'] = len(re.findall(r'CREATE\s+SEQUENCE', sql_content, re.IGNORECASE))
        
        return sql_content
