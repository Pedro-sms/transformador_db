import logging
import re
from datetime import datetime
from typing import Tuple, Dict, Any, List, Optional
from firebird_client import FirebirdClient, TableSchema
from schema_converter import SchemaConverter
from data_migrator import DataMigrator

logger = logging.getLogger(__name__)

class SQLGenerator:
    def __init__(self, db_path: str, batch_size: int = 1000, include_data: bool = True):
        self.db_path = db_path
        self.batch_size = batch_size  # Reduzido para debug
        self.include_data = include_data
        self.firebird_client = FirebirdClient(db_path)
        self.schema_converter = SchemaConverter()
        self.data_migrator = DataMigrator(self.firebird_client, batch_size)
        self.stats = {
            'total_tables': 0,
            'processed_tables_schema': 0,
            'processed_tables_data': 0,
            'total_rows': 0,
            'processed_rows': 0,
            'total_sequences': 0,
            'processed_sequences': 0,
            'total_views': 0,
            'processed_views': 0,
            'errors': [],
            'warnings': [],
            'start_time': None,
            'end_time': None,
            'duration': None
        }
    
    def generate_complete_sql(self) -> Tuple[str, dict]:
        """Gera o SQL completo para migração do banco com tratamento robusto de erros"""
        self.stats['start_time'] = datetime.now()
        logger.info(f"Iniciando geração de SQL para {self.db_path}")
        
        try:
            # Conectar ao banco
            if not self.firebird_client.connect():
                error_msg = f"Falha ao conectar ao banco Firebird: {self.db_path}"
                self.stats['errors'].append(error_msg)
                logger.error(error_msg)
                return "", self.stats
            
            # Testar conexão e obter informações básicas
            connection_info = self.firebird_client.test_connection()
            if connection_info['status'] == 'error':
                error_msg = f"Erro na conexão: {connection_info['message']}"
                self.stats['errors'].append(error_msg)
                logger.error(error_msg)
                return "", self.stats
            
            logger.info(f"Conectado ao Firebird {connection_info.get('firebird_version', 'Unknown')} "
                       f"com charset {connection_info.get('charset', 'Unknown')}")
            
            # Obter listas de objetos
            tables = self.firebird_client.get_tables()
            views = self.firebird_client.get_views()
            generators = self.firebird_client.get_generators()
            
            self.stats['total_tables'] = len(tables)
            self.stats['total_views'] = len(views)
            self.stats['total_sequences'] = len(generators)
            
            logger.info(f"Encontrados: {len(tables)} tabelas, {len(views)} views, {len(generators)} generators")
            
            # Calcular total de registros para estatísticas
            total_rows = 0
            for table in tables:
                try:
                    count = self.firebird_client.get_table_count(table)
                    total_rows += count
                    logger.info(f"Tabela {table}: {count:,} registros")
                except Exception as e:
                    logger.warning(f"Erro ao contar registros da tabela {table}: {str(e)}")
            
            self.stats['total_rows'] = total_rows
            
            # Gerar componentes do SQL
            components = []
            
            # 1. Gerar sequences (generators)
            if generators:
                sequences_sql, sequences_stats = self._generate_sequences_sql(generators)
                if sequences_sql:
                    components.extend(sequences_sql)
                self._update_stats(sequences_stats)
            
            # 2. Gerar schema das tabelas
            schema_sql, schema_stats = self._generate_schema_sql(tables)
            if schema_sql:
                components.extend(schema_sql)
            self._update_stats(schema_stats)
            
            # 3. Gerar views
            if views:
                views_sql, views_stats = self._generate_views_sql(views)
                if views_sql:
                    components.extend(views_sql)
                self._update_stats(views_stats)
            
            # 4. Gerar dados se solicitado
            if self.include_data and not self.stats['errors']:
                data_sql, data_stats = self._generate_data_sql(tables)
                if data_sql:
                    components.extend(data_sql)
                self._update_stats(data_stats)
            
            # 5. Gerar constraints após os dados para evitar problemas
            constraints_sql, constraints_stats = self._generate_constraints_sql(tables)
            if constraints_sql:
                components.extend(constraints_sql)
            self._update_stats(constraints_stats)
            
            # Combinar todos os componentes
            complete_sql = self._combine_sql_components(components)
            
            self.stats['end_time'] = datetime.now()
            self.stats['duration'] = self.stats['end_time'] - self.stats['start_time']
            
            logger.info(f"Geração concluída em {self.stats['duration']}. "
                       f"Processadas {self.stats['processed_tables_schema']} tabelas, "
                       f"{self.stats['processed_rows']} registros")
            
            return complete_sql, self.stats
            
        except Exception as e:
            error_msg = f"Erro inesperado durante a geração do SQL: {str(e)}"
            self.stats['errors'].append(error_msg)
            logger.exception(error_msg)
            return "", self.stats
        finally:
            self.firebird_client.close()
    
    def _generate_sequences_sql(self, generators: List[Dict[str, Any]]) -> Tuple[List[str], Dict[str, Any]]:
        """Gera SQL para criação de sequences (conversão de generators)"""
        sequences_sql = ["-- SEQUENCES (converted from Firebird generators)\n"]
        stats = {
            'processed_sequences': 0,
            'errors': [],
            'warnings': []
        }
        
        try:
            sequence_statements = self.schema_converter.generate_sequences(generators)
            if sequence_statements:
                sequences_sql.extend(sequence_statements)
                sequences_sql.append("")  # Linha em branco
                stats['processed_sequences'] = len(generators)
                logger.info(f"Processadas {len(generators)} sequences")
            
        except Exception as e:
            error_msg = f"Erro ao processar sequences: {str(e)}"
            stats['errors'].append(error_msg)
            logger.error(error_msg)
        
        return sequences_sql, stats
    
    def _generate_schema_sql(self, tables: List[str]) -> Tuple[List[str], Dict[str, Any]]:
        """Gera SQL para criação das tabelas (estrutura básica sem constraints)"""
        schema_sql = ["-- TABLES SCHEMA (structure without foreign keys)\n"]
        stats = {
            'processed_tables_schema': 0,
            'errors': [],
            'warnings': []
        }
        
        for table in tables:
            try:
                logger.info(f"Processando schema da tabela: {table}")
                table_schema = self.firebird_client.get_table_schema(table)
                table_schema_sql = self.schema_converter.convert_table_schema(table_schema)
                
                # Garantir terminação com ponto e vírgula
                if not table_schema_sql.strip().endswith(';'):
                    table_schema_sql += ';'
                
                schema_sql.append(f"-- Table: {table} ({table_schema.row_count:,} rows)")
                schema_sql.append(table_schema_sql)
                schema_sql.append("")  # Linha em branco para separação
                
                stats['processed_tables_schema'] += 1
                
            except Exception as e:
                error_msg = f"Erro no schema da tabela {table}: {str(e)}"
                stats['errors'].append(error_msg)
                logger.error(error_msg)
        
        return schema_sql, stats
    
    def _generate_views_sql(self, views: List[str]) -> Tuple[List[str], Dict[str, Any]]:
        """Gera SQL para criação das views"""
        views_sql = ["\n-- VIEWS\n"]
        stats = {
            'processed_views': 0,
            'errors': [],
            'warnings': []
        }
        
        for view in views:
            try:
                logger.info(f"Processando view: {view}")
                
                # Obter definição da view
                cursor = self.firebird_client.connection.cursor()
                cursor.execute(f"""
                    SELECT RDB$VIEW_SOURCE 
                    FROM RDB$RELATIONS 
                    WHERE RDB$RELATION_NAME = '{view}'
                """)
                
                result = cursor.fetchone()
                if result and result[0]:
                    view_definition = result[0].strip()
                    converted_view = self.schema_converter.convert_view_schema(view, view_definition)
                    
                    views_sql.append(f"-- View: {view}")
                    views_sql.append(converted_view)
                    views_sql.append("")
                    
                    stats['processed_views'] += 1
                    stats['warnings'].append(f"View {view} convertida - revisar sintaxe se necessário")
                else:
                    stats['warnings'].append(f"Não foi possível obter definição da view {view}")
                
            except Exception as e:
                error_msg = f"Erro ao processar view {view}: {str(e)}"
                stats['errors'].append(error_msg)
                logger.error(error_msg)
        
        return views_sql, stats
    
    def _generate_constraints_sql(self, tables: List[str]) -> Tuple[List[str], Dict[str, Any]]:
        """Gera SQL para constraints (índices, FKs) - executado APÓS inserção dos dados"""
        constraints_sql = ["\n-- CONSTRAINTS AND INDEXES\n"]
        stats = {
            'processed_constraints': 0,
            'errors': [],
            'warnings': []
        }
        
        for table in tables:
            try:
                logger.info(f"Processando constraints da tabela: {table}")
                table_schema = self.firebird_client.get_table_schema(table)
                constraints = self.schema_converter.generate_constraints(table_schema)
                
                if constraints:
                    constraints_sql.append(f"-- Constraints and indexes for table: {table}")
                    constraints_sql.extend(constraints)
                    constraints_sql.append("")  # Linha em branco
                    stats['processed_constraints'] += len(constraints)
                    
            except Exception as e:
                error_msg = f"Erro nas constraints da tabela {table}: {str(e)}"
                stats['errors'].append(error_msg)
                logger.error(error_msg)
        
        return constraints_sql, stats
    
    def _generate_data_sql(self, tables: List[str]) -> Tuple[List[str], Dict[str, Any]]:
        """Gera SQL para inserção de dados"""
        data_sql = ["\n-- DATA MIGRATION\n"]
        stats = {
            'processed_tables_data': 0,
            'processed_rows': 0,
            'errors': [],
            'warnings': []
        }
        
        # Ordenar tabelas por dependências (aproximação simples)
        sorted_tables = self._sort_tables_by_dependencies(tables)
        
        for table in sorted_tables:
            try:
                logger.info(f"Processando dados da tabela: {table}")
                table_schema = self.firebird_client.get_table_schema(table)
                
                if table_schema.row_count > 0:
                    # Migrar dados da tabela
                    table_data_sql = self.data_migrator.migrate_table_data(
                        table, 
                        table_schema.row_count, 
                        table_schema,
                        use_copy=False,  # Sempre usar INSERT para debug
                        use_transactions=False  # Transações serão controladas no nível superior
                    )
                    
                    if table_data_sql.strip():
                        data_sql.append(table_data_sql)
                        
                        stats['processed_rows'] += table_schema.row_count
                        stats['processed_tables_data'] += 1
                        
                        logger.info(f"Tabela {table}: {table_schema.row_count:,} registros processados")
                else:
                    data_sql.append(f"-- Tabela {table} está vazia")
                    data_sql.append("")
                    logger.info(f"Tabela {table} está vazia, pulando dados")
                
            except Exception as e:
                error_msg = f"Erro nos dados da tabela {table}: {str(e)}"
                stats['errors'].append(error_msg)
                logger.error(error_msg)
        
        return data_sql, stats
    
    def _sort_tables_by_dependencies(self, tables: List[str]) -> List[str]:
        """Ordena tabelas aproximadamente por dependências de chaves estrangeiras"""
        table_dependencies = {}
        
        # Construir grafo de dependências simples
        for table in tables:
            try:
                table_schema = self.firebird_client.get_table_schema(table)
                dependencies = []
                
                for fk in table_schema.foreign_keys:
                    target_table = fk.get('target_table', '')
                    if target_table and target_table in tables and target_table != table:
                        dependencies.append(target_table)
                
                table_dependencies[table] = dependencies
                
            except Exception as e:
                logger.warning(f"Erro ao analisar dependências da tabela {table}: {str(e)}")
                table_dependencies[table] = []
        
        # Ordenação topológica simples
        sorted_tables = []
        remaining_tables = set(tables)
        
        max_iterations = len(tables) * 2  # Prevenir loop infinito
        iteration = 0
        
        while remaining_tables and iteration < max_iterations:
            iteration += 1
            
            # Encontrar tabelas sem dependências restantes
            ready_tables = []
            for table in remaining_tables:
                deps = table_dependencies.get(table, [])
                if not any(dep in remaining_tables for dep in deps):
                    ready_tables.append(table)
            
            if not ready_tables:
                # Ciclo detectado ou erro - adicionar tabelas restantes
                ready_tables = list(remaining_tables)
                logger.warning("Possível dependência circular detectada entre tabelas")
            
            sorted_tables.extend(ready_tables)
            remaining_tables -= set(ready_tables)
        
        return sorted_tables
    
    def _update_stats(self, new_stats: Dict[str, Any]):
        """Atualiza as estatísticas com novos valores"""
        for key, value in new_stats.items():
            if key in self.stats:
                if isinstance(value, list):
                    self.stats[key].extend(value)
                elif isinstance(value, (int, float)):
                    self.stats[key] += value
    
    def _combine_sql_components(self, components: List[str]) -> str:
        """Combina todos os componentes SQL em uma string final"""
        
        # Cabeçalho completo
        header = f"""-- PostgreSQL Database Schema and Data
-- Converted from Firebird database: {self.db_path}
-- Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- Include data: {self.include_data}
-- Batch size: {self.batch_size:,}
--
-- IMPORTANT NOTES:
-- 1. Review all triggers and stored procedures manually
-- 2. Verify data types and constraints
-- 3. Test thoroughly before production use
-- 4. Some Firebird-specific features may need manual conversion
--

-- PostgreSQL Configuration
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;
SET default_tablespace = '';
SET default_table_access_method = heap;

-- Disable foreign key checks during migration
SET session_replication_role = replica;

"""
        
        # Combinar componentes
        complete_sql = header + "\n".join(components)
        
        # Rodapé
        footer = f"""
-- Re-enable foreign key checks
SET session_replication_role = DEFAULT;

-- Migration Statistics:
-- Tables processed: {self.stats['processed_tables_schema']}/{self.stats['total_tables']}
-- Data rows migrated: {self.stats['processed_rows']:,}/{self.stats['total_rows']:,}
-- Sequences created: {self.stats['processed_sequences']}/{self.stats['total_sequences']}
-- Views created: {self.stats['processed_views']}/{self.stats['total_views']}
-- Errors: {len(self.stats['errors'])}
-- Warnings: {len(self.stats['warnings'])}

-- End of migration script
"""
        
        return complete_sql + footer
    
    def save_to_file(self, sql_content: str, file_path: str):
        """Salva o conteúdo SQL em um arquivo com encoding correto"""
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(sql_content)
            logger.info(f"SQL salvo com sucesso em: {file_path}")
            
            # Calcular tamanho do arquivo
            import os
            file_size = os.path.getsize(file_path)
            logger.info(f"Tamanho do arquivo gerado: {file_size / (1024*1024):.2f} MB")
            
        except Exception as e:
            error_msg = f"Erro ao salvar arquivo {file_path}: {str(e)}"
            self.stats['errors'].append(error_msg)
            logger.error(error_msg)
            raise
    
    def generate_migration_report(self) -> str:
        """Gera relatório detalhado da migração"""
        if not self.stats['start_time']:
            return "Nenhuma migração foi executada ainda."
        
        duration = self.stats.get('duration', 'N/A')
        
        report = f"""
RELATÓRIO DE MIGRAÇÃO FIREBIRD → POSTGRESQL
==========================================

Arquivo fonte: {self.db_path}
Data/Hora: {self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}
Duração: {duration}
Incluir dados: {'Sim' if self.include_data else 'Não'}
Tamanho do lote: {self.batch_size:,}

ESTATÍSTICAS:
- Tabelas: {self.stats['processed_tables_schema']}/{self.stats['total_tables']}
- Registros migrados: {self.stats['processed_rows']:,}/{self.stats['total_rows']:,}
- Sequences: {self.stats['processed_sequences']}/{self.stats['total_sequences']}
- Views: {self.stats['processed_views']}/{self.stats['total_views']}
- Constraints: {self.stats.get('processed_constraints', 0)}

RESULTADO:
- Erros: {len(self.stats['errors'])}
- Avisos: {len(self.stats['warnings'])}

"""
        
        if self.stats['errors']:
            report += "ERROS ENCONTRADOS:\n"
            for i, error in enumerate(self.stats['errors'][:10], 1):
                report += f"{i}. {error}\n"
            if len(self.stats['errors']) > 10:
                report += f"... e mais {len(self.stats['errors']) - 10} erros\n"
        
        if self.stats['warnings']:
            report += "\nAVISOS:\n"
            for i, warning in enumerate(self.stats['warnings'][:10], 1):
                report += f"{i}. {warning}\n"
            if len(self.stats['warnings']) > 10:
                report += f"... e mais {len(self.stats['warnings']) - 10} avisos\n"
        
        return report
