import fdb
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import datetime
import decimal

logger = logging.getLogger(__name__)

@dataclass
class TableSchema:
    name: str
    columns: List[Dict[str, Any]]
    primary_key: Optional[List[str]]
    indexes: List[Dict[str, Any]]
    foreign_keys: List[Dict[str, Any]]
    check_constraints: List[Dict[str, Any]]
    row_count: int

class FirebirdClient:
    def __init__(self, db_path: str, user: str = 'SYSDBA', password: str = 'masterkey', 
                 charset: str = 'UTF8', timeout: int = 30):
        self.db_path = db_path
        self.user = user
        self.password = password
        self.charset = charset
        self.timeout = timeout
        self.connection = None
        self.firebird_version = None

    def connect(self) -> bool:
        """Estabelece conexão com o banco Firebird com tratamento robusto de diferentes cenários"""
        try:
            # Tentar diferentes charsets se UTF8 falhar
            charsets = [self.charset, 'WIN1252', 'ISO8859_1', 'NONE', 'LATIN1']
            
            for charset in charsets:
                try:
                    logger.info(f"Tentando conectar com charset: {charset}")
                    self.connection = fdb.connect(
                        dsn=self.db_path,
                        user=self.user,
                        password=self.password,
                        charset=charset
                    )
                    self.charset = charset  # Atualiza charset usado
                    logger.info(f"Conectado ao Firebird com charset: {charset}")
                    self._get_firebird_version()
                    return True
                    
                except fdb.DatabaseError as e:
                    logger.warning(f"Falha ao conectar com charset {charset}: {str(e)}")
                    continue
                except Exception as e:
                    logger.warning(f"Erro inesperado com charset {charset}: {str(e)}")
                    continue
            
            raise Exception("Não foi possível conectar com nenhum charset disponível")
            
        except Exception as e:
            logger.error(f"Erro ao conectar ao Firebird: {str(e)}")
            return False

    def _get_firebird_version(self):
        """Obtém a versão do Firebird para ajustar queries se necessário"""
        try:
            cursor = self.connection.cursor()
            # Tentar diferentes métodos para obter a versão
            try:
                cursor.execute("SELECT rdb$get_context('SYSTEM', 'ENGINE_VERSION') from rdb$database")
                result = cursor.fetchone()
                if result and result[0]:
                    self.firebird_version = result[0].strip()
                    return
            except:
                pass
            
            # Método alternativo
            try:
                cursor.execute("SELECT rdb$get_context('SYSTEM', 'FIREBIRD_VERSION') from rdb$database")
                result = cursor.fetchone()
                if result and result[0]:
                    self.firebird_version = result[0].strip()
                    return
            except:
                pass
            
            self.firebird_version = "Unknown"
            logger.info(f"Versão do Firebird: {self.firebird_version}")
            
        except Exception as e:
            logger.warning(f"Não foi possível obter versão do Firebird: {str(e)}")
            self.firebird_version = "Unknown"

    def get_tables(self) -> List[str]:
        """Obtém lista de tabelas do banco, excluindo tabelas de sistema"""
        if not self.connection:
            if not self.connect():
                raise Exception("Não foi possível conectar ao banco")
        
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                SELECT TRIM(RDB$RELATION_NAME) as TABLE_NAME
                FROM RDB$RELATIONS 
                WHERE RDB$VIEW_BLR IS NULL 
                AND (RDB$SYSTEM_FLAG IS NULL OR RDB$SYSTEM_FLAG = 0)
                AND RDB$RELATION_TYPE = 0
                AND RDB$RELATION_NAME NOT STARTING WITH 'RDB$'
                AND RDB$RELATION_NAME NOT STARTING WITH 'MON$'
                ORDER BY RDB$RELATION_NAME
            """)
            
            tables = [row[0] for row in cursor.fetchall() if row[0]]
            logger.info(f"Encontradas {len(tables)} tabelas no banco")
            return tables
            
        except Exception as e:
            logger.error(f"Erro ao obter lista de tabelas: {str(e)}")
            raise

    def get_views(self) -> List[str]:
        """Obtém lista de views do banco"""
        if not self.connection:
            if not self.connect():
                raise Exception("Não foi possível conectar ao banco")
        
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                SELECT TRIM(RDB$RELATION_NAME) as VIEW_NAME
                FROM RDB$RELATIONS 
                WHERE RDB$VIEW_BLR IS NOT NULL 
                AND (RDB$SYSTEM_FLAG IS NULL OR RDB$SYSTEM_FLAG = 0)
                AND RDB$RELATION_NAME NOT STARTING WITH 'RDB$'
                ORDER BY RDB$RELATION_NAME
            """)
            
            return [row[0] for row in cursor.fetchall() if row[0]]
            
        except Exception as e:
            logger.error(f"Erro ao obter lista de views: {str(e)}")
            return []

    def get_generators(self) -> List[Dict[str, Any]]:
        """Obtém lista de generators/sequences"""
        if not self.connection:
            if not self.connect():
                raise Exception("Não foi possível conectar ao banco")
        
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                SELECT 
                    TRIM(RDB$GENERATOR_NAME) as GEN_NAME,
                    RDB$GENERATOR_ID,
                    COALESCE(GEN_ID(RDB$GENERATOR_NAME, 0), 0) as CURRENT_VALUE
                FROM RDB$GENERATORS 
                WHERE (RDB$SYSTEM_FLAG IS NULL OR RDB$SYSTEM_FLAG = 0)
                AND RDB$GENERATOR_NAME NOT STARTING WITH 'RDB$'
                ORDER BY RDB$GENERATOR_NAME
            """)
            
            generators = []
            for row in cursor.fetchall():
                if row[0]:  # Verificar se nome não é None
                    generators.append({
                        'name': row[0],
                        'id': row[1] or 0,
                        'current_value': row[2] or 0
                    })
            
            return generators
            
        except Exception as e:
            logger.error(f"Erro ao obter generators: {str(e)}")
            return []

    def get_table_schema(self, table_name: str) -> TableSchema:
        """Obtém schema completo de uma tabela"""
        cursor = self.connection.cursor()
        
        try:
            # Obter informações das colunas
            cursor.execute(f"""
                SELECT 
                    TRIM(RF.RDB$FIELD_NAME) as FIELD_NAME,
                    F.RDB$FIELD_TYPE,
                    COALESCE(F.RDB$FIELD_LENGTH, 0) as FIELD_LENGTH,
                    COALESCE(F.RDB$FIELD_PRECISION, 0) as FIELD_PRECISION,
                    COALESCE(F.RDB$FIELD_SCALE, 0) as FIELD_SCALE,
                    COALESCE(F.RDB$FIELD_SUB_TYPE, 0) as FIELD_SUB_TYPE,
                    RF.RDB$NULL_FLAG,
                    RF.RDB$DEFAULT_SOURCE,
                    RF.RDB$DESCRIPTION,
                    COALESCE(RF.RDB$FIELD_POSITION, 0) as FIELD_POSITION
                FROM RDB$RELATION_FIELDS RF
                JOIN RDB$FIELDS F ON RF.RDB$FIELD_SOURCE = F.RDB$FIELD_NAME
                WHERE RF.RDB$RELATION_NAME = '{table_name}'
                ORDER BY RF.RDB$FIELD_POSITION
            """)
            
            columns = []
            for row in cursor.fetchall():
                column = {
                    'name': row[0],
                    'type': row[1] or 0,
                    'length': row[2] or 0,
                    'precision': row[3] or 0,
                    'scale': row[4] or 0,
                    'subtype': row[5] or 0,
                    'nullable': row[6] is None,
                    'default': row[7].strip() if row[7] else None,
                    'description': row[8].strip() if row[8] else None,
                    'position': row[9] or 0
                }
                columns.append(column)

            # Obter chave primária
            primary_key = self._get_primary_key(table_name, cursor)
            
            # Obter índices
            indexes = self._get_indexes(table_name, cursor)
            
            # Obter chaves estrangeiras
            foreign_keys = self._get_foreign_keys(table_name, cursor)
            
            # Obter check constraints
            check_constraints = self._get_check_constraints(table_name, cursor)
            
            # Obter contagem de linhas
            try:
                cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                row_count = cursor.fetchone()[0] or 0
            except Exception as e:
                logger.warning(f"Não foi possível contar linhas da tabela {table_name}: {str(e)}")
                row_count = 0

            return TableSchema(
                name=table_name,
                columns=columns,
                primary_key=primary_key,
                indexes=indexes,
                foreign_keys=foreign_keys,
                check_constraints=check_constraints,
                row_count=row_count
            )
            
        except Exception as e:
            logger.error(f"Erro ao obter schema da tabela {table_name}: {str(e)}")
            raise

    def _get_primary_key(self, table_name: str, cursor) -> Optional[List[str]]:
        """Obtém chave primária da tabela"""
        try:
            cursor.execute(f"""
                SELECT TRIM(SG.RDB$FIELD_NAME) as FIELD_NAME
                FROM RDB$INDICES IX
                JOIN RDB$INDEX_SEGMENTS SG ON IX.RDB$INDEX_NAME = SG.RDB$INDEX_NAME
                WHERE IX.RDB$RELATION_NAME = '{table_name}' 
                AND (IX.RDB$INDEX_NAME STARTING WITH 'RDB$PRIMARY' OR IX.RDB$INDEX_NAME STARTING WITH 'PK_')
                ORDER BY SG.RDB$FIELD_POSITION
            """)
            
            pk_fields = [row[0] for row in cursor.fetchall() if row[0]]
            return pk_fields if pk_fields else None
            
        except Exception as e:
            logger.warning(f"Erro ao obter chave primária da tabela {table_name}: {str(e)}")
            return None

    def _get_indexes(self, table_name: str, cursor) -> List[Dict[str, Any]]:
        """Obtém índices da tabela"""
        try:
            cursor.execute(f"""
                SELECT 
                    TRIM(IX.RDB$INDEX_NAME) as INDEX_NAME,
                    TRIM(SG.RDB$FIELD_NAME) as FIELD_NAME,
                    COALESCE(IX.RDB$UNIQUE_FLAG, 0) as UNIQUE_FLAG,
                    COALESCE(SG.RDB$FIELD_POSITION, 0) as FIELD_POSITION,
                    COALESCE(IX.RDB$INDEX_TYPE, 0) as INDEX_TYPE
                FROM RDB$INDICES IX
                JOIN RDB$INDEX_SEGMENTS SG ON IX.RDB$INDEX_NAME = SG.RDB$INDEX_NAME
                WHERE IX.RDB$RELATION_NAME = '{table_name}' 
                AND NOT (IX.RDB$INDEX_NAME STARTING WITH 'RDB$PRIMARY' OR IX.RDB$INDEX_NAME STARTING WITH 'RDB$FOREIGN')
                AND IX.RDB$INDEX_NAME IS NOT NULL
                ORDER BY IX.RDB$INDEX_NAME, SG.RDB$FIELD_POSITION
            """)
            
            indexes_dict = {}
            for row in cursor.fetchall():
                if not row[0]:  # Skip if index name is None
                    continue
                    
                index_name = row[0]
                field_name = row[1]
                
                if index_name not in indexes_dict:
                    indexes_dict[index_name] = {
                        'name': index_name,
                        'fields': [],
                        'unique': row[2] == 1,
                        'type': row[4] or 0
                    }
                
                if field_name:
                    indexes_dict[index_name]['fields'].append(field_name)
            
            return list(indexes_dict.values())
            
        except Exception as e:
            logger.warning(f"Erro ao obter índices da tabela {table_name}: {str(e)}")
            return []

    def _get_foreign_keys(self, table_name: str, cursor) -> List[Dict[str, Any]]:
        """Obtém chaves estrangeiras da tabela"""
        try:
            cursor.execute(f"""
                SELECT DISTINCT
                    TRIM(RC.RDB$CONSTRAINT_NAME) as CONSTRAINT_NAME,
                    TRIM(RC.RDB$RELATION_NAME) as SOURCE_TABLE,
                    TRIM(ISP.RDB$FIELD_NAME) as SOURCE_FIELD,
                    TRIM(RC.RDB$CONST_NAME_UQ) as TARGET_CONSTRAINT,
                    TRIM(REF.RDB$RELATION_NAME) as TARGET_TABLE,
                    TRIM(ISR.RDB$FIELD_NAME) as TARGET_FIELD,
                    TRIM(COALESCE(RC.RDB$DELETE_RULE, 'NO ACTION')) as DELETE_RULE,
                    TRIM(COALESCE(RC.RDB$UPDATE_RULE, 'NO ACTION')) as UPDATE_RULE
                FROM RDB$REF_CONSTRAINTS RC
                JOIN RDB$RELATION_CONSTRAINTS RCO ON RC.RDB$CONSTRAINT_NAME = RCO.RDB$CONSTRAINT_NAME
                JOIN RDB$INDEX_SEGMENTS ISP ON RCO.RDB$INDEX_NAME = ISP.RDB$INDEX_NAME
                JOIN RDB$RELATION_CONSTRAINTS REF_RCO ON RC.RDB$CONST_NAME_UQ = REF_RCO.RDB$CONSTRAINT_NAME
                JOIN RDB$INDEX_SEGMENTS ISR ON REF_RCO.RDB$INDEX_NAME = ISR.RDB$INDEX_NAME
                JOIN RDB$RELATIONS REF ON REF_RCO.RDB$RELATION_NAME = REF.RDB$RELATION_NAME
                WHERE RC.RDB$RELATION_NAME = '{table_name}'
                AND RC.RDB$CONSTRAINT_NAME IS NOT NULL
                ORDER BY RC.RDB$CONSTRAINT_NAME
            """)
            
            fks = []
            for row in cursor.fetchall():
                if row[0]:  # Skip if constraint name is None
                    fks.append({
                        'name': row[0],
                        'source_table': row[1] or table_name,
                        'source_field': row[2] or '',
                        'target_table': row[4] or '',
                        'target_field': row[5] or '',
                        'delete_rule': row[6] or 'NO ACTION',
                        'update_rule': row[7] or 'NO ACTION'
                    })
            
            return fks
            
        except Exception as e:
            logger.warning(f"Erro ao obter chaves estrangeiras da tabela {table_name}: {str(e)}")
            return []

    def _get_check_constraints(self, table_name: str, cursor) -> List[Dict[str, Any]]:
        """Obtém check constraints da tabela"""
        try:
            cursor.execute(f"""
                SELECT 
                    TRIM(CC.RDB$CONSTRAINT_NAME) as CONSTRAINT_NAME,
                    TRIM(CC.RDB$TRIGGER_NAME) as TRIGGER_NAME,
                    T.RDB$TRIGGER_SOURCE
                FROM RDB$CHECK_CONSTRAINTS CC
                JOIN RDB$RELATION_CONSTRAINTS RC ON CC.RDB$CONSTRAINT_NAME = RC.RDB$CONSTRAINT_NAME
                LEFT JOIN RDB$TRIGGERS T ON CC.RDB$TRIGGER_NAME = T.RDB$TRIGGER_NAME
                WHERE RC.RDB$RELATION_NAME = '{table_name}'
                AND CC.RDB$CONSTRAINT_NAME IS NOT NULL
            """)
            
            constraints = []
            for row in cursor.fetchall():
                if row[0]:  # Skip if constraint name is None
                    constraints.append({
                        'name': row[0],
                        'trigger_name': row[1] or '',
                        'condition': row[2].strip() if row[2] else ''
                    })
            
            return constraints
            
        except Exception as e:
            logger.warning(f"Erro ao obter check constraints da tabela {table_name}: {str(e)}")
            return []

    def get_table_data_batch(self, table_name: str, offset: int, limit: int) -> List[Dict]:
        """
        Obtém um lote de dados de uma tabela usando sintaxe correta do Firebird
        Com tratamento melhor de tipos de dados
        """
        cursor = self.connection.cursor()
        
        try:
            # Sintaxe correta para Firebird (FIRST/SKIP ao invés de OFFSET/FETCH)
            if offset > 0:
                query = f'SELECT FIRST {limit} SKIP {offset} * FROM "{table_name}"'
            else:
                query = f'SELECT FIRST {limit} * FROM "{table_name}"'
            
            cursor.execute(query)
            columns = [desc[0].strip() for desc in cursor.description]
            data = cursor.fetchall()
            
            # Converter para lista de dicionários com tratamento de tipos
            result = []
            for row in data:
                row_dict = {}
                for i, value in enumerate(row):
                    if i < len(columns):
                        # Tratamento específico para diferentes tipos
                        if value is not None:
                            if isinstance(value, (bytes, bytearray)):
                                # Manter bytes como bytes para posterior conversão
                                row_dict[columns[i]] = value
                            elif isinstance(value, str):
                                # Limpar strings de caracteres de controle problemáticos
                                cleaned_value = value.replace('\x00', '').strip()
                                row_dict[columns[i]] = cleaned_value
                            elif isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
                                # Manter objetos datetime como estão
                                row_dict[columns[i]] = value
                            elif isinstance(value, decimal.Decimal):
                                # Manter decimais como estão
                                row_dict[columns[i]] = value
                            else:
                                row_dict[columns[i]] = value
                        else:
                            row_dict[columns[i]] = None
                result.append(row_dict)
            
            return result
            
        except Exception as e:
            logger.error(f"Erro ao buscar dados da tabela {table_name} (offset={offset}, limit={limit}): {str(e)}")
            raise

    def get_table_count(self, table_name: str) -> int:
        """Obtém contagem total de registros de uma tabela"""
        cursor = self.connection.cursor()
        try:
            cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
            result = cursor.fetchone()
            return result[0] if result else 0
        except Exception as e:
            logger.error(f"Erro ao contar registros da tabela {table_name}: {str(e)}")
            return 0

    def test_connection(self) -> Dict[str, Any]:
        """Testa conexão e retorna informações do banco"""
        try:
            if not self.connection:
                if not self.connect():
                    return {'status': 'error', 'message': 'Falha na conexão'}
            
            cursor = self.connection.cursor()
            
            # Informações básicas do banco
            cursor.execute("""
                SELECT COUNT(*) 
                FROM RDB$RELATIONS 
                WHERE RDB$VIEW_BLR IS NULL 
                AND (RDB$SYSTEM_FLAG IS NULL OR RDB$SYSTEM_FLAG = 0)
                AND RDB$RELATION_NAME NOT STARTING WITH 'RDB$'
            """)
            table_count = cursor.fetchone()[0] or 0
            
            cursor.execute("""
                SELECT COUNT(*) 
                FROM RDB$RELATIONS 
                WHERE RDB$VIEW_BLR IS NOT NULL 
                AND (RDB$SYSTEM_FLAG IS NULL OR RDB$SYSTEM_FLAG = 0)
                AND RDB$RELATION_NAME NOT STARTING WITH 'RDB$'
            """)
            view_count = cursor.fetchone()[0] or 0
            
            cursor.execute("""
                SELECT COUNT(*) 
                FROM RDB$GENERATORS 
                WHERE (RDB$SYSTEM_FLAG IS NULL OR RDB$SYSTEM_FLAG = 0)
                AND RDB$GENERATOR_NAME NOT STARTING WITH 'RDB$'
            """)
            generator_count = cursor.fetchone()[0] or 0
            
            return {
                'status': 'success',
                'firebird_version': self.firebird_version,
                'charset': self.charset,
                'table_count': table_count,
                'view_count': view_count,
                'generator_count': generator_count
            }
            
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    def close(self):
        """Fecha a conexão com o banco"""
        if self.connection:
            try:
                self.connection.close()
                logger.info("Conexão com Firebird fechada")
            except Exception as e:
                logger.error(f"Erro ao fechar conexão: {str(e)}")
            finally:
                self.connection = None

    def __enter__(self):
        """Context manager entry"""
        if self.connect():
            return self
        else:
            raise Exception("Falha ao conectar ao banco Firebird")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
