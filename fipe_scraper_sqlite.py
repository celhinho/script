
# fipe_scraper_sqlite.py
# VERSÃO FINAL - TIMING PERFEITO + SQLITE
# ATUALIZADO EM FEVEREIRO/2026 - CORREÇÃO DE USER-AGENT E MÉTODOS DE REQUISIÇÃO

import requests
import json
import time
import os
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging

# ==================== CONFIGURAÇÃO PERFEITA ====================
class TimingPerfeito:
    """Configurações de timing otimizadas"""
    # Delays baseados em testes reais
    DELAY_MARCAS = 1.0
    DELAY_MODELOS = 1.0
    DELAY_ANOS = 1.0
    DELAY_PRECOS = 1.0
    
    # Controles de batch
    BATCH_PRECOS = 5
    PAUSA_BATCH = 2.0
    
    # Rate limit protection
    REQUESTS_PER_MINUTE = 30
    COOLDOWN_429 = 90
    TIMEOUT = 30

class ConfigFinal:
    """Configuração final otimizada"""
    BASE_URL = "https://veiculos.fipe.org.br/api/veiculos"
    REFERENCIA = 332  # abril 2026
    
    TIPOS_VEICULO = [
        {'id': 1, 'nome': 'carros'},
        {'id': 2, 'nome': 'caminhoes'},
        {'id': 3, 'nome': 'motos'}
    ]
    
    # Banco de dados SQLite
    DATABASE_FILE = "fipe_database_v3.db"
    ARQUIVO_LOG = "logs/fipe_scraper.log"
    
    # Modo teste
    MODO_TESTE = True
    MAX_MARCAS_TESTE = 3 if MODO_TESTE else None
    MAX_MODELOS_TESTE = 2 if MODO_TESTE else None

# ==================== GERENCIADOR SQLITE ====================
class GerenciadorSQLite:
    """Gerencia banco de dados SQLite para armazenamento eficiente"""
    
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.conn = None
        self.cursor = None
        self.dados_buffer = []
        
        # Criar diretório se não existir
        os.makedirs(os.path.dirname(db_file) or ".", exist_ok=True)
        
        # Conectar ao banco de dados
        self._conectar()
        
        # Criar tabelas se não existirem
        self._criar_tabelas()
    
    def _conectar(self):
        """Conecta ao banco de dados SQLite"""
        try:
            self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row  # Para acesso por nome de coluna
            self.cursor = self.conn.cursor()
        except Exception as e:
            raise Exception(f"Erro ao conectar ao banco de dados: {e}")
    
    def _criar_tabelas(self):
        """Cria todas as tabelas necessárias"""
        try:
            # Tabela de veículos (dados principais)
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS veiculos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tipo_veiculo INTEGER,
                    marca TEXT,
                    modelo TEXT,
                    ano TEXT,
                    valor TEXT,
                    combustivel TEXT,
                    referencia INTEGER,
                    codigo_fipe TEXT,
                    mes_referencia TEXT,
                    data_coleta TIMESTAMP,
                    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(tipo_veiculo, marca, modelo, ano, referencia)
                )
            ''')
            
            # Tabela de marcas
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS marcas (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tipo_veiculo INTEGER,
                    codigo_marca TEXT,
                    nome_marca TEXT,
                    data_coleta TIMESTAMP,
                    UNIQUE(tipo_veiculo, codigo_marca)
                )
            ''')
            
            # Tabela de modelos
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS modelos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tipo_veiculo INTEGER,
                    codigo_marca TEXT,
                    codigo_modelo TEXT,
                    nome_modelo TEXT,
                    data_coleta TIMESTAMP,
                    UNIQUE(tipo_veiculo, codigo_marca, codigo_modelo)
                )
            ''')
            
            # Tabela de anos
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS anos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tipo_veiculo INTEGER,
                    codigo_marca TEXT,
                    codigo_modelo TEXT,
                    codigo_ano TEXT,
                    nome_ano TEXT,
                    data_coleta TIMESTAMP,
                    UNIQUE(tipo_veiculo, codigo_marca, codigo_modelo, codigo_ano)
                )
            ''')
            
            # Tabela de estatísticas
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS estatisticas (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data_registro DATE,
                    total_veiculos INTEGER,
                    total_marcas INTEGER,
                    total_modelos INTEGER,
                    total_tipos INTEGER,
                    tempo_coleta_minutos INTEGER
                )
            ''')
            
            # Criar índices para performance
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_veiculos_tipo ON veiculos(tipo_veiculo)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_veiculos_marca ON veiculos(marca)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_veiculos_modelo ON veiculos(modelo)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_marcas_tipo ON marcas(tipo_veiculo)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_modelos_tipo_marca ON modelos(tipo_veiculo, codigo_marca)')
            
            self.conn.commit()
            
        except Exception as e:
            raise Exception(f"Erro ao criar tabelas: {e}")
    
    def adicionar_veiculo(self, dados: Dict):
        """Adiciona um veículo ao buffer para inserção em lote"""
        self.dados_buffer.append(dados)
    
    def salvar_buffer(self, forcar: bool = False) -> Dict:
        """
        Salva dados do buffer no banco de dados
        Retorna estatísticas da operação
        """
        if not self.dados_buffer and not forcar:
            return {'salvos': 0, 'total': self.contar_veiculos()}
        
        try:
            salvos = 0
            duplicados = 0
            
            for dados in self.dados_buffer:
                try:
                    # Inserir ou atualizar veículo
                    self.cursor.execute('''
                        INSERT OR REPLACE INTO veiculos 
                        (tipo_veiculo, marca, modelo, ano, valor, combustivel, referencia, 
                         codigo_fipe, mes_referencia, data_coleta)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        dados.get('tipo_veiculo'),
                        dados.get('marca'),
                        dados.get('modelo'),
                        dados.get('ano'),
                        dados.get('Valor', ''),
                        dados.get('Combustivel', ''),
                        dados.get('referencia'),
                        dados.get('CodigoFipe', ''),
                        dados.get('MesReferencia', ''),
                        dados.get('data_coleta')
                    ))
                    salvos += 1
                except sqlite3.IntegrityError:
                    duplicados += 1
                except Exception as e:
                    logging.debug(f"Erro ao salvar veículo: {e}")
            
            self.conn.commit()
            total = self.contar_veiculos()
            
            # Limpar buffer
            self.dados_buffer.clear()
            
            return {
                'sucesso': True,
                'salvos': salvos,
                'duplicados': duplicados,
                'total': total
            }
            
        except Exception as e:
            return {
                'sucesso': False,
                'erro': str(e)
            }
    
    def salvar_marca(self, tipo_veiculo: int, codigo_marca: str, nome_marca: str):
        """Salva uma marca no banco de dados"""
        try:
            self.cursor.execute('''
                INSERT OR REPLACE INTO marcas (tipo_veiculo, codigo_marca, nome_marca, data_coleta)
                VALUES (?, ?, ?, ?)
            ''', (tipo_veiculo, codigo_marca, nome_marca, datetime.now().isoformat()))
            self.conn.commit()
            return True
        except Exception as e:
            logging.debug(f"Erro ao salvar marca: {e}")
            return False
    
    def salvar_modelo(self, tipo_veiculo: int, codigo_marca: str, codigo_modelo: str, nome_modelo: str):
        """Salva um modelo no banco de dados"""
        try:
            self.cursor.execute('''
                INSERT OR REPLACE INTO modelos (tipo_veiculo, codigo_marca, codigo_modelo, nome_modelo, data_coleta)
                VALUES (?, ?, ?, ?, ?)
            ''', (tipo_veiculo, codigo_marca, codigo_modelo, nome_modelo, datetime.now().isoformat()))
            self.conn.commit()
            return True
        except Exception as e:
            logging.debug(f"Erro ao salvar modelo: {e}")
            return False
    
    def salvar_ano(self, tipo_veiculo: int, codigo_marca: str, codigo_modelo: str, codigo_ano: str, nome_ano: str):
        """Salva um ano no banco de dados"""
        try:
            self.cursor.execute('''
                INSERT OR REPLACE INTO anos (tipo_veiculo, codigo_marca, codigo_modelo, codigo_ano, nome_ano, data_coleta)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (tipo_veiculo, codigo_marca, codigo_modelo, codigo_ano, nome_ano, datetime.now().isoformat()))
            self.conn.commit()
            return True
        except Exception as e:
            logging.debug(f"Erro ao salvar ano: {e}")
            return False
    
    def contar_veiculos(self) -> int:
        """Retorna o total de veículos no banco de dados"""
        try:
            self.cursor.execute("SELECT COUNT(*) as total FROM veiculos")
            return self.cursor.fetchone()['total']
        except:
            return 0
    
    def contar_marcas(self, tipo_veiculo: int = None) -> int:
        """Retorna o total de marcas"""
        try:
            if tipo_veiculo:
                self.cursor.execute("SELECT COUNT(*) as total FROM marcas WHERE tipo_veiculo = ?", (tipo_veiculo,))
            else:
                self.cursor.execute("SELECT COUNT(*) as total FROM marcas")
            return self.cursor.fetchone()['total']
        except:
            return 0
            
    def estatisticas_banco(self) -> Dict:
        """Retorna estatísticas completas do banco"""
        try:
            self.cursor.execute("SELECT COUNT(*) as total FROM veiculos")
            total_veiculos = self.cursor.fetchone()['total']
            
            self.cursor.execute("SELECT COUNT(*) as total FROM marcas")
            total_marcas = self.cursor.fetchone()['total']
            
            self.cursor.execute("SELECT COUNT(*) as total FROM modelos")
            total_modelos = self.cursor.fetchone()['total']
            
            self.cursor.execute("SELECT COUNT(DISTINCT tipo_veiculo) as total FROM veiculos")
            total_tipos = self.cursor.fetchone()['total']
            
            self.cursor.execute("SELECT MAX(data_atualizacao) as ultima FROM veiculos")
            ultima_atualizacao = self.cursor.fetchone()['ultima']
            
            # Contagem por tipo
            por_tipo = {}
            tipo_map = {1: 'Carros', 2: 'Caminhões', 3: 'Motos'}
            for tid, tnome in tipo_map.items():
                self.cursor.execute("SELECT COUNT(*) as total FROM veiculos WHERE tipo_veiculo = ?", (tid,))
                por_tipo[tnome] = self.cursor.fetchone()['total']
                
            # Tamanho do arquivo
            tamanho_mb = os.path.getsize(self.db_file) / (1024 * 1024) if os.path.exists(self.db_file) else 0
            
            return {
                'total_veiculos': total_veiculos,
                'total_marcas': total_marcas,
                'total_modelos': total_modelos,
                'total_tipos': total_tipos,
                'ultima_atualizacao': ultima_atualizacao,
                'por_tipo': por_tipo,
                'tamanho_mb': tamanho_mb
            }
        except Exception as e:
            return {'erro': str(e)}

    def buscar_veiculos(self, filtros: Dict = None, limite: int = 100) -> List[Dict]:
        """Busca veículos com filtros"""
        query = "SELECT * FROM veiculos"
        params = []
        
        if filtros:
            condicoes = []
            for campo, valor in filtros.items():
                condicoes.append(f"{campo} LIKE ?")
                params.append(f"%{valor}%")
            query += " WHERE " + " AND ".join(condicoes)
            
        query += " ORDER BY data_coleta DESC LIMIT ?"
        params.append(limite)
        
        try:
            self.cursor.execute(query, params)
            return [dict(row) for row in self.cursor.fetchall()]
        except:
            return []

    def exportar_para_json(self, arquivo_saida: str):
        """Exporta todos os dados para JSON"""
        try:
            self.cursor.execute("SELECT * FROM veiculos")
            rows = self.cursor.fetchall()
            dados = [dict(row) for row in rows]
            
            os.makedirs(os.path.dirname(arquivo_saida) or ".", exist_ok=True)
            with open(arquivo_saida, 'w', encoding='utf-8') as f:
                json.dump(dados, f, indent=4, ensure_ascii=False)
            
            return {'sucesso': True, 'veiculos_exportados': len(dados)}
        except Exception as e:
            return {'sucesso': False, 'erro': str(e)}

    def fechar(self):
        """Fecha conexão com banco de dados"""
        if self.conn:
            self.conn.close()

# ==================== CACHE INTELIGENTE ====================
class CacheFipe:
    """Cache em memória para evitar requisições duplicadas"""
    def __init__(self):
        self.marcas = {}
        self.modelos = {}
        self.anos = {}
        self.precos = {}
        
    def get_marcas(self, tipo: int):
        return self.marcas.get(tipo)
        
    def set_marcas(self, tipo: int, dados: List):
        self.marcas[tipo] = dados
        
    def get_modelos(self, tipo: int, marca_id: str):
        key = f"{tipo}_{marca_id}"
        return self.modelos.get(key)
        
    def set_modelos(self, tipo: int, marca_id: str, dados: List):
        key = f"{tipo}_{marca_id}"
        self.modelos[key] = dados
        
    def get_anos(self, tipo: int, marca_id: str, modelo_id: str):
        key = f"{tipo}_{marca_id}_{modelo_id}"
        return self.anos.get(key)
        
    def set_anos(self, tipo: int, marca_id: str, modelo_id: str, dados: List):
        key = f"{tipo}_{marca_id}_{modelo_id}"
        self.anos[key] = dados
        
    def get_preco(self, tipo: int, marca_id: str, modelo_id: str, ano_id: str):
        key = f"{tipo}_{marca_id}_{modelo_id}_{ano_id}"
        return self.precos.get(key)
        
    def set_preco(self, tipo: int, marca_id: str, modelo_id: str, ano_id: str, dados: Dict):
        key = f"{tipo}_{marca_id}_{modelo_id}_{ano_id}"
        self.precos[key] = dados

# ==================== SCRAPER PRINCIPAL ====================
class FipeScraperFinal:
    """Scraper definitivo com timing perfeito e SQLite"""
    
    def __init__(self, config: ConfigFinal):
        self.config = config
        self.timing = TimingPerfeito()
        self.db = GerenciadorSQLite(config.DATABASE_FILE)
        self.cache = CacheFipe()
        
        self.session = requests.Session()
        # CORREÇÃO: User-Agent moderno e completo
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/json",
            "Origin": "https://veiculos.fipe.org.br",
            "Referer": "https://veiculos.fipe.org.br/"
        })
        
        self.stats = {
            'inicio': datetime.now(),
            'veiculos_coletados': 0,
            'requisicoes_total': 0,
            'rate_limits': 0,
            'sucesso': 0,
            'falhas': 0
        }
        
        self.ultima_requisicao = 0
        self.contador_batch = 0
        
        # Setup logging
        self._setup_logging()
        
        self.logger.info("=" * 60)
        self.logger.info("🚀 FIPE SCRAPER SQLITE - TIMING PERFEITO")
        self.logger.info("=" * 60)
        self.logger.info(f"✓ Banco de dados: {self.config.DATABASE_FILE}")
        self.logger.info(f"✓ Timing otimizado")
        self.logger.info(f"✓ Cache inteligente")
        self.logger.info("=" * 60)
    
    def _setup_logging(self):
        """Configura logging"""
        os.makedirs('logs', exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.config.ARQUIVO_LOG, encoding='utf-8', mode='a'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def _controle_timing(self, delay: float):
        """Controle de timing entre requisições"""
        agora = time.time()
        tempo_espera = max(0, delay - (agora - self.ultima_requisicao))
        
        if tempo_espera > 0:
            time.sleep(tempo_espera)
        
        self.ultima_requisicao = time.time()
    
    def _requisicao_segura(self, endpoint: str, dados: Dict = None) -> Optional[Dict]:
        """Requisição HTTP com timing e retry controlados"""
        url = f"{self.config.BASE_URL}/{endpoint}"
        
        for tentativa in range(3):
            try:
                # Controle de timing
                delay = getattr(self.timing, f"DELAY_{endpoint.split('Consultar')[-1].upper()}", 
                              self.timing.DELAY_PRECOS)
                self._controle_timing(delay)
                
                # Fazer requisição
                # CORREÇÃO: ConsultarTabelaDeReferencia agora exige POST
                if endpoint == 'ConsultarTabelaDeReferencia':
                    response = self.session.post(url, timeout=15)
                else:
                    response = self.session.post(url, json=dados, timeout=15)
                
                self.stats['requisicoes_total'] += 1
                
                if response.status_code == 429:
                    self.stats['rate_limits'] += 1
                    wait = self.timing.COOLDOWN_429 * (tentativa + 1)
                    self.logger.warning(f"⚠️  Rate limit! Aguardando {wait}s...")
                    time.sleep(wait)
                    continue
                
                if response.status_code != 200:
                    self.logger.error(f"Erro HTTP {response.status_code} em {endpoint}")
                    continue
                
                try:
                    return response.json()
                except:
                    texto = response.text.strip()
                    if texto.startswith('ss['):
                        texto = texto[2:]
                    try:
                        return json.loads(texto)
                    except:
                        return None
                
            except Exception as e:
                self.logger.debug(f"Tentativa {tentativa+1}: {e}")
                time.sleep(2)
        
        return None
    
    def _obter_marcas_otimizado(self, tipo: int) -> List[Dict]:
        """Obtém todas as marcas"""
        cache = self.cache.get_marcas(tipo)
        if cache:
            return cache
        
        dados = {
            "codigoTabelaReferencia": self.config.REFERENCIA,
            "codigoTipoVeiculo": tipo
        }
        
        marcas = self._requisicao_segura("ConsultarMarcas", dados)
        
        if marcas:
            self.cache.set_marcas(tipo, marcas)
            # Salvar marcas no banco de dados
            for marca in marcas:
                self.db.salvar_marca(tipo, marca['Value'], marca['Label'])
            return marcas
        
        return []
    
    def _obter_modelos_otimizado(self, tipo: int, marca: Dict) -> List[Dict]:
        """Obtém todos os modelos"""
        marca_id = marca['Value']
        
        cache = self.cache.get_modelos(tipo, marca_id)
        if cache:
            return cache
        
        dados = {
            "codigoTabelaReferencia": self.config.REFERENCIA,
            "codigoTipoVeiculo": tipo,
            "codigoMarca": marca_id
        }
        
        response = self._requisicao_segura("ConsultarModelos", dados)
        
        if response and 'Modelos' in response:
            modelos = response['Modelos']
            self.cache.set_modelos(tipo, marca_id, modelos)
            # Salvar modelos no banco de dados
            for modelo in modelos:
                self.db.salvar_modelo(tipo, marca_id, modelo['Value'], modelo['Label'])
            return modelos
        
        return []
    
    def _obter_anos_otimizado(self, tipo: int, marca: Dict, modelo: Dict) -> List[Dict]:
        """Obtém todos os anos"""
        marca_id = marca['Value']
        modelo_id = modelo['Value']
        
        cache = self.cache.get_anos(tipo, marca_id, modelo_id)
        if cache:
            return cache
        
        dados = {
            "codigoTabelaReferencia": self.config.REFERENCIA,
            "codigoTipoVeiculo": tipo,
            "codigoMarca": marca_id,
            "codigoModelo": modelo_id
        }
        
        anos = self._requisicao_segura("ConsultarAnoModelo", dados)
        
        if anos:
            self.cache.set_anos(tipo, marca_id, modelo_id, anos)
            # Salvar anos no banco de dados
            for ano in anos:
                self.db.salvar_ano(tipo, marca_id, modelo_id, ano['Value'], ano['Label'])
            return anos
        
        return []
    
    def _obter_preco_otimizado(self, tipo: int, marca: Dict, modelo: Dict, ano: Dict) -> Optional[Dict]:
        """Obtém preço com cache"""
        marca_id = marca['Value']
        modelo_id = modelo['Value']
        ano_id = ano['Value']
        
        cache = self.cache.get_preco(tipo, marca_id, modelo_id, ano_id)
        if cache:
            return cache
        
        # Parse ano
        if '-' in str(ano_id):
            try:
                ano_num, combustivel = str(ano_id).split('-')
            except:
                ano_num = str(ano_id)
                combustivel = "1"
        else:
            ano_num = str(ano_id)
            combustivel = "1"
        
        tipo_map = {1: "carro", 2: "caminhao", 3: "moto"}
        
        dados = {
            "codigoTabelaReferencia": self.config.REFERENCIA,
            "codigoTipoVeiculo": tipo,
            "codigoMarca": marca_id,
            "codigoModelo": modelo_id,
            "anoModelo": ano_num,
            "codigoTipoCombustivel": combustivel,
            "tipoVeiculo": tipo_map.get(tipo, "carro"),
            "modeloCodigoExterno": "",
            "tipoConsulta": "tradicional"
        }
        
        self.contador_batch += 1
        if self.contador_batch >= self.timing.BATCH_PRECOS:
            self._controle_timing(self.timing.PAUSA_BATCH)
            self.contador_batch = 0
        
        resultado = self._requisicao_segura("ConsultarValorComTodosParametros", dados)
        
        if resultado:
            resultado.update({
                'marca': marca.get('Label', ''),
                'modelo': modelo.get('Label', ''),
                'ano': ano.get('Label', ''),
                'tipo_veiculo': tipo,
                'referencia': self.config.REFERENCIA,
                'data_coleta': datetime.now().isoformat()
            })
            
            self.cache.set_preco(tipo, marca_id, modelo_id, ano_id, resultado)
            return resultado
        
        return None
    
    def _processar_marca_inteligente(self, tipo: int, marca: Dict, idx: int, total: int) -> int:
        """Processa uma marca de forma inteligente"""
        marca_nome = marca.get('Label', f'Marca_{idx}')
        
        modelos = self._obter_modelos_otimizado(tipo, marca)
        
        if not modelos:
            self.logger.debug(f"  {marca_nome}: sem modelos")
            return 0
        
        if self.config.MAX_MODELOS_TESTE:
            modelos = modelos[:self.config.MAX_MODELOS_TESTE]
        
        veiculos_coletados = 0
        total_modelos = len(modelos)
        
        for modelo_idx, modelo in enumerate(modelos, 1):
            modelo_nome = modelo.get('Label', f'Modelo_{modelo_idx}')
            
            anos = self._obter_anos_otimizado(tipo, marca, modelo)
            
            if not anos:
                continue
            
            for ano in anos:
                try:
                    preco = self._obter_preco_otimizado(tipo, marca, modelo, ano)
                    
                    if preco:
                        # Adicionar ao buffer do banco de dados
                        self.db.adicionar_veiculo(preco)
                        self.stats['sucesso'] += 1
                        self.stats['veiculos_coletados'] += 1
                        veiculos_coletados += 1
                        
                        # Salvar buffer a cada 50 veículos
                        if len(self.db.dados_buffer) >= 50:
                            resultado = self.db.salvar_buffer()
                            if resultado['sucesso']:
                                self.logger.info(f"    💾 Salvos {resultado['salvos']} veículos "
                                              f"(total: {resultado['total']})")
                        
                except Exception as e:
                    self.stats['falhas'] += 1
                    self.logger.debug(f"Erro no veículo: {e}")
                    
        return veiculos_coletados

    def executar(self):
        """Executa a coleta completa"""
        self.logger.info(f"Iniciando coleta para Referência: {self.config.REFERENCIA}")
        
        try:
            for tipo_info in self.config.TIPOS_VEICULO:
                tipo_id = tipo_info['id']
                tipo_nome = tipo_info['nome']
                
                self.logger.info(f"\n📁 TIPO: {tipo_nome.upper()}")
                
                marcas = self._obter_marcas_otimizado(tipo_id)
                
                if self.config.MAX_MARCAS_TESTE:
                    marcas = marcas[:self.config.MAX_MARCAS_TESTE]
                
                total_marcas = len(marcas)
                self.logger.info(f"Encontradas {total_marcas} marcas")
                
                for idx, marca in enumerate(marcas, 1):
                    marca_nome = marca.get('Label', f'Marca_{idx}')
                    self.logger.info(f"  [{idx}/{total_marcas}] Processando {marca_nome}...")
                    
                    self._processar_marca_inteligente(tipo_id, marca, idx, total_marcas)
            
            # Finalizar salvamento do buffer
            resultado = self.db.salvar_buffer(forcar=True)
            self.logger.info(f"\n✅ Coleta finalizada! Total de veículos no banco: {resultado['total']}")
            
        except KeyboardInterrupt:
            self.logger.warning("\n⚠️  Coleta interrompida pelo usuário. Salvando dados...")
            self.db.salvar_buffer(forcar=True)
        except Exception as e:
            self.logger.error(f"\n❌ Erro fatal: {e}")
            self.db.salvar_buffer(forcar=True)
        finally:
            self._mostrar_resumo()
            self.db.fechar()

    def _mostrar_resumo(self):
        """Mostra resumo da execução"""
        tempo = datetime.now() - self.stats['inicio']
        minutos = tempo.total_seconds() / 60
        
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📊 RESUMO DA EXECUÇÃO")
        self.logger.info("-" * 60)
        self.logger.info(f"⏱️  Tempo total: {minutos:.1f} minutos")
        self.logger.info(f"🚗 Veículos coletados: {self.stats['veiculos_coletados']}")
        self.logger.info(f"🌐 Requisições total: {self.stats['requisicoes_total']}")
        self.logger.info(f"⚠️  Rate limits (429): {self.stats['rate_limits']}")
        self.logger.info(f"✅ Sucesso: {self.stats['sucesso']}")
        self.logger.info(f"❌ Falhas: {self.stats['falhas']}")
        self.logger.info("=" * 60)

# ==================== MENU E INTERFACE ====================
def menu_principal():
    """Menu interativo"""
    config = ConfigFinal()
    
    while True:
        os.system('cls' if os.name == 'nt' else 'clear')
        print("=" * 60)
        print("🚀 FIPE SCRAPER SQLITE - MENU PRINCIPAL")
        print("=" * 60)
        print(f"Referência atual: {config.REFERENCIA} (Fevereiro 2026)")
        print(f"Banco de dados: {config.DATABASE_FILE}")
        print("-" * 60)
        print("1. Iniciar Coleta COMPLETA (Produção)")
        print("2. Iniciar Coleta de TESTE (Rápida)")
        print("3. Ver Estatísticas do Banco de Dados")
        print("4. Exportar Dados para JSON")
        print("5. Buscar Veículo no Banco")
        print("0. Sair")
        print("-" * 60)
        
        opcao = input("Escolha uma opção: ").strip()
        
        if opcao == '1':
            config.MODO_TESTE = False
            config.MAX_MARCAS_TESTE = None
            config.MAX_MODELOS_TESTE = None
            scraper = FipeScraperFinal(config)
            scraper.executar()
            input("\nPressione Enter para voltar ao menu...")
            
        elif opcao == '2':
            config.MODO_TESTE = True
            config.MAX_MARCAS_TESTE = 3
            config.MAX_MODELOS_TESTE = 2
            scraper = FipeScraperFinal(config)
            scraper.executar()
            input("\nPressione Enter para voltar ao menu...")
            
        elif opcao == '3':
            ver_estatisticas()
            input("\nPressione Enter para voltar ao menu...")
            
        elif opcao == '4':
            exportar_para_json()
            input("\nPressione Enter para voltar ao menu...")
            
        elif opcao == '5':
            buscar_veiculos()
            input("\nPressione Enter para voltar ao menu...")
            
        elif opcao == '0':
            print("\nSaindo... Até logo!")
            break
        else:
            print("\nOpção inválida!")
            time.sleep(1)

def ver_estatisticas():
    """Mostra estatísticas do banco de dados"""
    db_file = "fipe_database_v3.db"
    
    if not os.path.exists(db_file):
        print(f"\n❌ Banco de dados não encontrado: {db_file}")
        return
    
    try:
        db = GerenciadorSQLite(db_file)
        stats = db.estatisticas_banco()
        db.fechar()
        
        if 'erro' in stats:
            print(f"\n❌ Erro ao acessar banco de dados: {stats['erro']}")
            return
        
        print(f"\n📊 DADOS NO BANCO DE DADOS:")
        print(f"   • Veículos: {stats['total_veiculos']:,}")
        print(f"   • Marcas: {stats['total_marcas']}")
        print(f"   • Modelos: {stats['total_modelos']}")
        print(f"   • Tipos de veículo: {stats['total_tipos']}")
        
        if 'por_tipo' in stats:
            for tipo, quantidade in stats['por_tipo'].items():
                print(f"   • {tipo}: {quantidade:,}")
        
        print(f"   • Tamanho do banco: {stats['tamanho_mb']:.2f} MB")
        print(f"   • Última atualização: {stats['ultima_atualizacao']}")
        
        # Últimos 5 veículos
        print(f"\n📝 ÚLTIMOS 5 VEÍCULOS COLETADOS:")
        db = GerenciadorSQLite(db_file)
        veiculos = db.buscar_veiculos(limite=5)
        db.fechar()
        
        for veiculo in veiculos:
            print(f"   • {veiculo['marca']} {veiculo['modelo']} {veiculo['ano']} - {veiculo['valor']}")
        
        print("\n" + "="*60)
        
    except Exception as e:
        print(f"❌ Erro: {e}")

def exportar_para_json():
    """Exporta dados do SQLite para JSON"""
    db_file = "fipe_database_v3.db"
    json_file = "dados_fipe/fipe_exportado.json"
    
    if not os.path.exists(db_file):
        print(f"\n❌ Banco de dados não encontrado: {db_file}")
        return
    
    try:
        db = GerenciadorSQLite(db_file)
        resultado = db.exportar_para_json(json_file)
        db.fechar()
        
        if resultado['sucesso']:
            print(f"\n✅ Exportação concluída!")
            print(f"   • Arquivo: {json_file}")
            print(f"   • Veículos exportados: {resultado['veiculos_exportados']:,}")
            
            tamanho_bytes = os.path.getsize(json_file) if os.path.exists(json_file) else 0
            tamanho_mb = tamanho_bytes / (1024 * 1024)
            print(f"   • Tamanho do JSON: {tamanho_mb:.2f} MB")
        else:
            print(f"\n❌ Erro na exportação: {resultado['erro']}")
            
        print("\n" + "="*60)
        
    except Exception as e:
        print(f"❌ Erro: {e}")

def buscar_veiculos():
    """Busca veículos no banco de dados"""
    db_file = "fipe_database_v3.db"
    
    if not os.path.exists(db_file):
        print(f"\n❌ Banco de dados não encontrado: {db_file}")
        return
    
    try:
        print("\n🔍 BUSCAR VEÍCULOS")
        
        marca = input("Marca (opcional, pressione Enter para pular): ").strip()
        modelo = input("Modelo (opcional): ").strip()
        tipo_input = input("Tipo (1=Carros, 2=Caminhões, 3=Motos, Enter=todos): ").strip()
        
        filtros = {}
        if marca:
            filtros['marca'] = marca
        if modelo:
            filtros['modelo'] = modelo
        if tipo_input and tipo_input in ['1', '2', '3']:
            filtros['tipo_veiculo'] = int(tipo_input)
        
        limite_input = input("Quantos resultados (padrão: 20): ").strip()
        limite = int(limite_input) if limite_input.isdigit() else 20
        
        db = GerenciadorSQLite(db_file)
        veiculos = db.buscar_veiculos(filtros, limite)
        db.fechar()
        
        print(f"\n🔎 RESULTADOS DA BUSCA ({len(veiculos)} veículos):")
        print("-" * 60)
        
        for i, veiculo in enumerate(veiculos, 1):
            tipo_map = {1: 'Carro', 2: 'Caminhão', 3: 'Moto'}
            tipo = tipo_map.get(veiculo['tipo_veiculo'], 'Desconhecido')
            
            print(f"{i}. {veiculo['marca']} {veiculo['modelo']} {veiculo['ano']}")
            print(f"   Tipo: {tipo} | Valor: {veiculo['valor']}")
            print(f"   Combustível: {veiculo['combustivel']} | Código FIPE: {veiculo['codigo_fipe']}")
            print(f"   Coletado em: {veiculo['data_coleta'][:19]}")
            print()
        
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Erro na busca: {e}")

# ==================== EXECUÇÃO ====================
if __name__ == "__main__":
    menu_principal()
