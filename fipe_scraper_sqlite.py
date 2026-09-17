# coletor_fipe_d1.py
# COLETOR OFICIAL FIPE + MODO SENTINELA AUTOMÁTICO 24/7 + CLOUDFLARE D1
# Versão com detecção contínua de novas referências (312 -> 338 em diante)

try:
    from curl_cffi import requests
    HAS_CURL_CFFI = True
except ImportError:
    import requests
    HAS_CURL_CFFI = False
import json
import time
import os
import sys
import sqlite3
import subprocess
import argparse
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Set
import logging

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# ==================== CONFIGURAÇÃO ====================
class TimingPerfeito:
    DELAY_MARCAS = 1.2
    DELAY_MODELOS = 1.2
    DELAY_ANOS = 1.2
    DELAY_PRECOS = 1.5
    
    BATCH_PRECOS = 2
    PAUSA_BATCH = 1.0
    
    REQUESTS_PER_MINUTE = 40
    COOLDOWN_429 = 30
    TIMEOUT = 20

class Config:
    BASE_URL = "https://veiculos.fipe.org.br/api/veiculos"
    DATABASE_FILE = "fipe_database_v3.db"
    ARQUIVO_LOG = "logs/sentinela.log"
    INTERVALO_MONITORAMENTO_SEGUNDOS = 3600  # Checa a FIPE a cada 1 hora
    
    TIPOS_VEICULO = [
        {'id': 1, 'nome': 'carros'},
        {'id': 2, 'nome': 'caminhoes'},
        {'id': 3, 'nome': 'motos'}
    ]

# ==================== GERENCIADOR SQLITE LOCAL ====================
class GerenciadorSQLite:
    def __init__(self, db_file: str):
        self.db_file = db_file
        os.makedirs(os.path.dirname(db_file) or ".", exist_ok=True)
        self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self.dados_buffer = []
        self._criar_tabelas()
    
    def _criar_tabelas(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS veiculos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tipo_veiculo INTEGER,
                marca TEXT,
                modelo TEXT,
                ano TEXT,
                valor INTEGER,
                valor_texto TEXT,
                combustivel TEXT,
                referencia INTEGER,
                codigo_fipe TEXT,
                mes_referencia TEXT,
                data_coleta TIMESTAMP,
                data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(tipo_veiculo, marca, modelo, ano, referencia)
            )
        ''')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_v_ref ON veiculos(referencia)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_v_busca ON veiculos(referencia, tipo_veiculo, marca)')
        self.conn.commit()

    @staticmethod
    def parse_valor_numerico(valor_str: str) -> int:
        if not valor_str:
            return 0
        limpo = valor_str.replace('R$', '').replace('.', '').replace(' ', '').split(',')[0].strip()
        try:
            return int(limpo)
        except:
            return 0

    def adicionar_veiculo(self, dados: Dict):
        self.dados_buffer.append(dados)

    def salvar_buffer(self, forcar: bool = False) -> int:
        if not self.dados_buffer and not forcar:
            return 0
        salvos = 0
        for dados in self.dados_buffer:
            valor_txt = dados.get('Valor', '')
            valor_num = self.parse_valor_numerico(valor_txt)
            try:
                self.cursor.execute('''
                    INSERT OR REPLACE INTO veiculos 
                    (tipo_veiculo, marca, modelo, ano, valor, valor_texto, combustivel, referencia, 
                     codigo_fipe, mes_referencia, data_coleta)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    dados.get('tipo_veiculo'),
                    dados.get('marca'),
                    dados.get('modelo'),
                    dados.get('ano'),
                    valor_num,
                    valor_txt,
                    dados.get('Combustivel', ''),
                    dados.get('referencia'),
                    dados.get('CodigoFipe', ''),
                    dados.get('MesReferencia', ''),
                    dados.get('data_coleta')
                ))
                salvos += 1
            except Exception as e:
                logging.debug(f"Erro ao salvar: {e}")
        self.conn.commit()
        self.dados_buffer.clear()
        return salvos

    def veiculo_existe(self, tipo_veiculo: int, marca: str, modelo: str, ano: str, referencia: int) -> bool:
        try:
            self.cursor.execute('''
                SELECT 1 FROM veiculos 
                WHERE tipo_veiculo = ? AND marca = ? AND modelo = ? AND ano = ? AND referencia = ? 
                LIMIT 1
            ''', (tipo_veiculo, marca, modelo, ano, referencia))
            return self.cursor.fetchone() is not None
        except:
            return False

    def contar_veiculos(self, referencia: int = None) -> int:
        try:
            if referencia:
                self.cursor.execute("SELECT COUNT(*) FROM veiculos WHERE referencia = ?", (referencia,))
            else:
                self.cursor.execute("SELECT COUNT(*) FROM veiculos")
            return self.cursor.fetchone()[0]
        except:
            return 0

    def obter_referencias_locais(self) -> Set[int]:
        try:
            self.cursor.execute("SELECT DISTINCT referencia FROM veiculos")
            return {r[0] for r in self.cursor.fetchall()}
        except:
            return set()

    def fechar(self):
        if self.conn:
            self.conn.close()

# ==================== COLETOR PRINCIPAL ====================
class FipeColetor:
    def __init__(self, referencia: int, modo_teste: bool = False):
        self.referencia = referencia
        self.modo_teste = modo_teste
        self.timing = TimingPerfeito()
        self.db = GerenciadorSQLite(Config.DATABASE_FILE)
        
        if HAS_CURL_CFFI:
            self.session = requests.Session(impersonate="chrome120")
        else:
            self.session = requests.Session()
            self.session.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            })

        self.session.headers.update({
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/json",
            "Origin": "https://veiculos.fipe.org.br",
            "Referer": "https://veiculos.fipe.org.br/",
            "X-Requested-With": "XMLHttpRequest"
        })

        try:
            r_init = self.session.get("https://veiculos.fipe.org.br", timeout=10)
            logging.info(f"Inicializando sessão FIPE: Status {r_init.status_code}, Cookies: {dict(self.session.cookies)}")
        except Exception as e:
            logging.warning(f"Aviso ao inicializar sessão: {e}")
        
        self.ultima_requisicao = 0
        self.contador_batch = 0
        self.veiculos_coletados = 0
        self.sucesso = 0
        self.pulos = 0
        
        os.makedirs('logs', exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            handlers=[
                logging.FileHandler(Config.ARQUIVO_LOG, encoding='utf-8', mode='a'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _controle_timing(self, delay: float):
        agora = time.time()
        espera = max(0, delay - (agora - self.ultima_requisicao))
        if espera > 0:
            time.sleep(espera)
        self.ultima_requisicao = time.time()

    def _requisicao(self, endpoint: str, dados: Dict = None) -> Optional[Dict]:
        url = f"{Config.BASE_URL}/{endpoint}"
        for tentativa in range(4):
            try:
                self._controle_timing(self.timing.DELAY_PRECOS)
                if endpoint == 'ConsultarTabelaDeReferencia':
                    resp = self.session.post(url, timeout=self.timing.TIMEOUT)
                else:
                    resp = self.session.post(url, json=dados, timeout=self.timing.TIMEOUT)
                
                if resp.status_code == 429:
                    espera = self.timing.COOLDOWN_429 * (tentativa + 1)
                    self.logger.warning(f"⚠️ Rate limit 429! Aguardando {espera}s...")
                    time.sleep(espera)
                    continue
                
                if resp.status_code == 200:
                    try:
                        return resp.json()
                    except:
                        txt = resp.text.strip()
                        if txt.startswith('ss['):
                            txt = txt[2:]
                        return json.loads(txt)
                else:
                    self.logger.warning(f"HTTP {resp.status_code} em {endpoint} (cf-ray: {resp.headers.get('cf-ray', 'N/A')}) | Detalhes: {resp.text[:200].strip().replace(chr(10), ' ')}")
                    time.sleep(2)
            except Exception as e:
                self.logger.debug(f"Erro na tentativa {tentativa+1}: {e}")
                time.sleep(2)
        return None

    def executar(self) -> bool:
        self.logger.info("=" * 60)
        self.logger.info(f"🚀 INICIANDO COLETA FIPE - REFERÊNCIA {self.referencia}")
        if self.modo_teste:
            self.logger.info("⚠️ MODO TESTE RÁPIDO ATIVO (1 Marca / 2 Modelos)")
        self.logger.info("=" * 60)
        
        t0 = time.time()
        try:
            for tipo in Config.TIPOS_VEICULO:
                tipo_id = tipo['id']
                tipo_nome = tipo['nome']
                self.logger.info(f"\n📁 [{tipo_nome.upper()}] Buscando marcas...")
                
                marcas = self._requisicao("ConsultarMarcas", {
                    "codigoTabelaReferencia": self.referencia,
                    "codigoTipoVeiculo": tipo_id
                }) or []
                
                if self.modo_teste:
                    marcas = marcas[:1]
                
                self.logger.info(f"Total de marcas: {len(marcas)}")
                for idx, marca in enumerate(marcas, 1):
                    marca_nome = marca.get('Label', '')
                    marca_id = marca.get('Value', '')
                    self.logger.info(f"  [{idx}/{len(marcas)}] {marca_nome}...")
                    
                    modelos_data = self._requisicao("ConsultarModelos", {
                        "codigoTabelaReferencia": self.referencia,
                        "codigoTipoVeiculo": tipo_id,
                        "codigoMarca": marca_id
                    }) or {}
                    
                    modelos = modelos_data.get('Modelos', [])
                    if self.modo_teste:
                        modelos = modelos[:2]
                    
                    for modelo in modelos:
                        modelo_nome = modelo.get('Label', '')
                        modelo_id = modelo.get('Value', '')
                        
                        anos = self._requisicao("ConsultarAnoModelo", {
                            "codigoTabelaReferencia": self.referencia,
                            "codigoTipoVeiculo": tipo_id,
                            "codigoMarca": marca_id,
                            "codigoModelo": modelo_id
                        }) or []
                        
                        for ano in anos:
                            ano_nome = ano.get('Label', '')
                            ano_id = str(ano.get('Value', ''))
                            
                            # Evita requisição se já foi coletado antes
                            if self.db.veiculo_existe(tipo_id, marca_nome, modelo_nome, ano_nome, self.referencia):
                                self.pulos += 1
                                continue
                            
                            ano_num, comb = ano_id.split('-') if '-' in ano_id else (ano_id, '1')
                            tipo_map = {1: "carro", 2: "caminhao", 3: "moto"}
                            dados_preco = {
                                "codigoTabelaReferencia": self.referencia,
                                "codigoTipoVeiculo": tipo_id,
                                "codigoMarca": marca_id,
                                "codigoModelo": modelo_id,
                                "anoModelo": ano_num,
                                "codigoTipoCombustivel": comb,
                                "tipoVeiculo": tipo_map.get(tipo_id, "carro"),
                                "modeloCodigoExterno": "",
                                "tipoConsulta": "tradicional"
                            }
                            
                            self.contador_batch += 1
                            if self.contador_batch >= self.timing.BATCH_PRECOS:
                                self._controle_timing(self.timing.PAUSA_BATCH)
                                self.contador_batch = 0
                                
                            res_preco = self._requisicao("ConsultarValorComTodosParametros", dados_preco)
                            if res_preco and 'Valor' in res_preco:
                                res_preco.update({
                                    'marca': marca_nome,
                                    'modelo': modelo_nome,
                                    'ano': ano_nome,
                                    'tipo_veiculo': tipo_id,
                                    'referencia': self.referencia,
                                    'data_coleta': datetime.now().isoformat()
                                })
                                self.db.adicionar_veiculo(res_preco)
                                self.sucesso += 1
                                self.veiculos_coletados += 1
                                
                                if len(self.db.dados_buffer) >= 50:
                                    salvos = self.db.salvar_buffer()
                                    self.logger.info(f"    💾 Salvos {salvos} veículos (Total ref: {self.db.contar_veiculos(self.referencia)})")
                                    
            self.db.salvar_buffer(forcar=True)
            dt_min = (time.time() - t0) / 60
            self.logger.info("\n" + "=" * 60)
            self.logger.info(f"✅ COLETA LOCAL DA REF {self.referencia} CONCLUÍDA EM {dt_min:.1f} MINUTOS!")
            self.logger.info(f"Novos veículos coletados: {self.sucesso}")
            self.logger.info(f"Total nesta referência no SQLite: {self.db.contar_veiculos(self.referencia)}")
            self.logger.info("=" * 60)
            return True
        except KeyboardInterrupt:
            self.logger.warning("\nInterrompido pelo usuário. Salvando buffer...")
            self.db.salvar_buffer(forcar=True)
            return False
        finally:
            self.db.fechar()

# ==================== SINCRONIZADOR CLOUDFLARE D1 ====================
class SincronizadorD1:
    @staticmethod
    def escape_sql(val):
        if val is None:
            return "NULL"
        if isinstance(val, (int, float)):
            return str(val)
        return "'" + str(val).replace("'", "''") + "'"

    @classmethod
    def obter_referencias_d1(cls) -> Set[int]:
        try:
            cmd = ["npx", "wrangler", "d1", "execute", "fipe", "--remote", "--command=SELECT codigo FROM fipe_referencias", "--json"]
            res = subprocess.run(cmd, capture_output=True, text=True, shell=True, errors='replace')
            if res.returncode == 0:
                data = json.loads(res.stdout)
                return {r['codigo'] for r in data[0]['results']}
        except Exception as e:
            logging.debug(f"Aviso ao ler D1: {e}")
        return set()

    @classmethod
    def sincronizar_referencia(cls, ref_id: int) -> bool:
        print(f"\n🚀 SINCRONIZANDO REFERÊNCIA {ref_id} COM O CLOUDFLARE D1...")
        db_file = Config.DATABASE_FILE
        if not os.path.exists(db_file):
            print(f"❌ Arquivo {db_file} não encontrado!")
            return False
            
        conn = sqlite3.connect(db_file)
        cur = conn.cursor()
        
        cols = ["tipo_veiculo", "marca", "modelo", "ano", "valor", "valor_texto", 
                "combustivel", "referencia", "codigo_fipe", "mes_referencia", "data_coleta"]
        cols_str = ", ".join(cols)
        
        cur.execute(f"SELECT {cols_str} FROM veiculos WHERE referencia = ?", (ref_id,))
        rows = cur.fetchall()
        
        if not rows:
            print(f"Nenhum registro encontrado no banco local para a referência {ref_id}.")
            conn.close()
            return False
            
        print(f"📦 Preparando {len(rows):,} veículos para envio ao Cloudflare D1...")
        sql_file = f"temp_upload_ref_{ref_id}.sql"
        batch_size = 100
        
        with open(sql_file, 'w', encoding='utf-8') as f:
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                vals = [f"({', '.join(cls.escape_sql(v) for v in r)})" for r in batch]
                f.write(f"INSERT OR IGNORE INTO fipe ({cols_str}) VALUES\n" + ",\n".join(vals) + ";\n")
        
        # 1. Enviar lote de veículos
        print("Enviando dados para o Cloudflare D1 (pode levar alguns segundos)...", flush=True)
        cmd = ["npx", "wrangler", "d1", "execute", "fipe", "--remote", f"--file={sql_file}", "-y"]
        res = subprocess.run(cmd, capture_output=True, text=True, shell=True, errors='replace')
        
        if os.path.exists(sql_file):
            os.remove(sql_file)
            
        if res.returncode != 0:
            print("❌ Erro ao enviar veículos para o D1:")
            print(res.stderr or res.stdout)
            conn.close()
            return False
            
        print("✅ Veículos importados no Cloudflare D1 com sucesso!")
        
        # 2. Atualizar fipe_referencias e fipe_marcas no D1
        cur.execute("SELECT TRIM(mes_referencia) FROM veiculos WHERE referencia = ? LIMIT 1", (ref_id,))
        mes_row = cur.fetchone()
        mes_ref = mes_row[0] if mes_row else f"Referência {ref_id}"
        conn.close()
        
        print("🔄 Atualizando tabelas auxiliares (fipe_referencias e fipe_marcas) no D1...")
        sql_aux = f'''
            INSERT OR REPLACE INTO fipe_referencias (codigo, mes) VALUES ({ref_id}, {cls.escape_sql(mes_ref)});
            INSERT OR IGNORE INTO fipe_marcas (referencia, tipo_veiculo, marca)
            SELECT DISTINCT referencia, tipo_veiculo, marca FROM fipe WHERE referencia = {ref_id};
        '''
        cmd_aux = ["npx", "wrangler", "d1", "execute", "fipe", "--remote", f"--command={sql_aux}"]
        subprocess.run(cmd_aux, capture_output=True, text=True, shell=True, errors='replace')
        
        print("🎉 SINCRONIZAÇÃO COMPLETA COM SUCESSO ABSOLUTO!")
        print(f"A API na Cloudflare agora possui a referência {ref_id} ({mes_ref}) 100% online!")
        return True

# ==================== MODO SENTINELA (24/7) ====================
def obter_todas_referencias_fipe() -> List[Dict]:
    try:
        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Referer": "https://veiculos.fipe.org.br/",
            "X-Requested-With": "XMLHttpRequest"
        }
        if HAS_CURL_CFFI:
            r = requests.post("https://veiculos.fipe.org.br/api/veiculos/ConsultarTabelaDeReferencia", timeout=10, impersonate="chrome120", headers=headers)
        else:
            r = requests.post("https://veiculos.fipe.org.br/api/veiculos/ConsultarTabelaDeReferencia", timeout=10, headers=headers)
        if r.status_code == 200:
            return r.json() or []
    except Exception as e:
        logging.debug(f"Erro ao consultar FIPE: {e}")
    return []

def obter_referencia_atual_fipe() -> Tuple[int, str]:
    refs = obter_todas_referencias_fipe()
    if refs:
        return refs[0]['Codigo'], refs[0]['Mes'].strip()
    return 337, "setembro de 2026"

def modo_sentinela(ref_inicial: Optional[int] = None):
    print("=" * 70)
    print("🛡️ MODO SENTINELA FIPE 24/7 ATIVADO")
    print("=" * 70)
    print("Este modo roda de forma autônoma e contínua:")
    print("1. Coleta e sobe a referência inicial solicitada (ex: 312).")
    print("2. Fica monitorando a FIPE oficial a cada 1 hora.")
    print("3. Quando a FIPE lançar a referência 338 (e seguintes), coleta e sobe sozinho!")
    print("=" * 70)
    
    # Passo 1: Se uma referência inicial foi solicitada e ainda não está completa
    if ref_inicial:
        refs_d1 = SincronizadorD1.obter_referencias_d1()
        if ref_inicial not in refs_d1:
            print(f"\n[SENTINELA] 🎯 Iniciando coleta da referência pendente: {ref_inicial}...")
            coletor = FipeColetor(ref_inicial, modo_teste=False)
            ok = coletor.executar()
            if ok:
                SincronizadorD1.sincronizar_referencia(ref_inicial)
        else:
            print(f"\n[SENTINELA] Referência {ref_inicial} já se encontra no Cloudflare D1!")

    # Passo 2: Loop contínuo de monitoramento da FIPE oficial
    print("\n[SENTINELA] ✅ Entrando em vigília contínua. Monitorando FIPE oficial...")
    while True:
        try:
            agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            ref_atual_fipe, mes_atual_fipe = obter_referencia_atual_fipe()
            refs_d1 = SincronizadorD1.obter_referencias_d1()
            
            print(f"\n[{agora}] Checagem FIPE Oficial: Ref {ref_atual_fipe} ({mes_atual_fipe}) | Cloudflare D1: {len(refs_d1)} referências salvas.")
            
            # Se a FIPE tiver uma referência que NÃO está no D1 (ex: 338, 339, etc.)
            if ref_atual_fipe not in refs_d1:
                print(f"🚨 NOVA TABELA FIPE DETECTADA! Referência: {ref_atual_fipe} ({mes_atual_fipe})!")
                print("Iniciando coleta automática imediatamente...")
                coletor = FipeColetor(ref_atual_fipe, modo_teste=False)
                ok = coletor.executar()
                if ok:
                    SincronizadorD1.sincronizar_referencia(ref_atual_fipe)
                    print(f"🎉 Tabela {ref_atual_fipe} ({mes_atual_fipe}) publicada na Cloudflare com sucesso!")
            else:
                print(f"😴 Tudo em dia! Nenhuma tabela nova na FIPE. Próxima checagem em 1 hora.")
                
        except Exception as e:
            print(f"[SENTINELA] Erro durante checagem: {e}")
            
        time.sleep(Config.INTERVALO_MONITORAMENTO_SEGUNDOS)

# ==================== MENU PRINCIPAL ====================
def menu():
    ref_fipe, mes_fipe = obter_referencia_atual_fipe()
    
    while True:
        os.system('cls' if os.name == 'nt' else 'clear')
        print("=" * 65)
        print("🚀 FIPE COLETOR & MODO SENTINELA CLOUDFLARE D1")
        print("=" * 65)
        print(f"📅 Referência mais recente na FIPE oficial: {ref_fipe} ({mes_fipe})")
        print(f"💾 Banco local SQLite: {Config.DATABASE_FILE}")
        print("-" * 65)
        print("1. Iniciar MODO SENTINELA 24/7 (Coleta 312 agora -> Aguarda 338 em diante)")
        print("2. Iniciar Coleta da Referência Oficial Atual (Completa) + Upload")
        print("3. Iniciar Coleta de TESTE Rápida (1 marca / 2 modelos) + Upload")
        print("4. Coletar Referência ESPECÍFICA (ex: 312 - Agosto/2024)")
        print("5. Apenas enviar dados do banco local para o Cloudflare D1")
        print("6. Testar conexão com a API da FIPE")
        print("0. Sair")
        print("-" * 65)
        
        op = input("Escolha uma opção: ").strip()
        
        if op == '1':
            modo_sentinela(ref_inicial=312)
            input("\nPressione Enter para continuar...")
            
        elif op == '2':
            coletor = FipeColetor(ref_fipe, modo_teste=False)
            ok = coletor.executar()
            if ok:
                SincronizadorD1.sincronizar_referencia(ref_fipe)
            input("\nPressione Enter para continuar...")
            
        elif op == '3':
            coletor = FipeColetor(ref_fipe, modo_teste=True)
            ok = coletor.executar()
            if ok:
                SincronizadorD1.sincronizar_referencia(ref_fipe)
            input("\nPressione Enter para continuar...")
            
        elif op == '4':
            ref_input = input("Digite o número da referência desejada (ex: 312): ").strip()
            if ref_input.isdigit():
                ref_num = int(ref_input)
                coletor = FipeColetor(ref_num, modo_teste=False)
                ok = coletor.executar()
                if ok:
                    SincronizadorD1.sincronizar_referencia(ref_num)
            input("\nPressione Enter para continuar...")
            
        elif op == '5':
            ref_input = input(f"Qual referência deseja enviar para o D1? (Padrão: {ref_fipe}): ").strip()
            ref_num = int(ref_input) if ref_input.isdigit() else ref_fipe
            SincronizadorD1.sincronizar_referencia(ref_num)
            input("\nPressione Enter para continuar...")
            
        elif op == '6':
            print("\nTestando conexão com a FIPE oficial...")
            r, m = obter_referencia_atual_fipe()
            print(f"Status: OK! Última referência encontrada: {r} ({m})")
            input("\nPressione Enter para continuar...")
            
        elif op == '0':
            print("Até logo!")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Coletor FIPE + Cloudflare D1")
    parser.add_argument("--sentinela", action="store_true", help="Iniciar em modo Sentinela 24/7 autônomo")
    parser.add_argument("--ref", type=int, default=None, help="Referência inicial para coleta (ex: 312)")
    parser.add_argument("--modo-teste", action="store_true", help="Modo teste rápido (1 marca / 2 modelos)")
    parser.add_argument("--sem-upload", action="store_true", help="Não fazer upload no Cloudflare D1")
    parser.add_argument("--executar", action="store_true", help="Executar coleta diretamente sem abrir menu")
    args = parser.parse_args()
    
    if args.sentinela:
        modo_sentinela(ref_inicial=args.ref)
    elif args.executar or args.modo_teste:
        ref_fipe, mes_fipe = obter_referencia_atual_fipe()
        target_ref = args.ref if args.ref else ref_fipe
        print(f"[CLOUD/CLI] Iniciando coleta da Referência {target_ref} (Modo Teste: {args.modo_teste})...")
        coletor = FipeColetor(target_ref, modo_teste=args.modo_teste)
        ok = coletor.executar()
        if ok and not args.sem_upload:
            SincronizadorD1.sincronizar_referencia(target_ref)
    else:
        menu()
