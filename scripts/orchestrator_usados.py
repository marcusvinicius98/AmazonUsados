import os
import re
import logging
import asyncio
import json
# import time # Removido, pois asyncio.get_event_loop().time() é usado para qid
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException, TimeoutException,
    StaleElementReferenceException, InvalidSelectorException
)
from webdriver_manager.chrome import ChromeDriverManager
from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(name)s:%(funcName)s:%(lineno)d] - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("ORCHESTRATOR_USADOS")
# logger.propagate = False # Removido para permitir que o logger raiz capture logs se necessário, ou ajuste conforme sua preferência
# logger.setLevel(logging.INFO) # Definido pelo basicConfig

# Configurações
SELETOR_ITEM_PRODUTO_USADO = "div.s-result-item.s-asin"
SELETOR_NOME_PRODUTO_USADO = "span.a-size-base-plus.a-color-base.a-text-normal"
SELETOR_LINK_PRODUTO_USADO = "a.a-link-normal.s-underline-text.s-underline-link-text.s-link-style.a-text-normal"
SELETOR_PRECO_USADO = "span.a-price-whole"
SELETOR_FRACAO_PRECO = "span.a-price-fraction"
SELETOR_INDICADOR_USADO = "span.a-size-base.a-color-secondary"
SELETOR_PROXIMA_PAGINA = "a.s-pagination-next"

# Categorias extraídas do HTML
CATEGORIES = [
    {"name": "Amazon Quase Novo - Alimentos e Bebidas", "safe_name": "Amazon_Quase_Novo_Alimentos_e_Bebidas", "url": "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011&rh=n%3A24669725011%2Cn%3A18991079011&s=popularity-rank&dc&fs=true&qid=1748002601&rnid=24669725011&xpid=M2soDZTyDMNhF&ref=sr_nr_n_1"},
    {"name": "Amazon Quase Novo - Automotivo", "safe_name": "Amazon_Quase_Novo_Automotivo", "url": "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011&rh=n%3A24669725011%2Cn%3A18914209011&s=popularity-rank&dc&fs=true&qid=1748002601&rnid=24669725011&xpid=M2soDZTyDMNhF&ref=sr_nr_n_2"},
    {"name": "Amazon Quase Novo - Bebês", "safe_name": "Amazon_Quase_Novo_Bebes", "url": "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011&rh=n%3A24669725011%2Cn%3A17242603011&s=popularity-rank&dc&fs=true&qid=1748002601&rnid=24669725011&xpid=M2soDZTyDMNhF&ref=sr_nr_n_3"},
    {"name": "Amazon Quase Novo - Beleza", "safe_name": "Amazon_Quase_Novo_Beleza", "url": "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011&rh=n%3A24669725011%2Cn%3A16194414011&s=popularity-rank&dc&fs=true&qid=1748002601&rnid=24669725011&xpid=M2soDZTyDMNhF&ref=sr_nr_n_4"},
    {"name": "Amazon Quase Novo - Brinquedos e Jogos", "safe_name": "Amazon_Quase_Novo_Brinquedos_e_Jogos", "url": "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011&rh=n%3A24669725011%2Cn%3A16194299011&s=popularity-rank&dc&fs=true&qid=1748002601&rnid=24669725011&xpid=M2soDZTyDMNhF&ref=sr_nr_n_5"},
    {"name": "Amazon Quase Novo - Casa", "safe_name": "Amazon_Quase_Novo_Casa", "url": "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011&rh=n%3A24669725011%2Cn%3A16191000011&s=popularity-rank&dc&fs=true&qid=1748002601&rnid=24669725011&xpid=M2soDZTyDMNhF&ref=sr_nr_n_6"},
    {"name": "Amazon Quase Novo - Computadores e Informática", "safe_name": "Amazon_Quase_Novo_Computadores_e_Informatica", "url": "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011&rh=n%3A24669725011%2Cn%3A16339926011&s=popularity-rank&dc&fs=true&qid=1748002601&rnid=24669725011&xpid=M2soDZTyDMNhF&ref=sr_nr_n_7"},
    {"name": "Amazon Quase Novo - Cozinha", "safe_name": "Amazon_Quase_Novo_Cozinha", "url": "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011&rh=n%3A24669725011%2Cn%3A16957125011&s=popularity-rank&dc&fs=true&qid=1748002601&rnid=24669725011&xpid=M2soDZTyDMNhF&ref=sr_nr_n_8"},
    {"name": "Amazon Quase Novo - Dispositivos Amazon e Acessórios", "safe_name": "Amazon_Quase_Novo_Dispositivos_Amazon_e_Acessorios", "url": "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011&rh=n%3A24669725011%2Cn%3A16333486011&s=popularity-rank&dc&fs=true&qid=1748002601&rnid=24669725011&xpid=M2soDZTyDMNhF&ref=sr_nr_n_9"},
    {"name": "Amazon Quase Novo - Eletrodomésticos", "safe_name": "Amazon_Quase_Novo_Eletrodomesticos", "url": "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011&rh=n%3A24669725011%2Cn%3A16522082011&s=popularity-rank&dc&fs=true&qid=1748002601&rnid=24669725011&xpid=M2soDZTyDMNhF&ref=sr_nr_n_10"},
    {"name": "Amazon Quase Novo - Eletrônicos e Tecnologia", "safe_name": "Amazon_Quase_Novo_Eletronicos_e_Tecnologia", "url": "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011&rh=n%3A24669725011%2Cn%3A16209062011&s=popularity-rank&dc&fs=true&qid=1748002601&rnid=24669725011&xpid=M2soDZTyDMNhF&ref=sr_nr_n_11"},
    {"name": "Amazon Quase Novo - Esporte, Aventura e Lazer", "safe_name": "Amazon_Quase_Novo_Esporte_Aventura_e_Lazer", "url": "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011&rh=n%3A24669725011%2Cn%3A17349396011&s=popularity-rank&dc&fs=true&qid=1748002601&rnid=24669725011&xpid=M2soDZTyDMNhF&ref=sr_nr_n_12"},
    {"name": "Amazon Quase Novo - Ferramentas e Materiais de Construção", "safe_name": "Amazon_Quase_Novo_Ferramentas_e_Materiais_de_Construcao", "url": "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011&rh=n%3A24669725011%2Cn%3A16957182011&s=popularity-rank&dc&fs=true&qid=1748002601&rnid=24669725011&xpid=M2soDZTyDMNhF&ref=sr_nr_n_13"},
    {"name": "Amazon Quase Novo - Games e Consoles", "safe_name": "Amazon_Quase_Novo_Games_e_Consoles", "url": "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011&rh=n%3A24669725011%2Cn%3A7791985011&s=popularity-rank&dc&fs=true&qid=1748002601&rnid=24669725011&xpid=M2soDZTyDMNhF&ref=sr_nr_n_14"},
    {"name": "Amazon Quase Novo - Instrumentos Musicais", "safe_name": "Amazon_Quase_Novo_Instrumentos_Musicais", "url": "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011&rh=n%3A24669725011%2Cn%3A18991252011&s=popularity-rank&dc&fs=true&qid=1748002601&rnid=24669725011&xpid=M2soDZTyDMNhF&ref=sr_nr_n_15"},
    {"name": "Amazon Quase Novo - Jardim e Piscina", "safe_name": "Amazon_Quase_Novo_Jardim_e_Piscina", "url": "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011&rh=n%3A24669725011%2Cn%3A18991021011&s=popularity-rank&dc&fs=true&qid=1748002601&rnid=24669725011&xpid=M2soDZTyDMNhF&ref=sr_nr_n_16"},
    {"name": "Amazon Quase Novo - Livros", "safe_name": "Amazon_Quase_Novo_Livros", "url": "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011&rh=n%3A24669725011%2Cn%3A6740748011&s=popularity-rank&dc&fs=true&qid=1748002601&rnid=24669725011&xpid=M2soDZTyDMNhF&ref=sr_nr_n_17"},
    {"name": "Amazon Quase Novo - Loja Kindle", "safe_name": "Amazon_Quase_Novo_Loja_Kindle", "url": "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011&rh=n%3A24669725011%2Cn%3A5308307011&s=popularity-rank&dc&fs=true&qid=1748002601&rnid=24669725011&xpid=M2soDZTyDMNhF&ref=sr_nr_n_18"},
    {"name": "Amazon Quase Novo - Moda", "safe_name": "Amazon_Quase_Novo_Moda", "url": "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011&rh=n%3A24669725011%2Cn%3A17365811011&s=popularity-rank&dc&fs=true&qid=1748002601&rnid=24669725011&xpid=M2soDZTyDMNhF&ref=sr_nr_n_19"},
    {"name": "Amazon Quase Novo - Papelaria e Escritório", "safe_name": "Amazon_Quase_Novo_Papelaria_e_Escritorio", "url": "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011&rh=n%3A24669725011%2Cn%3A16957239011&s=popularity-rank&dc&fs=true&qid=1748002601&rnid=24669725011&xpid=M2soDZTyDMNhF&ref=sr_nr_n_20"},
    {"name": "Amazon Quase Novo - Pet Shop", "safe_name": "Amazon_Quase_Novo_Pet_Shop", "url": "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011&rh=n%3A24669725011%2Cn%3A18991136011&s=popularity-rank&dc&fs=true&qid=1748002601&rnid=24669725011&xpid=M2soDZTyDMNhF&ref=sr_nr_n_21"},
    {"name": "Amazon Quase Novo - Saúde e Bem-Estar", "safe_name": "Amazon_Quase_Novo_Saude_e_Bem_Estar", "url": "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011&rh=n%3A24669725011%2Cn%3A16215417011&s=popularity-rank&dc&fs=true&qid=1748002601&rnid=24669725011&xpid=M2soDZTyDMNhF&ref=sr_nr_n_22"},
]

MIN_DESCONTO_USADOS_STR = os.getenv("MIN_DESCONTO_PERCENTUAL_USADOS", "40").strip()
try:
    MIN_DESCONTO_USADOS = int(MIN_DESCONTO_USADOS_STR)
    if not (0 <= MIN_DESCONTO_USADOS <= 100):
        logger.warning(f"MIN_DESCONTO_USADOS ({MIN_DESCONTO_USADOS}%) fora do intervalo [0, 100]. Usando 40%.")
        MIN_DESCONTO_USADOS = 40
except ValueError:
    logger.warning(f"Valor inválido para MIN_DESCONTO_PERCENTUAL_USADOS ('{MIN_DESCONTO_USADOS_STR}'). Usando 40%.")
    MIN_DESCONTO_USADOS = 40
logger.info(f"Desconto mínimo para notificação de usados: {MIN_DESCONTO_USADOS}%")

USAR_HISTORICO_STR = os.getenv("USAR_HISTORICO_GLOBAL_USADOS", "true").strip().lower()
USAR_HISTORICO = USAR_HISTORICO_STR == "true"
logger.info(f"Usar histórico para produtos usados: {USAR_HISTORICO}")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_IDS_STR = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_CHAT_IDS_LIST = [chat_id.strip() for chat_id in TELEGRAM_CHAT_IDS_STR.split(',') if chat_id.strip()]

MAX_PAGINAS_POR_LINK_GLOBAL = int(os.getenv("MAX_PAGINAS_USADOS", "10")) # Adicionado env var
HISTORY_DIR_BASE = "history_files_usados"
DEBUG_LOGS_DIR_BASE = "debug_logs_usados"
GLOBAL_HISTORY_FILENAME = "price_history_USADOS_GLOBAL.json"
CONCURRENCY_LIMIT = int(os.getenv("CONCURRENCY_LIMIT_USADOS", "3")) # Adicionado env var

logger.info(f"Máximo de páginas por categoria: {MAX_PAGINAS_POR_LINK_GLOBAL}")
logger.info(f"Limite de concorrência: {CONCURRENCY_LIMIT}")

os.makedirs(HISTORY_DIR_BASE, exist_ok=True)
logger.info(f"Diretório de histórico '{HISTORY_DIR_BASE}' verificado/criado.")
os.makedirs(DEBUG_LOGS_DIR_BASE, exist_ok=True)
logger.info(f"Diretório de logs de debug '{DEBUG_LOGS_DIR_BASE}' verificado/criado.")


bot_instance_global = None
if TELEGRAM_TOKEN and TELEGRAM_CHAT_IDS_LIST:
    try:
        bot_instance_global = Bot(token=TELEGRAM_TOKEN)
        logger.info(f"Instância global do Bot Telegram criada. IDs de Chat: {TELEGRAM_CHAT_IDS_LIST}")
    except Exception as e:
        logger.error(f"Falha ao inicializar Bot global: {e}", exc_info=True)
else:
    logger.warning("Token do Telegram ou Chat IDs não configurados. Notificações Telegram desabilitadas.")


def iniciar_driver_sync_worker(worker_logger, driver_path=None):
    worker_logger.info("Iniciando configuração do WebDriver...")
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920x1080")
    user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36" # Atualizado
    chrome_options.add_argument(f"user-agent={user_agent}")
    worker_logger.info(f"User-Agent: {user_agent}")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    worker_logger.info(f"Opções do Chrome configuradas: {chrome_options.arguments}")

    service = None
    try:
        if driver_path:
            worker_logger.info(f"Usando Service com driver_path: {driver_path}")
            service = Service(driver_path)
        else:
            worker_logger.info("Usando Service com ChromeDriverManager para instalar/gerenciar o ChromeDriver.")
            service = Service(ChromeDriverManager().install())
        worker_logger.info("Serviço do ChromeDriver configurado.")
    except Exception as e:
        worker_logger.error(f"Erro ao configurar o Service do ChromeDriver: {e}", exc_info=True)
        raise

    driver = None
    try:
        worker_logger.info("Tentando instanciar o webdriver.Chrome...")
        driver = webdriver.Chrome(service=service, options=chrome_options)
        worker_logger.info("WebDriver instanciado com sucesso.")
        page_load_timeout = 60 # Aumentado
        driver.set_page_load_timeout(page_load_timeout)
        worker_logger.info(f"Timeout de carregamento de página definido para {page_load_timeout}s.")
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        worker_logger.info("Script para ocultar 'navigator.webdriver' executado.")
        return driver
    except Exception as e:
        worker_logger.error(f"Erro ao instanciar ou configurar o WebDriver: {e}", exc_info=True)
        if driver:
            driver.quit()
        raise

async def send_telegram_message_async(bot, chat_id, message, parse_mode, msg_logger):
    msg_logger.debug(f"Tentando enviar mensagem para chat_id: {chat_id}")
    try:
        await bot.send_message(chat_id=chat_id, text=message, parse_mode=parse_mode)
        msg_logger.info(f"Mensagem enviada com sucesso para {chat_id}.")
        return True
    except TelegramError as e:
        msg_logger.error(f"Erro Telegram ao enviar mensagem para {chat_id}: {e}", exc_info=True)
        return False
    except Exception as e:
        msg_logger.error(f"Erro inesperado ao enviar mensagem para {chat_id}: {e}", exc_info=True)
        return False


def escape_md(text):
    return re.sub(r'([_\*\[\]\(\)~`>#+\-=|{}.!])', r'\\\1', str(text)) # Adicionado str() para segurança

def get_price_from_element(element, price_logger):
    price_logger.debug("Tentando extrair preço do elemento.")
    try:
        price_whole_el = element.find_element(By.CSS_SELECTOR, SELETOR_PRECO_USADO)
        price_whole = price_whole_el.text
        price_logger.debug(f"Parte inteira do preço encontrada: '{price_whole}'")

        price_fraction_el = element.find_element(By.CSS_SELECTOR, SELETOR_FRACAO_PRECO)
        price_fraction = price_fraction_el.text
        price_logger.debug(f"Fração do preço encontrada: '{price_fraction}'")

        raw_price = f"{price_whole}.{price_fraction}"
        cleaned = re.sub(r'[^\d.]', '', raw_price)
        price_logger.debug(f"Preço bruto: '{raw_price}', Preço limpo: '{cleaned}'")
        final_price = float(cleaned)
        price_logger.debug(f"Preço final como float: {final_price}")
        return final_price
    except NoSuchElementException as e:
        price_logger.warning(f"Elemento de preço (inteiro ou fração) não encontrado: {e}")
        return None
    except ValueError as e:
        price_logger.error(f"Erro de valor ao converter preço '{cleaned}' para float: {e}", exc_info=True)
        return None
    except Exception as e:
        price_logger.error(f"Erro inesperado ao obter preço: {e}", exc_info=True)
        return None

def load_history():
    history_path = os.path.join(HISTORY_DIR_BASE, GLOBAL_HISTORY_FILENAME)
    logger.info(f"Tentando carregar histórico de: {history_path}")
    if os.path.exists(history_path):
        try:
            with open(history_path, 'r', encoding='utf-8') as f:
                history_data = json.load(f)
            logger.info(f"Histórico carregado com sucesso. {len(history_data)} ASINs no histórico.")
            return history_data
        except json.JSONDecodeError as e:
            logger.error(f"Erro ao decodificar JSON do histórico '{history_path}': {e}. Retornando histórico vazio.", exc_info=True)
            return {}
        except Exception as e:
            logger.error(f"Erro inesperado ao carregar histórico de '{history_path}': {e}. Retornando histórico vazio.", exc_info=True)
            return {}
    else:
        logger.info("Arquivo de histórico não encontrado. Retornando histórico vazio.")
        return {}

def save_history(history):
    history_path = os.path.join(HISTORY_DIR_BASE, GLOBAL_HISTORY_FILENAME)
    logger.info(f"Tentando salvar histórico ({len(history)} ASINs) em: {history_path}")
    try:
        with open(history_path, 'w', encoding='utf-8') as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
        logger.info("Histórico salvo com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao salvar histórico em '{history_path}': {e}", exc_info=True)

def get_url_for_page_worker(base_url, page_number, current_logger):
    current_logger.debug(f"Gerando URL para página {page_number} a partir de base: {base_url}")
    parsed_url = urlparse(base_url)
    query_params = parse_qs(parsed_url.query)
    query_params['page'] = [str(page_number)]
    try:
        # Tenta obter o loop de eventos asyncio. Se não estiver em um contexto asyncio, pode falhar.
        qid_time = asyncio.get_event_loop().time()
    except RuntimeError:
        # Fallback se não houver loop de eventos (ex: se chamado de um thread não gerenciado por asyncio)
        import time
        qid_time = time.time()
        current_logger.warning("asyncio.get_event_loop().time() falhou, usando time.time() para qid.")

    query_params['qid'] = [str(int(qid_time * 1000))]
    query_params['ref'] = [f'sr_pg_{page_number}']
    new_query = urlencode(query_params, doseq=True)
    final_url = urlunparse(parsed_url._replace(query=new_query))
    current_logger.debug(f"URL gerada: {final_url}")
    return final_url

def check_captcha_sync_worker(driver, captcha_logger):
    captcha_logger.debug("Verificando a presença de CAPTCHA.")
    try:
        WebDriverWait(driver, 5).until(EC.any_of( # Timeout um pouco maior para detecção
            EC.presence_of_element_located((By.CSS_SELECTOR, "form[action*='captcha'] img")),
            EC.presence_of_element_located((By.XPATH, "//h4[contains(text(), 'Insira os caracteres')]")),
            EC.presence_of_element_located((By.XPATH, "//h4[contains(text(), 'Digite os caracteres que você vê abaixo')]")),
            EC.presence_of_element_located((By.CSS_SELECTOR, "iframe[src*='captcha']"))
        ))
        captcha_logger.warning(f"CAPTCHA detectado! URL: {driver.current_url}")
        # Salvar screenshot em caso de CAPTCHA
        screenshot_path = os.path.join(DEBUG_LOGS_DIR_BASE, f"captcha_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
        driver.save_screenshot(screenshot_path)
        captcha_logger.info(f"Screenshot do CAPTCHA salvo em: {screenshot_path}")
        return True
    except (TimeoutException, NoSuchElementException):
        captcha_logger.debug("Nenhum CAPTCHA detectado dentro do timeout.")
        return False
    except Exception as e:
        captcha_logger.error(f"Erro inesperado ao verificar CAPTCHA: {e}", exc_info=True)
        return False # Assume no captcha on error to avoid getting stuck

def wait_for_page_load(driver, page_load_logger):
    page_load_logger.debug("Aguardando carregamento completo da página (document.readyState == 'complete').")
    try:
        WebDriverWait(driver, 60).until( # Timeout aumentado
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        page_load_logger.info("Página carregada completamente (document.readyState is 'complete').")
        return True
    except TimeoutException:
        page_load_logger.error("Timeout (60s) ao esperar o carregamento completo da página (document.readyState).", exc_info=True)
        current_url = driver.current_url
        page_source_path = os.path.join(DEBUG_LOGS_DIR_BASE, f"timeout_readyState_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html")
        try:
            with open(page_source_path, "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            page_load_logger.info(f"Page source em timeout salva em {page_source_path} para URL: {current_url}")
        except Exception as e_save:
            page_load_logger.error(f"Falha ao salvar page source: {e_save}")
        return False
    except Exception as e:
        page_load_logger.error(f"Erro inesperado ao aguardar carregamento da página: {e}", exc_info=True)
        return False


async def process_category(
    driver, url_inicial_categoria, nome_categoria, category_specific_logger,
    history, min_desconto, bot, chat_ids_list
):
    category_specific_logger.info(f"--- Iniciando processamento da Categoria: {nome_categoria} --- URL inicial: {url_inicial_categoria} ---")

    parsed_initial_url = urlparse(url_inicial_categoria)
    query_params = parse_qs(parsed_initial_url.query)
    query_params.pop('page', None)
    query_params.pop('ref', None)
    query_params.pop('qid', None) # qid deve ser gerado por página
    cleaned_query_string = urlencode(query_params, doseq=True)
    base_url_para_paginacao = urlunparse(parsed_initial_url._replace(query=cleaned_query_string))
    category_specific_logger.info(f"[{nome_categoria}] URL base para paginação: {base_url_para_paginacao}")

    paginas_sem_produtos_consecutivas = 0
    produtos_processados_na_categoria = 0
    paginas_processadas = 0

    for page_num in range(1, MAX_PAGINAS_POR_LINK_GLOBAL + 1):
        paginas_processadas += 1
        page_url = get_url_for_page_worker(base_url_para_paginacao, page_num, category_specific_logger)
        category_specific_logger.info(f"[{nome_categoria}] Tentando carregar Página: {page_num}/{MAX_PAGINAS_POR_LINK_GLOBAL}, URL: {page_url}")

        max_load_attempts = 3
        page_loaded_successfully = False
        for attempt in range(1, max_load_attempts + 1):
            category_specific_logger.info(f"[{nome_categoria}] Tentativa {attempt}/{max_load_attempts} de carregar URL: {page_url}")
            try:
                await asyncio.to_thread(driver.get, page_url)
                category_specific_logger.info(f"[{nome_categoria}] URL {page_url} solicitada ao driver.")
                await asyncio.sleep(5) # Delay pós-solicitação, antes de verificar o estado

                if not await asyncio.to_thread(wait_for_page_load, driver, category_specific_logger):
                    category_specific_logger.warning(f"[{nome_categoria}] Página {page_num} (tentativa {attempt}) não carregou completamente (readyState).")
                    await asyncio.sleep(3 * attempt) # Backoff
                    continue

                if await asyncio.to_thread(check_captcha_sync_worker, driver, category_specific_logger):
                    category_specific_logger.error(f"[{nome_categoria}] CAPTCHA detectado na página {page_num} (tentativa {attempt}). Abortando categoria.")
                    await asyncio.sleep(10) # Delay maior para CAPTCHA
                    return # Aborta processamento desta categoria se CAPTCHA é detectado

                # Espera explícita pelos itens de produto
                category_specific_logger.debug(f"[{nome_categoria}] Aguardando presença de itens de produto com seletor: '{SELETOR_ITEM_PRODUTO_USADO}'")
                await asyncio.to_thread(
                    WebDriverWait(driver, 45).until,
                    EC.presence_of_element_located((By.CSS_SELECTOR, SELETOR_ITEM_PRODUTO_USADO))
                )
                category_specific_logger.info(f"[{nome_categoria}] Seletor de item de produto encontrado na página {page_num}.")
                page_loaded_successfully = True
                break # Sai do loop de tentativas de carregamento
            
            except TimeoutException as e_timeout:
                category_specific_logger.warning(f"[{nome_categoria}] Timeout (WebDriverWait) ao carregar/encontrar itens na página {page_num} (tentativa {attempt}): {e_timeout}")
                if attempt == max_load_attempts:
                    category_specific_logger.error(f"[{nome_categoria}] Todas as {max_load_attempts} tentativas de carregar a página {page_num} falharam devido a Timeout. Desistindo desta página.")
                    # Salvar screenshot em caso de falha persistente
                    screenshot_path = os.path.join(DEBUG_LOGS_DIR_BASE, f"timeout_final_cat_{nome_categoria}_p{page_num}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                    try:
                        await asyncio.to_thread(driver.save_screenshot, screenshot_path)
                        category_specific_logger.info(f"[{nome_categoria}] Screenshot da falha de carregamento salvo em: {screenshot_path}")
                    except Exception as e_ss:
                        category_specific_logger.error(f"[{nome_categoria}] Erro ao salvar screenshot: {e_ss}")
                    break # Sai do loop de tentativas e vai para a próxima página ou termina
                await asyncio.sleep(5 * attempt) # Backoff progressivo
            except Exception as e_general:
                category_specific_logger.error(f"[{nome_categoria}] Erro geral ao carregar página {page_num} (tentativa {attempt}): {e_general}", exc_info=True)
                if attempt == max_load_attempts:
                    category_specific_logger.error(f"[{nome_categoria}] Todas as {max_load_attempts} tentativas de carregar a página {page_num} falharam. Desistindo desta página.")
                    break
                await asyncio.sleep(5 * attempt)

        if not page_loaded_successfully:
            category_specific_logger.warning(f"[{nome_categoria}] Não foi possível carregar a página {page_num} após {max_load_attempts} tentativas. Pulando para a próxima.")
            # Considerar se deve quebrar o loop da categoria aqui ou continuar para a próxima página.
            # Se uma página falha, as seguintes também podem falhar.
            # paginas_sem_produtos_consecutivas +=1 # Ou um contador de falhas de página
            # if paginas_sem_produtos_consecutivas >= X: break
            continue


        items = []
        try:
            items = await asyncio.to_thread(driver.find_elements, By.CSS_SELECTOR, SELETOR_ITEM_PRODUTO_USADO)
            category_specific_logger.info(f"[{nome_categoria}] Página {page_num}: Encontrados {len(items)} elementos com seletor '{SELETOR_ITEM_PRODUTO_USADO}'.")
        except InvalidSelectorException:
            category_specific_logger.error(f"[{nome_categoria}] Seletor CSS '{SELETOR_ITEM_PRODUTO_USADO}' é inválido. Corrija o seletor.", exc_info=True)
            break # Não adianta continuar na categoria se o seletor principal está errado.
        except Exception as e:
            category_specific_logger.error(f"[{nome_categoria}] Erro ao buscar itens na página {page_num}: {e}", exc_info=True)
            # Pode ser uma página de erro da Amazon, sem resultados, etc.
            # Verificar se é um padrão "sem resultados"
            try:
                no_results_el = await asyncio.to_thread(driver.find_elements, By.XPATH, "//span[contains(text(),'Nenhum resultado para')] | //*[contains(text(),'não encontraram nenhum resultado')]")
                if no_results_el:
                    category_specific_logger.info(f"[{nome_categoria}] Página {page_num} indica 'Nenhum resultado'. Provavelmente fim dos produtos.")
                    break # Fim dos resultados
            except:
                pass # Ignora se a verificação de "nenhum resultado" falhar
            continue # Tenta a próxima página se não for erro de seletor


        if not items:
            category_specific_logger.warning(f"[{nome_categoria}] Nenhum item de produto encontrado na página {page_num} usando o seletor principal.")
            paginas_sem_produtos_consecutivas += 1
            if paginas_sem_produtos_consecutivas >= 2 and page_num > 1: # Só quebra se não for a primeira página
                category_specific_logger.info(f"[{nome_categoria}] {paginas_sem_produtos_consecutivas} páginas consecutivas sem produtos. Finalizando categoria.")
                break
            continue # Tenta a próxima página
        else:
            paginas_sem_produtos_consecutivas = 0 # Reseta o contador

        organic_items_count = 0
        for item_idx, item in enumerate(items):
            category_specific_logger.debug(f"[{nome_categoria}] Processando item {item_idx + 1}/{len(items)} na página {page_num}.")
            try:
                current_asin = await asyncio.to_thread(item.get_attribute, 'data-asin')
                if not current_asin:
                    category_specific_logger.debug(f"[{nome_categoria}] Item {item_idx+1} sem data-asin. Pulando.")
                    continue

                is_sponsored = False
                xpath_sponsored = ".//span[contains(translate(normalize-space(.), 'PATROCINADOABCDEFGHIJKLMNOPQRSTUVWXYZ', 'patrocinadoabcdefghijklmnopqrstuvwxyz'), 'patrocinado')] | .//div[@data-cy='sponsored-label'] | .//a[@data-a-Qualifier='sp'] | .//span[text()='Sponsored'] | .//span[contains(@class, 'sponsored')]"
                try:
                    sponsored_indicators = await asyncio.to_thread(item.find_elements, By.XPATH, xpath_sponsored)
                    if sponsored_indicators:
                        for ind_idx, ind in enumerate(sponsored_indicators):
                            # O log abaixo pode ser muito verboso, considere DEBUG
                            # category_specific_logger.debug(f"[{nome_categoria}] ASIN {current_asin}, Indicador sponsored {ind_idx+1}: visível? {await asyncio.to_thread(ind.is_displayed)}, texto: '{await asyncio.to_thread(ind.text)}'")
                            if await asyncio.to_thread(ind.is_displayed):
                                is_sponsored = True
                                break
                    if is_sponsored:
                        category_specific_logger.info(f"[{nome_categoria}] Item patrocinado ignorado (ASIN: {current_asin}).")
                        continue
                except Exception as e_sponsor:
                     category_specific_logger.warning(f"[{nome_categoria}] ASIN {current_asin}: Erro ao checar se é patrocinado: {e_sponsor}. Assumindo que não é.")


                # Checar se é "Usado"
                try:
                    indicador_usado_el = await asyncio.to_thread(item.find_element, By.CSS_SELECTOR, SELETOR_INDICADOR_USADO)
                    texto_indicador = (await asyncio.to_thread(indicador_usado_el.text)).lower()
                    category_specific_logger.debug(f"[{nome_categoria}] ASIN {current_asin}: Texto do indicador: '{texto_indicador}'")
                    if "usado" not in texto_indicador and "recondicionado" not in texto_indicador : # Adicionado "recondicionado"
                        category_specific_logger.debug(f"[{nome_categoria}] ASIN {current_asin} não é 'Usado' nem 'Recondicionado' pelo indicador ('{texto_indicador}'). Pulando.")
                        continue
                except NoSuchElementException:
                    category_specific_logger.debug(f"[{nome_categoria}] ASIN {current_asin} sem indicador de usado '{SELETOR_INDICADOR_USADO}'. Pulando.")
                    continue
                
                organic_items_count += 1
                produtos_processados_na_categoria += 1

                nome_el = await asyncio.to_thread(item.find_element, By.CSS_SELECTOR, SELETOR_NOME_PRODUTO_USADO)
                nome = (await asyncio.to_thread(nome_el.text))[:150].strip()

                preco = await asyncio.to_thread(get_price_from_element, item, category_specific_logger)

                if not preco:
                    category_specific_logger.warning(f"[{nome_categoria}] Preço não encontrado para o produto (ASIN: {current_asin}, Nome: {nome}). Pulando.")
                    continue
                
                # Link é reconstruído para garantir o formato /dp/ASIN
                link_base_amazon = "https://www.amazon.com.br"
                link_produto = f"{link_base_amazon}/dp/{current_asin}"
                category_specific_logger.info(f"[{nome_categoria}] Produto encontrado: ASIN {current_asin}, Nome: {nome}, Preço: R${preco:.2f}, Link: {link_produto}")


                if USAR_HISTORICO:
                    category_specific_logger.debug(f"[{nome_categoria}] Verificando histórico para ASIN {current_asin}.")
                    product_entry = history.get(current_asin)
                    
                    if product_entry: # ASIN existe no histórico
                        last_recorded_price_info = product_entry['precos'][-1] if product_entry['precos'] else None
                        last_price = last_recorded_price_info['preco'] if last_recorded_price_info else None
                        
                        if last_price is not None and preco >= last_price:
                            category_specific_logger.info(f"[{nome_categoria}] ASIN {current_asin}: Preço atual (R${preco:.2f}) >= último preço registrado (R${last_price:.2f}). Sem notificação.")
                            # Atualizar 'category_last_seen' mesmo se o preço não for menor, e data.
                            history[current_asin]['category_last_seen'] = nome_categoria
                            history[current_asin]['precos'].append({ # Adiciona o preço atual mesmo que maior, para histórico
                                'preco': preco,
                                'data': datetime.now().isoformat()
                            })
                            # Limitar o número de entradas de preço para evitar arquivos de histórico muito grandes
                            max_price_entries = 20 
                            if len(history[current_asin]['precos']) > max_price_entries:
                                history[current_asin]['precos'] = history[current_asin]['precos'][-max_price_entries:]
                            # Não notifica, mas pode salvar o histórico se quiser registrar a visualização
                            # save_history(history) # Salvar aqui pode ser custoso, melhor no final do script
                            continue # Pula para o próximo item
                        else:
                             category_specific_logger.info(f"[{nome_categoria}] ASIN {current_asin}: Preço atual (R${preco:.2f}) < último preço (R${last_price if last_price else 'N/A'}). Notificando.")
                    else: # Novo ASIN, não está no histórico
                        category_specific_logger.info(f"[{nome_categoria}] ASIN {current_asin} é novo no histórico. Notificando.")
                        history[current_asin] = {'nome': nome, 'precos': [], 'link': link_produto, 'category_last_seen': nome_categoria}
                        last_price = None # Para a mensagem de desconto

                    # Adicionar preço atual ao histórico (seja novo ou menor)
                    history[current_asin]['precos'].append({
                        'preco': preco,
                        'data': datetime.now().isoformat()
                    })
                    max_price_entries = 20 
                    if len(history[current_asin]['precos']) > max_price_entries:
                        history[current_asin]['precos'] = history[current_asin]['precos'][-max_price_entries:]
                    history[current_asin]['nome'] = nome # Atualiza o nome caso tenha mudado
                    history[current_asin]['link'] = link_produto # Atualiza o link caso tenha mudado
                    history[current_asin]['category_last_seen'] = nome_categoria
                    # save_history(history) # Salvar aqui pode ser custoso, melhor no final do script global

                # Lógica de notificação (desconto mínimo não é aplicado aqui, pois o foco é em usados e alterações de preço)
                # Se MIN_DESCONTO_USADOS for relevante, precisaria de um "preço de referência novo"
                # Por ora, a notificação ocorre para qualquer produto usado encontrado que seja novo no histórico ou teve queda de preço.
                
                if bot and bot_instance_global and TELEGRAM_CHAT_IDS_LIST:
                    desconto_info_msg = "Novo produto no rastreamento!"
                    if USAR_HISTORICO and last_price: # Só calcula desconto se havia preço anterior
                        if preco < last_price:
                            desconto_percentual = ((last_price - preco) / last_price) * 100
                            desconto_info_msg = f"Preço caiu! Antes: R${last_price:.2f}. Desconto: {desconto_percentual:.2f}%"
                        else:
                            # Este caso não deveria ser notificado se preco >= last_price, já tratado acima
                            desconto_info_msg = f"Preço estável ou aumentou desde R${last_price:.2f}." 

                    message = (
                        f"*{escape_md('Produto Usado Encontrado!')}*\n\n"
                        f"*Categoria*: {escape_md(nome_categoria)}\n"
                        f"*Nome*: {escape_md(nome)}\n"
                        f"*Preço*: R${preco:.2f}\n"
                        f"*Detalhe*: {escape_md(desconto_info_msg)}\n"
                        f"*Link*: {escape_md(link_produto)}"
                    )
                    category_specific_logger.debug(f"[{nome_categoria}] Preparando para enviar mensagem Telegram para ASIN {current_asin}.")
                    for chat_id in chat_ids_list:
                        await send_telegram_message_async(bot_instance_global, chat_id, message, ParseMode.MARKDOWN, category_specific_logger)
                else:
                    category_specific_logger.info(f"[{nome_categoria}] Bot Telegram não configurado ou desabilitado. Sem notificação para ASIN {current_asin}.")

            except StaleElementReferenceException:
                category_specific_logger.warning(f"[{nome_categoria}] Elemento obsoleto encontrado ao processar item {item_idx + 1}. Pulando item.", exc_info=False) # exc_info=False para não poluir tanto
                continue
            except NoSuchElementException as e_detail:
                category_specific_logger.warning(f"[{nome_categoria}] Detalhe não encontrado para item {item_idx + 1} (ASIN: {current_asin if 'current_asin' in locals() else 'N/A'}): {e_detail}. Pulando item.")
                continue
            except Exception as e_item:
                category_specific_logger.error(f"[{nome_categoria}] Erro inesperado ao processar item {item_idx + 1} (ASIN: {current_asin if 'current_asin' in locals() else 'N/A'}): {e_item}", exc_info=True)
                continue
        
        category_specific_logger.info(f"[{nome_categoria}] Página {page_num}: {organic_items_count} itens orgânicos processados.")

        # Lógica de Próxima Página
        try:
            category_specific_logger.debug(f"[{nome_categoria}] Verificando botão 'Próxima Página' (seletor: {SELETOR_PROXIMA_PAGINA})")
            next_page_button = await asyncio.to_thread(driver.find_element, By.CSS_SELECTOR, SELETOR_PROXIMA_PAGINA)
            # A Amazon usa 's-pagination-disabled' para indicar que o botão está desabilitado
            button_classes = await asyncio.to_thread(next_page_button.get_attribute, 'class')
            if 's-pagination-disabled' in button_classes or not await asyncio.to_thread(next_page_button.is_enabled):
                category_specific_logger.info(f"[{nome_categoria}] Botão 'Próxima Página' está desabilitado ou é a última página. Classes: '{button_classes}'. Fim da categoria.")
                break 
            category_specific_logger.info(f"[{nome_categoria}] Botão 'Próxima Página' encontrado e habilitado. Indo para a próxima.")
        except NoSuchElementException:
            category_specific_logger.info(f"[{nome_categoria}] Botão 'Próxima Página' não encontrado. Provavelmente fim da categoria.")
            break 
        except Exception as e_pagination:
            category_specific_logger.error(f"[{nome_categoria}] Erro ao verificar botão 'Próxima Página': {e_pagination}", exc_info=True)
            break

        await asyncio.sleep(max(3, int(os.getenv("DELAY_ENTRE_PAGINAS_USADOS", "5")))) # Delay entre páginas, configurável

    category_specific_logger.info(f"--- Concluído Categoria: {nome_categoria}. Páginas processadas: {paginas_processadas}. Total de produtos (orgânicos e usados) processados: {produtos_processados_na_categoria} ---")


async def scrape_category_worker_async(
    category_details, min_desconto, bot, chat_ids_list, semaphore, concurrency_limit, history, driver_path=None
):
    cat_name = category_details["name"]
    cat_safe_name = category_details["safe_name"]
    cat_url = category_details["url"]

    # Configuração do logger específico do worker
    worker_logger_name = f"worker.{cat_safe_name.replace('.', '_')}" # Garante nome válido
    worker_logger = logging.getLogger(worker_logger_name)
    
    # Evitar adicionar handlers múltiplos se a função for chamada várias vezes com o mesmo logger (improvável com asyncio.gather, mas bom previnir)
    if not worker_logger.handlers:
        log_filename = os.path.join(DEBUG_LOGS_DIR_BASE, f"scrape_debug_{cat_safe_name}.log")
        try:
            file_handler = logging.FileHandler(log_filename, encoding="utf-8", mode="w") # Sobrescreve o log a cada execução
            file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - [%(name)s:%(funcName)s:%(lineno)d] - %(message)s"))
            worker_logger.addHandler(file_handler)
        except Exception as e_fh:
             # Usa o logger principal se o file_handler falhar
            logger.error(f"Falha ao criar FileHandler para {worker_logger_name} em {log_filename}: {e_fh}. Logs irão para o console.")

        # Define o nível do logger do worker. Pode ser DEBUG para mais detalhes.
        # Considere controlar isso via variável de ambiente também.
        worker_logger_level_str = os.getenv("WORKER_LOG_LEVEL", "INFO").upper()
        worker_logger_level = getattr(logging, worker_logger_level_str, logging.INFO)
        worker_logger.setLevel(worker_logger_level)
        worker_logger.propagate = False # Evita duplicar logs se o logger raiz já tem handler de console

    driver = None
    async with semaphore:
        slots_ocupados_antes = concurrency_limit - semaphore._value
        logger.info(f"Semáforo ADQUIRIDO por: '{cat_name}'. Slots ocupados (antes+este): {slots_ocupados_antes}/{concurrency_limit}.")
        worker_logger.info(f"--- [WORKER INÍCIO] Categoria: {cat_name} (URL: {cat_url}) ---")

        try:
            worker_logger.info(f"[{cat_name}] Tentando iniciar o driver...")
            driver = await asyncio.to_thread(iniciar_driver_sync_worker, worker_logger, driver_path)
            worker_logger.info(f"[{cat_name}] Driver Selenium iniciado com sucesso.")
            
            await process_category(
                driver, cat_url, cat_name, worker_logger, history,
                min_desconto, bot, chat_ids_list
            )
            worker_logger.info(f"[{cat_name}] Processamento da categoria concluído.")

        except Exception as e_worker:
            worker_logger.error(f"[{cat_name}] Erro principal no worker para {cat_name}: {e_worker}", exc_info=True)
        finally:
            if driver:
                worker_logger.info(f"[{cat_name}] Tentando fechar o driver Selenium...")
                try:
                    await asyncio.to_thread(driver.quit)
                    worker_logger.info(f"[{cat_name}] Driver Selenium fechado com sucesso.")
                except Exception as e_quit:
                    worker_logger.error(f"[{cat_name}] Erro ao fechar o driver Selenium: {e_quit}", exc_info=True)
            
            worker_logger.info(f"--- [WORKER FIM] Categoria: {cat_name} ---")
            # Fecha o file handler para garantir que os logs sejam escritos
            for handler in list(worker_logger.handlers): # list() para evitar problemas ao modificar durante iteração
                if isinstance(handler, logging.FileHandler):
                    handler.close()
                    worker_logger.removeHandler(handler)


    slots_ocupados_depois = concurrency_limit - semaphore._value
    logger.info(f"Worker para '{cat_name}' LIBEROU semáforo. Slots ocupados agora: {slots_ocupados_depois}/{concurrency_limit}.")


async def orchestrate_all_usados_scrapes_main_async():
    logger.info("--- INICIANDO ORQUESTRADOR DE SCRAPING DE USADOS ---")
    
    global history # Certificar que estamos usando a variável global se modificada em process_category e salva aqui
    history = load_history() # Carrega o histórico uma vez no início

    installed_chromedriver_path = None
    try:
        logger.info("Tentando instalar/localizar ChromeDriver via WebDriverManager...")
        installed_chromedriver_path = ChromeDriverManager().install()
        logger.info(f"WebDriverManager instalou/localizou ChromeDriver em: {installed_chromedriver_path}")
    except Exception as e:
        logger.error(f"Falha ao instalar/localizar ChromeDriver via WebDriverManager: {e}. O script pode falhar se o ChromeDriver não estiver no PATH.", exc_info=True)
        # Não necessariamente fatal, se o chromedriver estiver no PATH ou fornecido de outra forma,
        # a função iniciar_driver_sync_worker (sem driver_path) pode tentar usá-lo.

    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    logger.info(f"Limite de concorrência definido para: {CONCURRENCY_LIMIT} scrapes simultâneos.")

    tasks = []
    logger.info(f"Criando tasks para {len(CATEGORIES)} categorias...")
    for i, category in enumerate(CATEGORIES):
        logger.info(f"Configurando task {i+1}/{len(CATEGORIES)} para categoria: {category['name']}")
        tasks.append(scrape_category_worker_async(
            category_details=category,
            min_desconto=MIN_DESCONTO_USADOS,
            bot=bot_instance_global, # Passa a instância do bot
            chat_ids_list=TELEGRAM_CHAT_IDS_LIST,
            semaphore=semaphore,
            concurrency_limit=CONCURRENCY_LIMIT,
            history=history, # Passa a referência do dicionário de histórico compartilhado
            driver_path=installed_chromedriver_path
        ))

    logger.info(f"Iniciando execução de {len(tasks)} tasks com asyncio.gather...")
    # return_exceptions=True fará com que gather não pare se uma task falhar,
    # e os resultados conterão as exceções para as tasks falhas.
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    logger.info("Todas as tasks de scraping foram concluídas ou falharam.")
    for i, result in enumerate(results):
        cat_name = CATEGORIES[i]['name']
        if isinstance(result, Exception):
            logger.error(f"Task para categoria '{cat_name}' resultou em uma exceção: {result}", exc_info=result)
        else:
            logger.info(f"Task para categoria '{cat_name}' concluída com sucesso (retorno: {result}).")


    if USAR_HISTORICO:
        logger.info("Salvando histórico global final...")
        save_history(history) # Salva o histórico uma vez no final, com todas as atualizações
    
    logger.info("--- ORQUESTRADOR DE SCRAPING DE USADOS CONCLUÍDO ---")

if __name__ == "__main__":
    script_name = os.path.basename(__file__)
    logger.info(f"Orquestrador de USADOS ('{script_name}') chamado via __main__.")
    
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_IDS_LIST:
        logger.warning("ALERTA USADOS: Token do Telegram ou Chat IDs não configurados. Notificações Telegram estarão desabilitadas.")
    
    # Para depuração de asyncio em alguns ambientes:
    # os.environ['PYTHONASYNCIODEBUG'] = '1'
    # logging.getLogger('asyncio').setLevel(logging.DEBUG)

    try:
        asyncio.run(orchestrate_all_usados_scrapes_main_async())
    except KeyboardInterrupt:
        logger.info("Execução interrompida pelo usuário (KeyboardInterrupt).")
    except Exception as e_main:
        logger.critical(f"Erro fatal no loop principal do orquestrador: {e_main}", exc_info=True)
    finally:
        logger.info(f"Finalizando script '{script_name}'.")
