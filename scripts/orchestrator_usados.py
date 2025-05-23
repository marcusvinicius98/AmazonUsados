import os
import re
import logging
import asyncio
import json
import time
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
    format="%(asctime)s - %(levelname)s - [%(module)s:%(funcName)s:%(lineno)d] - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("ORCHESTRATOR_USADOS")
logger.propagate = False
logger.setLevel(logging.INFO)

# Configurações
SELETOR_ITEM_PRODUTO_USADO = "div.s-result-item.s-asin"
SELETOR_NOME_PRODUTO_USADO = "span.a-size-base-plus.a-color-base.a-text-normal"
SELETOR_LINK_PRODUTO_USADO = "a.a-link-normal.s-underline-text.s-underline-link-text.s-link-style.a-text-normal"
SELETOR_PRECO_USADO = "span.a-price-whole"
SELETOR_FRACAO_PRECO = "span.a-price-fraction"
SELETOR_INDICADOR_USADO = "span.a-size-base.a-color-secondary"
SELETOR_PROXIMA_PAGINA = "a.s-pagination-next"

USED_PRODUCTS_LINK = "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011&rh=n%3A24669725011&s=popularity-rank&fs=true&page=1&qid=1747998790&xpid=M2soDZTyDMNhF&ref=sr_pg_1"

CATEGORIES = [
    {"name": "Amazon Usados - Warehouse", "safe_name": "Amazon_Usados_Warehouse", "url": USED_PRODUCTS_LINK},
]

MIN_DESCONTO_USADOS_STR = os.getenv("MIN_DESCONTO_PERCENTUAL_USADOS", "40").strip()
try:
    MIN_DESCONTO_USADOS = int(MIN_DESCONTO_USADOS_STR)
    if not (0 <= MIN_DESCONTO_USADOS <= 100):
        MIN_DESCONTO_USADOS = 40
except ValueError:
    MIN_DESCONTO_USADOS = 40
logger.info(f"Desconto mínimo para notificação de usados: {MIN_DESCONTO_USADOS}%")

USAR_HISTORICO_STR = os.getenv("USAR_HISTORICO_GLOBAL_USADOS", "true").strip().lower()
USAR_HISTORICO = USAR_HISTORICO_STR == "true"
logger.info(f"Usar histórico para produtos usados: {USAR_HISTORICO}")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_IDS_STR = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_CHAT_IDS_LIST = [chat_id.strip() for chat_id in TELEGRAM_CHAT_IDS_STR.split(',') if chat_id.strip()]

MAX_PAGINAS_POR_LINK_GLOBAL = 10
HISTORY_DIR_BASE = "history_files_usados"
DEBUG_LOGS_DIR_BASE = "debug_logs_usados"
GLOBAL_HISTORY_FILENAME = "price_history_USADOS_GLOBAL.json"

os.makedirs(HISTORY_DIR_BASE, exist_ok=True)
os.makedirs(DEBUG_LOGS_DIR_BASE, exist_ok=True)

def iniciar_driver_sync_worker(logger, driver_path=None):
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")  # Novo headless para melhor compatibilidade
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920x1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.7049.114 Safari/537.36")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")  # Evita detecção de bot
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])

    if driver_path:
        service = Service(driver_path)
    else:
        service = Service(ChromeDriverManager().install())

    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_page_load_timeout(30)  # Timeout de 30 segundos para carregamento da página
    return driver

async def send_telegram_message_async(bot, chat_id, message, parse_mode, logger):
    try:
        await bot.send_message(chat_id=chat_id, text=message, parse_mode=parse_mode)
        logger.info(f"Mensagem enviada para {chat_id}")
        return True
    except TelegramError as e:
        logger.error(f"Erro ao enviar mensagem para {chat_id}: {e}")
        return False

def escape_md(text):
    return re.sub(r'([_\*\[\]\(\)~`>#+\-=|{}.!])', r'\\1', text)

def get_price_from_element(element, logger):
    try:
        price_whole = element.find_element(By.CSS_SELECTOR, SELETOR_PRECO_USADO).text
        price_fraction = element.find_element(By.CSS_SELECTOR, SELETOR_FRACAO_PRECO).text
        raw_price = f"{price_whole}.{price_fraction}"
        cleaned = re.sub(r'[^\d.]', '', raw_price)
        return float(cleaned)
    except Exception as e:
        logger.error(f"Erro ao obter preço: {e}")
        return None

def load_history():
    history_path = os.path.join(HISTORY_DIR_BASE, GLOBAL_HISTORY_FILENAME)
    if os.path.exists(history_path):
        try:
            with open(history_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Erro ao carregar histórico: {e}")
    return {}

def save_history(history):
    history_path = os.path.join(HISTORY_DIR_BASE, GLOBAL_HISTORY_FILENAME)
    try:
        with open(history_path, 'w', encoding='utf-8') as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Erro ao salvar histórico: {e}")

def get_url_for_page_worker(base_url, page_number):
    parsed_url = urlparse(base_url)
    query_params = parse_qs(parsed_url.query)
    query_params['page'] = [str(page_number)]
    new_query = urlencode(query_params, doseq=True)
    return urlunparse(parsed_url._replace(query=new_query))

def check_captcha_sync_worker(driver, logger):
    try:
        driver.find_element(By.ID, 'captchacharacters')
        logger.warning("Captcha detectado!")
        return True
    except NoSuchElementException:
        return False

def wait_for_page_load(driver, logger):
    try:
        WebDriverWait(driver, 30).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        logger.info("Página carregada completamente.")
        return True
    except TimeoutException:
        logger.error("Timeout ao esperar o carregamento completo da página.")
        return False

async def process_category(driver, category, history):
    url = category['url']
    logger.info(f"Processando categoria: {category['name']}, URL: {url}")
    bot = Bot(token=TELEGRAM_TOKEN) if TELEGRAM_TOKEN and TELEGRAM_CHAT_IDS_LIST else None

    for page in range(1, MAX_PAGINAS_POR_LINK_GLOBAL + 1):
        page_url = get_url_for_page_worker(url, page)
        logger.info(f"Acessando página {page}: {page_url}")
        
        for attempt in range(3):  # Tentar até 3 vezes em caso de falha
            try:
                driver.get(page_url)
                if not wait_for_page_load(driver, logger):
                    logger.warning(f"Tentativa {attempt + 1} falhou: página não carregou completamente.")
                    time.sleep(2)  # Delay para evitar bloqueios
                    continue

                if check_captcha_sync_worker(driver, logger):
                    logger.error(f"Captcha detectado na página {page}. Tentando novamente após delay.")
                    time.sleep(5)  # Delay maior para captcha
                    continue

                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, SELETOR_ITEM_PRODUTO_USADO))
                )
                items = driver.find_elements(By.CSS_SELECTOR, SELETOR_ITEM_PRODUTO_USADO)
                logger.info(f"Página {page}: Encontrados {len(items)} itens.")

                for item in items:
                    try:
                        # Verificar se é um produto usado
                        try:
                            indicador_usado = item.find_element(By.CSS_SELECTOR, SELETOR_INDICADOR_USADO).text
                            if "usado" not in indicador_usado.lower():
                                continue
                        except NoSuchElementException:
                            continue

                        nome = item.find_element(By.CSS_SELECTOR, SELETOR_NOME_PRODUTO_USADO).text
                        link = item.find_element(By.CSS_SELECTOR, SELETOR_LINK_PRODUTO_USADO).get_attribute('href')
                        preco = get_price_from_element(item, logger)

                        if not preco:
                            logger.warning(f"Preço não encontrado para o produto: {nome}")
                            continue

                        asin = item.get_attribute('data-asin')
                        if not asin:
                            logger.warning(f"ASIN não encontrado para o produto: {nome}")
                            continue

                        # Gerenciar histórico
                        if USAR_HISTORICO:
                            if asin not in history:
                                history[asin] = {'nome': nome, 'precos': [], 'link': link}
                            last_price = history[asin]['precos'][-1]['preco'] if history[asin]['precos'] else None
                            if last_price and preco >= last_price:
                                continue  # Não notificar se o preço não diminuiu
                            history[asin]['precos'].append({
                                'preco': preco,
                                'data': datetime.now().isoformat()
                            })
                            save_history(history)

                        # Enviar notificação
                        if bot:
                            desconto_msg = f"Desconto: {((last_price - preco) / last_price * 100):.2f}%" if last_price else "Novo produto"
                            message = (
                                f"*Produto Usado*: {escape_md(nome)}\n"
                                f"*Preço*: R${preco:.2f}\n"
                                f"*Desconto*: {desconto_msg}\n"
                                f"*Link*: {link}"
                            )
                            for chat_id in TELEGRAM_CHAT_IDS_LIST:
                                await send_telegram_message_async(bot, chat_id, message, ParseMode.MARKDOWN, logger)

                        logger.info(f"Produto: {nome}, Preço: R${preco:.2f}, Link: {link}")

                    except StaleElementReferenceException:
                        logger.warning("Elemento obsoleto encontrado, continuando...")
                        continue
                    except Exception as e:
                        logger.error(f"Erro ao processar item: {e}")
                        continue

                # Verificar se há próxima página
                try:
                    next_page = driver.find_element(By.CSS_SELECTOR, SELETOR_PROXIMA_PAGINA)
                    if 'disabled' in next_page.get_attribute('class'):
                        logger.info("Última página alcançada.")
                        break
                except NoSuchElementException:
                    logger.info("Botão de próxima página não encontrado. Finalizando.")
                    break

                time.sleep(2)  # Delay entre páginas para evitar bloqueios
                break  # Sucesso na tentativa, sair do loop de tentativas

            except TimeoutException:
                logger.warning(f"Timeout ao carregar página {page} na tentativa {attempt + 1}.")
                time.sleep(2)
                continue
            except Exception as e:
                logger.error(f"Erro inesperado na página {page}: {e}")
                time.sleep(2)
                continue

        else:
            logger.error(f"Falha após 3 tentativas para a página {page}. Abortando.")
            break

async def orchestrate_all_usados_scrapes_main_async():
    logger.info("--- INICIANDO ORQUESTRADOR DE SCRAPING DE USADOS ---")
    driver = iniciar_driver_sync_worker(logger)
    history = load_history()

    try:
        for category in CATEGORIES:
            await process_category(driver, category, history)
    finally:
        driver.quit()
        logger.info("--- ORQUESTRADOR DE SCRAPING DE USADOS CONCLUÍDO ---")

if __name__ == "__main__":
    logger.info(f"Orquestrador de USADOS chamado via __main__ (scripts/{os.path.basename(__file__)})")
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_IDS_LIST:
        logger.warning("ALERTA USADOS: Token do Telegram ou Chat IDs não configurados. Notificações desabilitadas.")
    asyncio.run(orchestrate_all_usados_scrapes_main_async())
