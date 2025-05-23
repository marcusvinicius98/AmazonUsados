import os
import re
import logging
import asyncio
import json
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(module)s:%(funcName)s:%(lineno)d] - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("ORCHESTRATOR_USADOS")
logger.propagate = False
logger.setLevel(logging.INFO)

# --- CONFIGURAÇÕES IMPORTANTES ---

SELETOR_ITEM_PRODUTO_USADO = "div.s-result-item[data-asin]"
SELETOR_NOME_PRODUTO_USADO = "span.a-size-medium.a-color-base.a-text-normal"
SELETOR_LINK_PRODUTO_USADO = "a.a-link-normal.s-no-outline"
SELETOR_PRECO_USADO_DENTRO_DO_ITEM = "span.a-offscreen"
SELETOR_INDICADOR_USADO_TEXTO = "span"

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
logger.info(f"Desconto mínimo para notificação de usados (sobre o último visto): {MIN_DESCONTO_USADOS}%")

USAR_HISTORICO_STR = os.getenv("USAR_HISTORICO_GLOBAL_USADOS", "true").strip().lower()
USAR_HISTORICO = USAR_HISTORICO_STR == "true"
logger.info(f"Usar histórico para produtos usados: {USAR_HISTORICO}")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_IDS_STR = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_CHAT_IDS_LIST = []
if TELEGRAM_CHAT_IDS_STR:
    TELEGRAM_CHAT_IDS_LIST = [chat_id.strip() for chat_id in TELEGRAM_CHAT_IDS_STR.split(',') if chat_id.strip()]

MAX_PAGINAS_POR_LINK_GLOBAL = 10
HISTORY_DIR_BASE = "history_files_usados"
DEBUG_LOGS_DIR_BASE = "debug_logs_usados"
GLOBAL_HISTORY_FILENAME = "price_history_USADOS_GLOBAL.json"

os.makedirs(HISTORY_DIR_BASE, exist_ok=True)
os.makedirs(DEBUG_LOGS_DIR_BASE, exist_ok=True)

bot_instance_global = None

def iniciar_driver_sync_worker(logger, driver_path=None):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    if driver_path:
        service = Service(driver_path)
    else:
        service = Service(ChromeDriverManager().install())

    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def send_telegram_message_async(bot, chat_id, message, parse_mode, logger):
    try:
        asyncio.run(bot.send_message(chat_id=chat_id, text=message, parse_mode=parse_mode))
        logger.info(f"Mensagem enviada para {chat_id}")
        return True
    except TelegramError as e:
        logger.error(f"Erro ao enviar mensagem para {chat_id}: {e}")
        return False

def escape_md(text):
    return re.sub(r'([_\*\[\]\(\)~`>#+\-=|{}.!])', r'\\\\1', text)

def get_price_from_direct_text(element, selector, logger):
    try:
        price_span = element.find_element(By.CSS_SELECTOR, selector)
        raw_text = price_span.text
        cleaned = re.sub(r'[^\d,]', '', raw_text).replace(',', '.')
        return float(cleaned)
    except Exception as e:
        logger.error(f"Erro ao obter preço: {e}")
        return None

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

async def orchestrate_all_usados_scrapes_main_async():
    logger.info("--- INICIANDO ORQUESTRADOR DE SCRAPING DE USADOS ---")
    driver = iniciar_driver_sync_worker(logger)

    for category in CATEGORIES:
        url = category['url']
        logger.info(f"Processando categoria: {category['name']}, URL: {url}")
        driver.get(url)

        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, SELETOR_ITEM_PRODUTO_USADO))
            )
            items = driver.find_elements(By.CSS_SELECTOR, SELETOR_ITEM_PRODUTO_USADO)
            logger.info(f"Encontrados {len(items)} itens.")
            for item in items:
                nome = item.find_element(By.CSS_SELECTOR, SELETOR_NOME_PRODUTO_USADO).text
                link = item.find_element(By.CSS_SELECTOR, SELETOR_LINK_PRODUTO_USADO).get_attribute('href')
                preco = get_price_from_direct_text(item, SELETOR_PRECO_USADO_DENTRO_DO_ITEM, logger)
                logger.info(f"Produto: {nome}, Preço: {preco}, Link: {link}")
        except TimeoutException:
            logger.warning("Timeout ao buscar produtos.")

    driver.quit()
    logger.info("--- ORQUESTRADOR DE SCRAPING DE USADOS CONCLUÍDO ---")

if __name__ == "__main__":
    logger.info(f"Orquestrador de USADOS chamado via __main__ (scripts/{os.path.basename(__file__)})")
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_IDS_LIST:
        logger.warning("ALERTA USADOS: Token do Telegram ou Chat IDs não configurados. Notificações desabilitadas.")

    asyncio.run(orchestrate_all_usados_scrapes_main_async())
