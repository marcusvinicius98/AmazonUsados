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
CONCURRENCY_LIMIT = 5  # Limite de scrapes simultâneos

os.makedirs(HISTORY_DIR_BASE, exist_ok=True)
os.makedirs(DEBUG_LOGS_DIR_BASE, exist_ok=True)

bot_instance_global = None
if TELEGRAM_TOKEN and TELEGRAM_CHAT_IDS_LIST:
    try:
        bot_instance_global = Bot(token=TELEGRAM_TOKEN)
        logger.info(f"Instância global do Bot Telegram criada. IDs de Chat: {TELEGRAM_CHAT_IDS_LIST}")
    except Exception as e:
        logger.error(f"Falha ao inicializar Bot global: {e}")

def iniciar_driver_sync_worker(logger, driver_path=None):
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920x1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.7049.114 Safari/537.36")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    if driver_path:
        service = Service(driver_path)
    else:
        service = Service(ChromeDriverManager().install())

    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_page_load_timeout(30)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
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
    query_params['qid'] = [str(int(asyncio.get_event_loop().time() * 1000))]
    query_params['ref'] = [f'sr_pg_{page_number}']
    new_query = urlencode(query_params, doseq=True)
    return urlunparse(parsed_url._replace(query=new_query))

def check_captcha_sync_worker(driver, logger):
    try:
        WebDriverWait(driver, 3).until(EC.any_of(
            EC.presence_of_element_located((By.CSS_SELECTOR, "form[action*='captcha'] img")),
            EC.presence_of_element_located((By.XPATH, "//h4[contains(text(), 'Insira os caracteres')]")),
            EC.presence_of_element_located((By.CSS_SELECTOR, "iframe[src*='captcha']"))
        ))
        logger.warning(f"CAPTCHA detectado! URL: {driver.current_url}")
        return True
    except (TimeoutException, NoSuchElementException):
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

async def process_category(
    driver, url_inicial_categoria, nome_categoria, category_specific_logger,
    history, min_desconto, bot, chat_ids_list
):
    parsed_initial_url = urlparse(url_inicial_categoria)
    query_params = parse_qs(parsed_initial_url.query)
    query_params.pop('page', None)
    query_params.pop('ref', None)
    query_params.pop('qid', None)
    cleaned_query_string = urlencode(query_params, doseq=True)
    base_url_para_paginacao = urlunparse(parsed_initial_url._replace(query=cleaned_query_string))

    category_specific_logger.info(f"--- Processando Categoria: {nome_categoria} --- URL base para paginação: {base_url_para_paginacao} ---")
    paginas_sem_produtos_consecutivas = 0
    loop_broken_flag = False

    for page in range(1, MAX_PAGINAS_POR_LINK_GLOBAL + 1):
        page_url = get_url_for_page_worker(base_url_para_paginacao, page)
        category_specific_logger.info(f"[{nome_categoria}] Processando URL: {page_url} (Página: {page}/{MAX_PAGINAS_POR_LINK_GLOBAL})")

        for attempt in range(3):
            try:
                await asyncio.to_thread(driver.get, page_url)
                await asyncio.sleep(4)
                if not await asyncio.to_thread(wait_for_page_load, driver, category_specific_logger):
                    category_specific_logger.warning(f"[{nome_categoria}] Tentativa {attempt + 1} falhou: página não carregou completamente.")
                    await asyncio.sleep(2)
                    continue

                if await asyncio.to_thread(check_captcha_sync_worker, driver, category_specific_logger):
                    category_specific_logger.error(f"[{nome_categoria}] Captcha detectado na página {page}. Tentando novamente após delay.")
                    await asyncio.sleep(5)
                    continue

                await asyncio.to_thread(WebDriverWait(driver, 30).until, EC.presence_of_element_located((By.CSS_SELECTOR, SELETOR_ITEM_PRODUTO_USADO)))
                items = await asyncio.to_thread(driver.find_elements, By.CSS_SELECTOR, SELETOR_ITEM_PRODUTO_USADO)
                category_specific_logger.info(f"[{nome_categoria}] Página {page}: Encontrados {len(items)} itens.")

                organic_items = []
                for item in items:
                    try:
                        current_asin = await asyncio.to_thread(item.get_attribute, 'data-asin')
                        if not current_asin:
                            continue

                        is_sponsored = False
                        xpath_sponsored = ".//span[contains(translate(normalize-space(.), 'PATROCINADOabcdefghijklmnopqrstuvwxyz', 'patrocinado'), 'patrocinado')] | .//div[@data-cy='sponsored-label'] | .//a[@data-a-Qualifier='sp'] | .//span[text()='Sponsored'] | .//span[contains(@class, 'sponsored')]"
                        sponsored_indicators = await asyncio.to_thread(item.find_elements, By.XPATH, xpath_sponsored)
                        if sponsored_indicators:
                            for ind in sponsored_indicators:
                                if await asyncio.to_thread(ind.is_displayed):
                                    is_sponsored = True
                                    break
                            if is_sponsored:
                                category_specific_logger.debug(f"[{nome_categoria}] Item patrocinado ignorado (ASIN: {current_asin}).")
                                continue
                        organic_items.append(item)
                    except StaleElementReferenceException:
                        continue

                category_specific_logger.info(f"[{nome_categoria}] {len(items)} itens na página, {len(organic_items)} orgânicos na pág {page}.")
                if not organic_items:
                    category_specific_logger.warning(f"[{nome_categoria}] Nenhum produto orgânico encontrado na pág {page}.")
                    paginas_sem_produtos_consecutivas += 1
                    if paginas_sem_produtos_consecutivas >= 2 and page > 1:
                        loop_broken_flag = True
                        break
                    continue
                else:
                    paginas_sem_produtos_consecutivas = 0

                for item in organic_items:
                    try:
                        try:
                            indicador_usado = await asyncio.to_thread(item.find_element, By.CSS_SELECTOR, SELETOR_INDICADOR_USADO)
                            if "usado" not in (await asyncio.to_thread(indicador_usado.text)).lower():
                                continue
                        except NoSuchElementException:
                            continue

                        nome = (await asyncio.to_thread(item.find_element, By.CSS_SELECTOR, SELETOR_NOME_PRODUTO_USADO).text))[:150]
                        link = await asyncio.to_thread(item.find_element, By.CSS_SELECTOR, SELETOR_LINK_PRODUTO_USADO).get_attribute('href'))
                        preco = await asyncio.to_thread(get_price_from_element, item, category_specific_logger)
                        asin = await asyncio.to_thread(item.get_attribute, 'data-asin')

                        if not preco or not asin:
                            category_specific_logger.warning(f"[{nome_categoria}] Preço ou ASIN não encontrado para o produto: {nome}")
                            continue

                        base_amazon_url = "https://www.amazon.com.br"
                        link = f"{base_amazon_url}/dp/{asin}"

                        if USAR_HISTORICO:
                            if asin not in history:
                                history[asin] = {'nome': nome, 'precos': [], 'link': link, 'category_last_seen': nome_categoria}
                            last_price = history[asin]['precos'][-1]['preco'] if history[asin]['precos'] else None
                            if last_price and preco >= last_price:
                                continue
                            history[asin]['precos'].append({
                                'preco': preco,
                                'data': datetime.now().isoformat()
                            })
                            history[asin]['category_last_seen'] = nome_categoria
                            save_history(history)

                        if bot:
                            desconto_msg = f"Desconto: {((last_price - preco) / last_price * 100):.2f}%" if last_price else "Novo produto"
                            message = (
                                f"*Produto Usado* ({escape_md(nome_categoria)}): {escape_md(nome)}\n"
                                f"*Preço*: R${preco:.2f}\n"
                                f"*Desconto*: {desconto_msg}\n"
                                f"*Link*: {link}"
                            )
                            for chat_id in chat_ids_list:
                                await send_telegram_message_async(bot, chat_id, message, ParseMode.MARKDOWN, category_specific_logger)

                        category_specific_logger.info(f"[{nome_categoria}] Produto: {nome[:30]}..., Preço: R${preco:.2f}, Link: {link}")

                    except StaleElementReferenceException:
                        category_specific_logger.warning(f"[{nome_categoria}] Elemento obsoleto encontrado, continuando...")
                        continue
                    except Exception as e:
                        category_specific_logger.error(f"[{nome_categoria}] Erro ao processar item: {e}")
                        continue

                try:
                    next_page = await asyncio.to_thread(driver.find_element, By.CSS_SELECTOR, SELETOR_PROXIMA_PAGINA)
                    if 'disabled' in (await asyncio.to_thread(next_page.get_attribute, 'class')):
                        category_specific_logger.info(f"[{nome_categoria}] Última página alcançada.")
                        break
                except NoSuchElementException:
                    category_specific_logger.info(f"[{nome_categoria}] Botão de próxima página não encontrado. Finalizando.")
                    break

                await asyncio.sleep(3)
                break

            except TimeoutException:
                category_specific_logger.warning(f"[{nome_categoria}] Timeout ao carregar página {page} na tentativa {attempt + 1}.")
                await asyncio.sleep(2)
                continue
            except Exception as e:
                category_specific_logger.error(f"[{nome_categoria}] Erro inesperado na página {page}: {e}")
                await asyncio.sleep(2)
                continue

        if loop_broken_flag:
            break

    category_specific_logger.info(f"--- Concluída Categoria: {nome_categoria} ---")

async def scrape_category_worker_async(
    category_details, min_desconto, bot, chat_ids_list, semaphore, concurrency_limit, history, driver_path=None
):
    cat_name = category_details["name"]
    cat_safe_name = category_details["safe_name"]
    cat_url = category_details["url"]

    worker_logger = logging.getLogger(f"worker.{cat_safe_name}")
    if not worker_logger.handlers:
        log_filename = os.path.join(DEBUG_LOGS_DIR_BASE, f"scrape_debug_{cat_safe_name}.log")
        file_handler = logging.FileHandler(log_filename, encoding="utf-8", mode="w")
        file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - [%(module)s:%(funcName)s:%(lineno)d] - %(message)s"))
        worker_logger.addHandler(file_handler)
        worker_logger.setLevel(logging.INFO)
        worker_logger.propagate = False

    driver = None
    async with semaphore:
        slots_ocupados = concurrency_limit - semaphore._value
        logger.info(f"Semáforo ADQUIRIDO por: '{cat_name}'. Slots ocupados: {slots_ocupados}/{concurrency_limit}.")
        worker_logger.info(f"--- [WORKER INÍCIO] Categoria: {cat_name} ---")

        try:
            driver = await asyncio.to_thread(iniciar_driver_sync_worker, worker_logger, driver_path)
            worker_logger.info(f"Driver Selenium iniciado para {cat_name}.")
            await process_category(
                driver, cat_url, cat_name, worker_logger, history,
                min_desconto, bot, chat_ids_list
            )
        except Exception as e:
            worker_logger.error(f"Erro principal no worker para {cat_name}: {e}")
        finally:
            if driver:
                await asyncio.to_thread(driver.quit)
                worker_logger.info(f"Driver Selenium para {cat_name} fechado.")
            worker_logger.info(f"--- [WORKER FIM] Categoria: {cat_name} ---")

        logger.info(f"Worker para '{cat_name}' LIBEROU semáforo.")

async def orchestrate_all_usados_scrapes_main_async():
    logger.info("--- INICIANDO ORQUESTRADOR DE SCRAPING DE USADOS ---")
    history = load_history()
    installed_chromedriver_path = None
    try:
        installed_chromedriver_path = ChromeDriverManager().install()
        logger.info(f"WebDriverManager instalou ChromeDriver em: {installed_chromedriver_path}")
    except Exception as e:
        logger.error(f"Falha ao instalar ChromeDriver: {e}")

    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    logger.info(f"Limite de concorrência definido para: {CONCURRENCY_LIMIT} scrapes simultâneos.")

    tasks = []
    for category in CATEGORIES:
        tasks.append(scrape_category_worker_async(
            category_details=category,
            min_desconto=MIN_DESCONTO_USADOS,
            bot=bot_instance_global,
            chat_ids_list=TELEGRAM_CHAT_IDS_LIST,
            semaphore=semaphore,
            concurrency_limit=CONCURRENCY_LIMIT,
            history=history,
            driver_path=installed_chromedriver_path
        ))

    await asyncio.gather(*tasks, return_exceptions=True)
    if USAR_HISTORICO:
        save_history(history)
    logger.info("--- ORQUESTRADOR DE SCRAPING DE USADOS CONCLUÍDO ---")

if __name__ == "__main__":
    logger.info(f"Orquestrador de USADOS chamado via __main__ (scripts/{os.path.basename(__file__)})")
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_IDS_LIST:
        logger.warning("ALERTA USADOS: Token do Telegram ou Chat IDs não configurados. Notificações desabilitadas.")
    asyncio.run(orchestrate_all_usados_scrapes_main_async())
