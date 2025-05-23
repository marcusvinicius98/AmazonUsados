import os
import re
import logging
import asyncio
import json
import random
import time
import requests
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from datetime import datetime
from fake_useragent import UserAgent

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

# --- Configuração de Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(name)s:%(funcName)s:%(lineno)d] - %(message)s",
    handlers=[logging.StreamHandler()]
)
logging.getLogger("webdriver_manager").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram.bot").setLevel(logging.WARNING)
logging.getLogger("telegram.ext").setLevel(logging.WARNING)

logger = logging.getLogger("SCRAPER_USADOS_GERAL")

# --- Configurações do Scraper ---
SELETOR_ITEM_PRODUTO_USADO = "div.s-result-item.s-asin"
SELETOR_NOME_PRODUTO_USADO = "span.a-size-base-plus.a-color-base.a-text-normal"
SELETOR_PRECO_USADO = "div.s-price-instructions-style a span.a-offscreen"
SELETOR_INDICADOR_USADO = "div.s-price-instructions-style a span[contains(text(), 'usado')]"
SELETOR_PROXIMA_PAGINA = "a.s-pagination-item.s-pagination-next[href*='page=']"

URL_GERAL_USADOS_BASE = (
    "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011"
    "&rh=n%3A24669725011&s=popularity-rank&fs=true&xpid=71AiW8sVquI1l"
)
NOME_FLUXO_GERAL = "Amazon Quase Novo (Geral)"

MIN_DESCONTO_USADOS_STR = os.getenv("MIN_DESCONTO_PERCENTUAL_USADOS", "40").strip()
try:
    MIN_DESCONTO_USADOS = int(MIN_DESCONTO_USADOS_STR)
    if not (0 <= MIN_DESCONTO_USADOS <= 100):
        logger.warning(f"MIN_DESCONTO_USADOS ({MIN_DESCONTO_USADOS}%) fora do intervalo. Usando 40%.")
        MIN_DESCONTO_USADOS = 40
except ValueError:
    logger.warning(f"Valor inválido para MIN_DESCONTO_PERCENTUAL_USADOS ('{MIN_DESCONTO_USADOS_STR}'). Usando 40%.")
    MIN_DESCONTO_USADOS = 40
logger.info(f"Desconto mínimo para notificação de usados: {MIN_DESCONTO_USADOS}%")

USAR_HISTORICO_STR = os.getenv("USAR_HISTORICO_USADOS", "true").strip().lower()
USAR_HISTORICO = USAR_HISTORICO_STR == "true"
logger.info(f"Usar histórico para produtos usados: {USAR_HISTORICO}")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_IDS_STR = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_CHAT_IDS_LIST = [chat_id.strip() for chat_id in TELEGRAM_CHAT_IDS_STR.split(',') if chat_id.strip()]

MAX_PAGINAS_POR_LINK_GLOBAL = int(os.getenv("MAX_PAGINAS_USADOS_GERAL", "500"))
logger.info(f"Máximo de páginas para busca geral de usados: {MAX_PAGINAS_POR_LINK_GLOBAL}")

HISTORY_DIR_BASE = "history_files_usados"
DEBUG_LOGS_DIR_BASE = "debug_logs_usados"
HISTORY_FILENAME_USADOS_GERAL = "price_history_USADOS_GERAL.json"
DEBUG_LOG_FILENAME_BASE_USADOS_GERAL = "scrape_debug_usados_geral"

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

def iniciar_driver_sync_worker(current_run_logger, driver_path=None):
    current_run_logger.info("Iniciando configuração do WebDriver...")
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    
    ua = UserAgent()
    user_agent = ua.random
    chrome_options.add_argument(f"user-agent={user_agent}")
    current_run_logger.info(f"User-Agent: {user_agent}")
    
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--mute-audio")
    chrome_options.add_argument("--no-first-run")
    chrome_options.add_argument("--disable-webgl")
    chrome_options.add_argument("--disable-webrtc")
    chrome_options.add_argument("--disable-features=WebRtcHideLocalIpsWithMdns,PrivacySandboxSettings4,OptimizationHints,InterestGroupStorage")
    chrome_options.add_argument("--lang=pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7")
    
    proxy_host = os.getenv("PROXY_HOST")
    proxy_port = os.getenv("PROXY_PORT")
    if proxy_host and proxy_port:
        chrome_options.add_argument(f'--proxy-server=http://{proxy_host}:{proxy_port}')
        current_run_logger.info(f"Usando proxy: http://{proxy_host}:{proxy_port}")
    
    current_run_logger.info(f"Opções do Chrome configuradas: {chrome_options.arguments}")

    service = None
    try:
        if driver_path and os.path.exists(driver_path):
            current_run_logger.info(f"Usando Service com driver_path: {driver_path}")
            service = Service(driver_path)
        else:
            if driver_path:
                current_run_logger.warning(f"Driver_path '{driver_path}' fornecido mas não encontrado. Usando WebDriverManager.")
            current_run_logger.info("Usando Service com ChromeDriverManager para instalar/gerenciar o ChromeDriver.")
            path_from_manager = ChromeDriverManager().install()
            service = Service(path_from_manager)
            current_run_logger.info(f"ChromeDriverManager configurou driver em: {path_from_manager}")
    except Exception as e:
        current_run_logger.error(f"Erro ao configurar o Service do ChromeDriver: {e}", exc_info=True)
        raise

    driver = None
    try:
        current_run_logger.info("Tentando instanciar o webdriver.Chrome...")
        driver = webdriver.Chrome(service=service, options=chrome_options)
        current_run_logger.info("WebDriver instanciado com sucesso.")
        page_load_timeout = 120
        driver.set_page_load_timeout(page_load_timeout)
        current_run_logger.info(f"Timeout de carregamento de página definido para {page_load_timeout}s.")
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        })
        current_run_logger.info("Script para ocultar 'navigator.webdriver' configurado para rodar em novos documentos.")
        return driver
    except Exception as e:
        current_run_logger.error(f"Erro ao instanciar ou configurar o WebDriver: {e}", exc_info=True)
        if driver:
            driver.quit()
        raise

async def get_initial_cookies(driver, logger):
    logger.info("Acessando página inicial para obter cookies...")
    try:
        await asyncio.to_thread(driver.get, "https://www.amazon.com.br")
        await asyncio.sleep(random.uniform(3, 5))
        await asyncio.to_thread(wait_for_page_load, driver, logger)
        logger.info("Cookies iniciais obtidos com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao obter cookies iniciais: {e}", exc_info=True)

async def simulate_scroll(driver, logger):
    logger.debug("Simulando rolagem na página...")
    try:
        await asyncio.to_thread(driver.execute_script, "window.scrollTo(0, document.body.scrollHeight);")
        await asyncio.sleep(random.uniform(1, 3))
        await asyncio.to_thread(driver.execute_script, "window.scrollTo(0, 0);")
        await asyncio.sleep(random.uniform(0.5, 1.5))
        logger.debug("Rolagem simulada com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao simular rolagem: {e}", exc_info=True)

async def send_telegram_message_async(bot, chat_id, message, parse_mode, msg_logger):
    msg_logger.debug(f"Tentando enviar mensagem para chat_id: {chat_id}")
    if not bot:
        msg_logger.error(f"[{msg_logger.name}] Instância do Bot não fornecida.")
        return False
    try:
        await bot.send_message(chat_id=chat_id, text=message, parse_mode=parse_mode)
        msg_logger.info(f"[{msg_logger.name}] Notificação Telegram enviada para CHAT_ID {chat_id}.")
        return True
    except TelegramError as e:
        msg_logger.error(f"[{msg_logger.name}] Erro Telegram ao enviar para CHAT_ID {chat_id}: {e.message}", exc_info=False)
        return False
    except Exception as e:
        msg_logger.error(f"[{msg_logger.name}] Erro inesperado ao enviar msg para CHAT_ID {chat_id}: {e}", exc_info=True)
        return False

def escape_md(text):
    return re.sub(r'([_\*\[\]\(\)~`>#+\-=|{}.!])', r'\\\1', str(text))

def get_price_from_element(element, price_logger):
    price_logger.debug("Tentando extrair preço do elemento.")
    try:
        price_el = element.find_element(By.CSS_SELECTOR, SELETOR_PRECO_USADO)
        price_text = price_el.text
        price_logger.debug(f"Texto do preço: '{price_text}'")
        cleaned = re.sub(r'[^\d,]', '', price_text).replace(',', '.')
        final_price = float(cleaned)
        price_logger.debug(f"Preço final: {final_price}")
        return final_price
    except NoSuchElementException:
        price_logger.debug(f"Elemento de preço não encontrado no item.")
        return None
    except ValueError:
        price_logger.warning(f"Erro de valor ao converter preço '{cleaned if 'cleaned' in locals() else 'N/A'}' para float.")
        return None
    except Exception as e:
        price_logger.error(f"Erro inesperado ao obter preço: {e}", exc_info=True)
        return None

def load_history_geral():
    history_path = os.path.join(HISTORY_DIR_BASE, HISTORY_FILENAME_USADOS_GERAL)
    logger.info(f"Tentando carregar histórico de: {history_path}")
    if os.path.exists(history_path):
        try:
            with open(history_path, 'r', encoding='utf-8') as f:
                history_data = json.load(f)
            logger.info(f"Histórico carregado. {len(history_data)} ASINs no histórico.")
            return history_data
        except Exception as e:
            logger.error(f"Erro ao carregar/decodificar histórico de '{history_path}': {e}. Retornando vazio.", exc_info=True)
            return {}
    else:
        logger.info("Arquivo de histórico não encontrado. Retornando histórico vazio.")
        return {}

def save_history_geral(history):
    history_path = os.path.join(HISTORY_DIR_BASE, HISTORY_FILENAME_USADOS_GERAL)
    logger.info(f"Tentando salvar histórico ({len(history)} ASINs) em: {history_path}")
    try:
        with open(history_path, 'w', encoding='utf-8') as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
        logger.info("Histórico salvo com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao salvar histórico em '{history_path}': {e}", exc_info=True)

def get_url_for_page_worker(base_url, page_number, current_run_logger):
    current_run_logger.debug(f"Gerando URL para página {page_number} a partir de base: {base_url}")
    parsed_url = urlparse(base_url)
    query_params = parse_qs(parsed_url.query)
    query_params['page'] = [str(page_number)]
    qid_time = int(time.time() * 1000)
    query_params['qid'] = [str(qid_time)]
    query_params['ref'] = [f'sr_pg_{page_number}']
    new_query = urlencode(query_params, doseq=True)
    final_url = urlunparse(parsed_url._replace(query=new_query))
    current_run_logger.debug(f"URL da página gerada: {final_url}")
    return final_url

def check_captcha_sync_worker(driver, current_run_logger):
    current_run_logger.debug("Verificando a presença de CAPTCHA.")
    try:
        WebDriverWait(driver, 5).until(EC.any_of(
            EC.presence_of_element_located((By.CSS_SELECTOR, "form[action*='captcha'] img")),
            EC.presence_of_element_located((By.XPATH, "//h4[contains(text(), 'Insira os caracteres')]")),
            EC.presence_of_element_located((By.XPATH, "//h4[contains(text(), 'Digite os caracteres que você vê abaixo')]")),
            EC.presence_of_element_located((By.CSS_SELECTOR, "iframe[src*='captcha']"))
        ))
        current_run_logger.warning(f"CAPTCHA detectado! URL: {driver.current_url}")
        timestamp_captcha = datetime.now().strftime('%Y%m%d_%H%M%S')
        screenshot_path = os.path.join(DEBUG_LOGS_DIR_BASE, f"captcha_usados_geral_{timestamp_captcha}.png")
        html_path = os.path.join(DEBUG_LOGS_DIR_BASE, f"captcha_usados_geral_{timestamp_captcha}.html")
        try:
            driver.save_screenshot(screenshot_path)
            current_run_logger.info(f"Screenshot do CAPTCHA salvo em: {screenshot_path}")
            with open(html_path, "w", encoding="utf-8") as f_html:
                f_html.write(driver.page_source)
            current_run_logger.info(f"HTML do CAPTCHA salvo em: {html_path}")
        except Exception as e_save_captcha:
            current_run_logger.error(f"Erro ao salvar debug do CAPTCHA: {e_save_captcha}")
        return True
    except (TimeoutException, NoSuchElementException):
        current_run_logger.debug("Nenhum CAPTCHA detectado.")
        return False
    except Exception as e:
        current_run_logger.error(f"Erro inesperado ao verificar CAPTCHA: {e}", exc_info=True)
        return False

def check_amazon_error_page_sync_worker(driver, current_run_logger):
    current_run_logger.debug("Verificando se é página de erro da Amazon ('Algo deu errado').")
    try:
        page_title = driver.title.lower()
        if "algo deu errado" in page_title or "sorry" in page_title:
            current_run_logger.warning(f"Página de erro da Amazon detectada! Título: {page_title}")
            timestamp_error = datetime.now().strftime('%Y%m%d_%H%M%S')
            screenshot_path = os.path.join(DEBUG_LOGS_DIR_BASE, f"error_usados_geral_{timestamp_error}.png")
            html_path = os.path.join(DEBUG_LOGS_DIR_BASE, f"error_usados_geral_{timestamp_error}.html")
            try:
                driver.save_screenshot(screenshot_path)
                current_run_logger.info(f"Screenshot da página de erro salvo em: {screenshot_path}")
                with open(html_path, "w", encoding="utf-8") as f_html:
                    f_html.write(driver.page_source)
                current_run_logger.info(f"HTML da página de erro salvo em: {html_path}")
            except Exception as e_save_error:
                current_run_logger.error(f"Erro ao salvar debug da página de erro: {e_save_error}")
            return True
        current_run_logger.debug("Nenhuma página de erro detectada.")
        return False
    except Exception as e:
        current_run_logger.error(f"Erro ao verificar página de erro: {e}", exc_info=True)
        return False

def wait_for_page_load(driver, logger, timeout=120):
    logger.debug(f"Aguardando carregamento completo da página (timeout={timeout}s)...")
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        logger.info("Página carregada completamente (document.readyState is 'complete').")
    except TimeoutException:
        logger.warning("Timeout ao esperar carregamento completo da página.")
    except Exception as e:
        logger.error(f"Erro ao esperar carregamento da página: {e}", exc_info=True)

def check_url_status(url, logger, max_retries=3, backoff_factor=2):
    logger.debug(f"Verificando status HTTP da URL: {url}")
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.head(url, timeout=10, allow_redirects=True)
            logger.info(f"Status HTTP da URL: {response.status_code}")
            if response.status_code == 200:
                return response.status_code
            elif response.status_code == 503:
                logger.warning(f"URL retornou status 503. Tentativa {attempt}/{max_retries}.")
                if attempt < max_retries:
                    sleep_time = backoff_factor ** attempt
                    logger.info(f"Aguardando {sleep_time}s antes da próxima tentativa...")
                    time.sleep(sleep_time)
            else:
                logger.warning(f"URL retornou status inesperado: {response.status_code}")
                return response.status_code
        except requests.RequestException as e:
            logger.error(f"Erro ao verificar status da URL: {e}")
            if attempt < max_retries:
                sleep_time = backoff_factor ** attempt
                logger.info(f"Aguardando {sleep_time}s antes da próxima tentativa...")
                time.sleep(sleep_time)
    logger.error(f"Falha ao obter status HTTP após {max_retries} tentativas.")
    return None

async def process_used_products_geral_async(driver, base_url, nome_fluxo, history, logger, max_paginas=MAX_PAGINAS_POR_LINK_GLOBAL):
    logger.info(f"--- Iniciando processamento para: {nome_fluxo} --- URL base: {base_url} ---")
    total_produtos_usados = []
    pagina_atual = 1
    max_tentativas = 3

    while pagina_atual <= max_paginas:
        url_pagina = get_url_for_page_worker(base_url, pagina_atual, logger)
        logger.info(f"[{nome_fluxo}] Carregando Página: {pagina_atual}/{max_paginas}, URL: {url_pagina}")

        status_code = check_url_status(url_pagina, logger)
        if status_code != 200:
            logger.warning(f"URL retornou status não-200 ({status_code}). Tentando carregar mesmo assim.")

        for tentativa in range(1, max_tentativas + 1):
            logger.info(f"[{nome_fluxo}] Tentativa {tentativa}/{max_tentativas} de carregar URL.")
            try:
                await asyncio.to_thread(driver.get, url_pagina)
                await asyncio.sleep(random.uniform(2, 5))
                await asyncio.to_thread(wait_for_page_load, driver, logger)
                await simulate_scroll(driver, logger)

                if check_captcha_sync_worker(driver, logger):
                    logger.error(f"[{nome_fluxo}] CAPTCHA detectado na página {pagina_atual}. Interrompendo.")
                    break
                if check_amazon_error_page_sync_worker(driver, logger):
                    logger.error(f"[{nome_fluxo}] Página de erro da Amazon detectada na página {pagina_atual}. Tentando novamente.")
                    if tentativa < max_tentativas:
                        await asyncio.sleep(random.uniform(5, 10))
                        continue
                    else:
                        logger.error(f"[{nome_fluxo}] Falha após {max_tentativas} tentativas. Interrompendo.")
                        break

                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, SELETOR_ITEM_PRODUTO_USADO))
                    )
                    logger.info(f"Seletor de item encontrado na página {pagina_atual}.")
                except TimeoutException:
                    logger.warning(f"Nenhum item encontrado na página {pagina_atual} com seletor: {SELETOR_ITEM_PRODUTO_USADO}")
                    break

                items = driver.find_elements(By.CSS_SELECTOR, SELETOR_ITEM_PRODUTO_USADO)
                logger.info(f"Página {pagina_atual}: Encontrados {len(items)} elementos com seletor principal.")

                for idx, item in enumerate(items, 1):
                    try:
                        item_logger = logging.getLogger(f"{logger.name}.Item_{pagina_atual}_{idx}")
                        item_logger.debug("Processando item...")

                        try:
                            used_indicator = item.find_element(By.CSS_SELECTOR, SELETOR_INDICADOR_USADO)
                            item_logger.debug(f"Indicador 'usado' encontrado: {used_indicator.text}")
                        except NoSuchElementException:
                            item_logger.debug("Item não identificado como 'usado'. Ignorando.")
                            continue

                        try:
                            nome_element = item.find_element(By.CSS_SELECTOR, SELETOR_NOME_PRODUTO_USADO)
                            nome = nome_element.text.strip()
                            item_logger.debug(f"Nome do produto: {nome}")
                        except NoSuchElementException:
                            item_logger.debug("Nome do produto não encontrado. Ignorando.")
                            continue

                        try:
                            link_element = item.find_element(By.CSS_SELECTOR, "a.a-link-normal.s-no-outline")
                            link = link_element.get_attribute("href")
                            item_logger.debug(f"Link do produto: {link}")
                        except NoSuchElementException:
                            item_logger.debug("Link do produto não encontrado. Ignorando.")
                            continue

                        asin_match = re.search(r'/dp/([A-Z0-9]{10})', link)
                        asin = asin_match.group(1) if asin_match else None
                        if not asin:
                            item_logger.debug("ASIN não encontrado no link. Ignorando.")
                            continue
                        item_logger.debug(f"ASIN: {asin}")

                        price = get_price_from_element(item, item_logger)
                        if price is None:
                            item_logger.debug("Preço não encontrado ou inválido. Ignorando.")
                            continue

                        produto = {
                            "nome": nome,
                            "asin": asin,
                            "link": link,
                            "preco_usado": price,
                            "timestamp": datetime.now().isoformat(),
                            "fluxo": nome_fluxo
                        }

                        if USAR_HISTORICO:
                            preco_historico = history.get(asin, {}).get("preco_usado")
                            if preco_historico and preco_historico <= price:
                                item_logger.debug(f"Preço atual ({price}) não é menor que o histórico ({preco_historico}). Ignorando.")
                                continue
                            history[asin] = produto
                            save_history_geral(history)

                        total_produtos_usados.append(produto)
                        item_logger.info(f"Produto 'usado' qualificado adicionado: {nome} | Preço: R${price:.2f}")

                        if bot_instance_global and TELEGRAM_CHAT_IDS_LIST:
                            message = (
                                f"*Oferta {nome_fluxo}*\n"
                                f"📦 *{escape_md(nome)}*\n"
                                f"💵 Preço Usado: R${price:.2f}\n"
                                f"🔗 [Comprar]({link})\n"
                                f"🕒 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                            )
                            for chat_id in TELEGRAM_CHAT_IDS_LIST:
                                await send_telegram_message_async(
                                    bot_instance_global, chat_id, message, ParseMode.MARKDOWN_V2, item_logger
                                )

                    except StaleElementReferenceException:
                        item_logger.warning("Elemento tornou-se obsoleto durante o processamento. Ignorando.")
                        continue
                    except Exception as e:
                        item_logger.error(f"Erro ao processar item: {e}", exc_info=True)
                        continue

                logger.info(f"Página {pagina_atual}: {len(total_produtos_usados)} produtos 'usados' processados até agora.")

                try:
                    next_button = WebDriverWait(driver, 15).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, SELETOR_PROXIMA_PAGINA))
                    )
                    logger.info(f"Botão 'Próxima Página' encontrado na página {pagina_atual}.")
                    pagina_atual += 1
                    await asyncio.sleep(random.uniform(3, 7))
                    continue
                except TimeoutException:
                    logger.info(f"Botão 'Próxima Página' não encontrado na página {pagina_atual}. Fim da busca.")
                    break
                except Exception as e:
                    logger.error(f"Erro ao buscar botão 'Próxima Página': {e}", exc_info=True)
                    break

            except Exception as e:
                logger.error(f"Erro ao carregar página {pagina_atual}: {e}", exc_info=True)
                if tentativa < max_tentativas:
                    await asyncio.sleep(random.uniform(5, 10))
                    continue
                else:
                    logger.error(f"Falha após {max_tentativas} tentativas na página {pagina_atual}. Interrompendo.")
                    break

        break

    logger.info(
        f"--- Concluído Fluxo: {nome_fluxo}. Páginas processadas: {pagina_atual-1}. "
        f"Total de produtos 'usados' qualificados encontrados: {len(total_produtos_usados)} ---"
    )
    return total_produtos_usados

async def run_usados_geral_scraper_async():
    logger.info(f"--- [SCRAPER INÍCIO] Fluxo: {NOME_FLUXO_GERAL} ---")
    driver = None
    try:
        logger.info("Tentando iniciar o driver Selenium...")
        driver = iniciar_driver_sync_worker(logger)
        logger.info("Driver Selenium iniciado com sucesso.")
        await get_initial_cookies(driver, logger)
        history = load_history_geral() if USAR_HISTORICO else {}
        await process_used_products_geral_async(driver, URL_GERAL_USADOS_BASE, NOME_FLUXO_GERAL, history, logger)
        logger.info("Processamento do fluxo de usados geral concluído.")
    except Exception as e:
        logger.error(f"Erro no fluxo geral de usados: {e}", exc_info=True)
    finally:
        if driver:
            logger.info("Tentando fechar o driver Selenium...")
            try:
                driver.quit()
                logger.info("Driver Selenium fechado.")
            except Exception as e:
                logger.error(f"Erro ao fechar o driver: {e}", exc_info=True)
        logger.info(f"--- [SCRAPER FIM] Fluxo: {NOME_FLUXO_GERAL} ---")

if __name__ == "__main__":
    asyncio.run(run_usados_geral_scraper_async())
