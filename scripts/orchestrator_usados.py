import os
import re
import logging
import asyncio
import json
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
SELETOR_PRECO_USADO = "span.a-price-whole"
SELETOR_FRACAO_PRECO = "span.a-price-fraction"
SELETOR_INDICADOR_USADO = "span.a-size-base.a-color-secondary"
SELETOR_PROXIMA_PAGINA = "a.s-pagination-item.s-pagination-next"

URL_GERAL_USADOS_BASE = "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011&rh=n%3A24669725011&s=popularity-rank&fs=true"
NOME_FLUXO_GERAL = "Amazon Quase Novo (Geral)"

MIN_DESCONTO_USADOS_STR = os.getenv("MIN_DESCONTO_PERCENTUAL_USADOS", "30").strip()
try:
    MIN_DESCONTO_USADOS = int(MIN_DESCONTO_USADOS_STR)
    if not (0 <= MIN_DESCONTO_USADOS <= 100):
        logger.warning(f"MIN_DESCONTO_USADOS ({MIN_DESCONTO_USADOS}%) fora do intervalo. Usando 30%.")
        MIN_DESCONTO_USADOS = 30
except ValueError:
    logger.warning(f"Valor inválido para MIN_DESCONTO_PERCENTUAL_USADOS ('{MIN_DESCONTO_USADOS_STR}'). Usando 30%.")
    MIN_DESCONTO_USADOS = 30
logger.info(f"Desconto mínimo para notificação de usados (informativo para mensagem): {MIN_DESCONTO_USADOS}%")

USAR_HISTORICO_STR = os.getenv("USAR_HISTORICO_USADOS", "true").strip().lower()
USAR_HISTORICO = USAR_HISTORICO_STR == "true"
logger.info(f"Usar histórico para produtos usados: {USAR_HISTORICO}")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_IDS_STR = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_CHAT_IDS_LIST = [chat_id.strip() for chat_id in TELEGRAM_CHAT_IDS_STR.split(',') if chat_id.strip()]

# !!! AJUSTADO PARA TESTE !!!
MAX_PAGINAS_USADOS_GERAL = 2 # int(os.getenv("MAX_PAGINAS_USADOS_GERAL", "25"))
logger.info(f"!!! MODO DE TESTE: Máximo de páginas para busca geral de usados: {MAX_PAGINAS_USADOS_GERAL} !!!")


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
    chrome_options.add_argument("--window-size=1920x1080")
    user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    chrome_options.add_argument(f"user-agent={user_agent}")
    current_run_logger.info(f"User-Agent: {user_agent}")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
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
        page_load_timeout = 75
        driver.set_page_load_timeout(page_load_timeout)
        current_run_logger.info(f"Timeout de carregamento de página definido para {page_load_timeout}s.")
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        current_run_logger.info("Script para ocultar 'navigator.webdriver' executado.")
        return driver
    except Exception as e:
        current_run_logger.error(f"Erro ao instanciar ou configurar o WebDriver: {e}", exc_info=True)
        if driver:
            driver.quit()
        raise

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
        price_whole_el = element.find_element(By.CSS_SELECTOR, SELETOR_PRECO_USADO)
        price_whole = price_whole_el.text
        price_logger.debug(f"Parte inteira: '{price_whole}'")
        price_fraction_el = element.find_element(By.CSS_SELECTOR, SELETOR_FRACAO_PRECO)
        price_fraction = price_fraction_el.text
        price_logger.debug(f"Fração: '{price_fraction}'")
        raw_price = f"{price_whole}.{price_fraction}"
        cleaned = re.sub(r'[^\d.]', '', raw_price)
        final_price = float(cleaned)
        price_logger.debug(f"Preço final: {final_price}")
        return final_price
    except NoSuchElementException:
        price_logger.debug(f"Elemento de preço (inteiro ou fração) não encontrado no item.")
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
    try:
        qid_time = asyncio.get_event_loop().time()
    except RuntimeError:
        import time
        qid_time = time.time()
        current_run_logger.warning("asyncio.get_event_loop().time() falhou, usando time.time() para qid.")
    query_params['qid'] = [str(int(qid_time * 1000))]
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

def wait_for_page_load(driver, current_run_logger):
    current_run_logger.debug("Aguardando carregamento completo da página (document.readyState).")
    try:
        WebDriverWait(driver, 75).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        current_run_logger.info("Página carregada completamente (document.readyState is 'complete').")
        return True
    except TimeoutException:
        current_run_logger.error("Timeout (75s) ao esperar carregamento completo da página (document.readyState).", exc_info=False)
        return False
    except Exception as e:
        current_run_logger.error(f"Erro inesperado ao aguardar carregamento da página: {e}", exc_info=True)
        return False

async def process_used_products_geral_async(
    driver, base_url_usados, scraper_logger,
    history_data, min_desconto_notif, bot_inst, chat_ids
):
    scraper_logger.info(f"--- Iniciando processamento para: {NOME_FLUXO_GERAL} --- URL base: {base_url_usados} ---")
    
    paginas_sem_produtos_consecutivas = 0
    produtos_encontrados_total = 0
    paginas_processadas_count = 0

    for page_num in range(1, MAX_PAGINAS_USADOS_GERAL + 1):
        paginas_processadas_count +=1
        current_page_url = get_url_for_page_worker(base_url_usados, page_num, scraper_logger)
        scraper_logger.info(f"[{NOME_FLUXO_GERAL}] Carregando Página: {page_num}/{MAX_PAGINAS_USADOS_GERAL}, URL: {current_page_url}")

        max_load_attempts = 3
        page_loaded_successfully = False
        for attempt in range(1, max_load_attempts + 1):
            scraper_logger.info(f"[{NOME_FLUXO_GERAL}] Tentativa {attempt}/{max_load_attempts} de carregar URL.")
            try:
                await asyncio.to_thread(driver.get, current_page_url)
                await asyncio.sleep(5)

                if not await asyncio.to_thread(wait_for_page_load, driver, scraper_logger):
                    scraper_logger.warning(f"Página {page_num} (tentativa {attempt}) não carregou (readyState).")
                    await asyncio.sleep(4 * attempt)
                    continue
                
                if await asyncio.to_thread(check_captcha_sync_worker, driver, scraper_logger):
                    scraper_logger.error(f"CAPTCHA na página {page_num}. Abortando este fluxo de Usados.")
                    return
                
                scraper_logger.debug(f"Aguardando presença de itens com seletor: '{SELETOR_ITEM_PRODUTO_USADO}'")
                await asyncio.to_thread(
                    WebDriverWait(driver, 60).until,
                    EC.presence_of_element_located((By.CSS_SELECTOR, SELETOR_ITEM_PRODUTO_USADO))
                )
                scraper_logger.info(f"Seletor de item encontrado na página {page_num}.")
                page_loaded_successfully = True
                break
            except TimeoutException:
                scraper_logger.warning(f"Timeout (WebDriverWait de 60s) ao esperar por itens na pág {page_num} (tentativa {attempt}).")
                
                timestamp_debug = datetime.now().strftime('%Y%m%d_%H%M%S')
                nome_arquivo_debug_base = f"timeout_items_p{page_num}_t{attempt}_{timestamp_debug}"
                screenshot_path_debug = os.path.join(DEBUG_LOGS_DIR_BASE, f"{nome_arquivo_debug_base}.png")
                html_path_debug = os.path.join(DEBUG_LOGS_DIR_BASE, f"{nome_arquivo_debug_base}.html")
                
                try:
                    current_url_debug = await asyncio.to_thread(driver.current_url)
                    scraper_logger.info(f"URL no momento do timeout de itens: {current_url_debug}")
                    await asyncio.to_thread(driver.save_screenshot, screenshot_path_debug)
                    scraper_logger.info(f"Screenshot do timeout de itens salvo em: {screenshot_path_debug}")
                    page_html_debug = await asyncio.to_thread(driver.page_source)
                    with open(html_path_debug, "w", encoding="utf-8") as f_html:
                        f_html.write(page_html_debug)
                    scraper_logger.info(f"HTML da página do timeout de itens salvo em: {html_path_debug}")
                except Exception as e_debug_save:
                    scraper_logger.error(f"Erro ao salvar screenshot/HTML de debug para timeout de itens: {e_debug_save}")

                try:
                    no_results_elements = await asyncio.to_thread(driver.find_elements, By.XPATH, "//span[contains(text(),'Nenhum resultado para')] | //*[contains(text(),'não encontraram nenhum resultado')] | //div[contains(@class, 's-no-results')]")
                    if no_results_elements:
                        for el_no_res in no_results_elements:
                            if await asyncio.to_thread(el_no_res.is_displayed):
                                scraper_logger.info(f"Página {page_num} indica 'Nenhum resultado' (visível). Fim dos produtos.")
                                return
                        scraper_logger.info(f"Página {page_num} contém elementos de 'Nenhum resultado', mas podem não estar visíveis.")
                except Exception as e_no_res_check:
                     scraper_logger.warning(f"Erro ao checar por 'Nenhum resultado': {e_no_res_check}")
                
                if attempt == max_load_attempts:
                    scraper_logger.error(f"Todas as {max_load_attempts} tentativas de carregar pág {page_num} falharam (Timeout esperando itens). Desistindo desta página.")
                    break 
                await asyncio.sleep(6 * attempt)
            except Exception as e_load:
                scraper_logger.error(f"Erro geral ao carregar pág {page_num} (tentativa {attempt}): {e_load}", exc_info=True)
                if attempt == max_load_attempts: break
                await asyncio.sleep(6 * attempt)

        if not page_loaded_successfully:
            scraper_logger.warning(f"Não foi possível carregar pág {page_num} após {max_load_attempts} tentativas. Pulando.")
            paginas_sem_produtos_consecutivas += 1
            if paginas_sem_produtos_consecutivas >= 3:
                 scraper_logger.info(f"{paginas_sem_produtos_consecutivas} páginas consecutivas sem sucesso/produtos. Finalizando busca.")
                 break
            continue
        
        items_on_page = []
        try:
            items_on_page = await asyncio.to_thread(driver.find_elements, By.CSS_SELECTOR, SELETOR_ITEM_PRODUTO_USADO)
            scraper_logger.info(f"Página {page_num}: Encontrados {len(items_on_page)} elementos com seletor principal.")
        except Exception as e_find:
            scraper_logger.error(f"Erro ao buscar itens na página {page_num}: {e_find}", exc_info=True)
            continue

        if not items_on_page:
            scraper_logger.warning(f"Nenhum item de produto encontrado na página {page_num} (após carregamento bem-sucedido).")
            paginas_sem_produtos_consecutivas += 1
            if paginas_sem_produtos_consecutivas >= 3 and page_num > 1:
                scraper_logger.info(f"{paginas_sem_produtos_consecutivas} págs sem produtos. Finalizando.")
                break
            continue
        else:
            paginas_sem_produtos_consecutivas = 0

        current_page_products_processed = 0
        for item_idx, item_element in enumerate(items_on_page):
            scraper_logger.debug(f"Processando item {item_idx + 1}/{len(items_on_page)} na página {page_num}.")
            try:
                asin = await asyncio.to_thread(item_element.get_attribute, 'data-asin')
                if not asin:
                    scraper_logger.debug("Item sem data-asin. Pulando.")
                    continue

                is_sponsored = False
                xpath_sponsored = ".//span[contains(translate(normalize-space(.), 'PATROCINADOABCDEFGHIJKLMNOPQRSTUVWXYZ', 'patrocinadoabcdefghijklmnopqrstuvwxyz'), 'patrocinado')] | .//div[@data-cy='sponsored-label'] | .//a[@data-a-Qualifier='sp']"
                try:
                    sponsored_els = await asyncio.to_thread(item_element.find_elements, By.XPATH, xpath_sponsored)
                    if sponsored_els:
                        for sp_el in sponsored_els:
                            if await asyncio.to_thread(sp_el.is_displayed):
                                is_sponsored = True; break
                    if is_sponsored:
                        scraper_logger.debug(f"ASIN {asin}: Item patrocinado. Pulando.")
                        continue
                except Exception:
                    pass 
                
                try:
                    indicador_el = await asyncio.to_thread(item_element.find_element, By.CSS_SELECTOR, SELETOR_INDICADOR_USADO)
                    texto_indicador = (await asyncio.to_thread(indicador_el.text)).lower()
                    scraper_logger.debug(f"ASIN {asin}: Texto do indicador: '{texto_indicador}'")
                    if "usado" not in texto_indicador and "recondicionado" not in texto_indicador:
                        scraper_logger.debug(f"ASIN {asin} não é 'Usado'/'Recondicionado' ('{texto_indicador}'). Pulando.")
                        continue
                except NoSuchElementException:
                    scraper_logger.debug(f"ASIN {asin} sem indicador de usado ('{SELETOR_INDICADOR_USADO}'). Pulando.")
                    continue
                
                nome_produto = "N/A"
                try:
                    nome_el = await asyncio.to_thread(item_element.find_element, By.CSS_SELECTOR, SELETOR_NOME_PRODUTO_USADO)
                    nome_produto = (await asyncio.to_thread(nome_el.text))[:150].strip()
                except NoSuchElementException:
                    scraper_logger.warning(f"ASIN {asin}: Nome não encontrado. Pulando.")
                    continue
                
                preco_produto = await asyncio.to_thread(get_price_from_element, item_element, scraper_logger)
                if not preco_produto:
                    scraper_logger.warning(f"ASIN {asin}, Nome: {nome_produto[:30]}...: Preço não encontrado/inválido. Pulando.")
                    continue
                
                link_produto_final = f"https://www.amazon.com.br/dp/{asin}"
                produtos_encontrados_total += 1
                current_page_products_processed +=1

                scraper_logger.info(f"[{NOME_FLUXO_GERAL}] Produto: '{nome_produto[:40]}...' (ASIN:{asin}), Preço: R${preco_produto:.2f}")

                if USAR_HISTORICO:
                    product_history_entry = history_data.get(asin)
                    last_price_in_history = None
                    if product_history_entry and product_history_entry.get('precos'):
                        last_price_in_history = product_history_entry['precos'][-1]['preco']
                    
                    if last_price_in_history is not None and preco_produto >= last_price_in_history:
                        scraper_logger.info(f"ASIN {asin}: Preço atual (R${preco_produto:.2f}) >= último (R${last_price_in_history:.2f}). Sem notificação.")
                        if asin not in history_data:
                             history_data[asin] = {'nome': nome_produto, 'precos': [], 'link': link_produto_final, 'fluxo_ultima_vez_visto': NOME_FLUXO_GERAL}
                        history_data[asin]['precos'].append({'preco': preco_produto, 'data': datetime.now().isoformat()})
                        history_data[asin]['fluxo_ultima_vez_visto'] = NOME_FLUXO_GERAL
                        if len(history_data[asin]['precos']) > 20:
                            history_data[asin]['precos'] = history_data[asin]['precos'][-20:]
                        continue

                    scraper_logger.info(f"ASIN {asin}: Novo no histórico ou preço caiu (Atual R${preco_produto:.2f} vs Anterior R${last_price_in_history if last_price_in_history else 'N/A'}). Notificando.")
                    if asin not in history_data:
                        history_data[asin] = {'nome': nome_produto, 'precos': [], 'link': link_produto_final, 'fluxo_ultima_vez_visto': NOME_FLUXO_GERAL}
                    
                    history_data[asin]['nome'] = nome_produto
                    history_data[asin]['link'] = link_produto_final
                    history_data[asin]['precos'].append({'preco': preco_produto, 'data': datetime.now().isoformat()})
                    history_data[asin]['fluxo_ultima_vez_visto'] = NOME_FLUXO_GERAL
                    if len(history_data[asin]['precos']) > 20:
                        history_data[asin]['precos'] = history_data[asin]['precos'][-20:]
                
                if bot_inst and chat_ids:
                    desconto_msg_str = "Novo produto no rastreamento!"
                    # Tentativa de usar last_price_in_history para a mensagem de desconto, se disponível e USAR_HISTORICO for True
                    if USAR_HISTORICO and product_history_entry and product_history_entry.get('precos') and len(product_history_entry['precos']) > 1:
                        # Pega o penúltimo preço, pois o último já é o atual que acabamos de adicionar
                        # ou o last_price_in_history se o preço não mudou mas é novo na sessão
                        prev_price_for_msg = product_history_entry['precos'][-2]['preco'] if len(product_history_entry['precos']) > 1 else last_price_in_history
                        if prev_price_for_msg and preco_produto < prev_price_for_msg:
                           desconto_perc = ((prev_price_for_msg - preco_produto) / prev_price_for_msg) * 100
                           desconto_msg_str = f"Preço caiu! Antes: R${prev_price_for_msg:.2f}. Desconto: {desconto_perc:.2f}%"
                    
                    telegram_message = (
                        f"*{escape_md('Amazon Quase Novo!')}*\n\n"
                        f"*{escape_md(nome_produto)}*\n"
                        f"Preço: R${preco_produto:.2f}\n"
                        f"Detalhe: {escape_md(desconto_msg_str)}\n\n"
                        f"🔗 {escape_md(link_produto_final)}"
                    )
                    scraper_logger.info(f"Enviando notificação para '{nome_produto[:30]}...' (ASIN:{asin})")
                    for cid in chat_ids:
                        await send_telegram_message_async(bot_inst, cid, telegram_message, ParseMode.MARKDOWN, scraper_logger)
                else:
                    scraper_logger.info(f"Bot não configurado. Sem notificação para ASIN {asin}.")

            except StaleElementReferenceException:
                scraper_logger.warning(f"Item obsoleto (StaleElement) na página {page_num}. Pulando item.")
                continue
            except Exception as e_item_proc:
                scraper_logger.error(f"Erro ao processar item na pág {page_num}: {e_item_proc}", exc_info=True)
                continue
        
        scraper_logger.info(f"Página {page_num}: {current_page_products_processed} produtos 'usados' processados.")

        try:
            scraper_logger.debug(f"Verificando botão 'Próxima Página' (seletor: {SELETOR_PROXIMA_PAGINA})")
            next_page_el = await asyncio.to_thread(driver.find_element, By.CSS_SELECTOR, SELETOR_PROXIMA_PAGINA)
            is_disabled = 's-pagination-disabled' in (await asyncio.to_thread(next_page_el.get_attribute, 'class') or "")
            has_href = await asyncio.to_thread(next_page_el.get_attribute, 'href')

            if is_disabled or not has_href:
                scraper_logger.info("Botão 'Próxima Página' desabilitado ou é o último. Fim da busca.")
                break
            scraper_logger.info("Botão 'Próxima Página' encontrado. Indo para a próxima.")
        except NoSuchElementException:
            scraper_logger.info("Botão 'Próxima Página' não encontrado. Fim da busca.")
            break
        except Exception as e_next_page:
            scraper_logger.error(f"Erro ao verificar 'Próxima Página': {e_next_page}", exc_info=True)
            break
        
        await asyncio.sleep(max(3, int(os.getenv("DELAY_ENTRE_PAGINAS_USADOS", "6"))))

    scraper_logger.info(f"--- Concluído Fluxo: {NOME_FLUXO_GERAL}. Páginas processadas: {paginas_processadas_count}. Total de produtos 'usados' qualificados encontrados: {produtos_encontrados_total} ---")

async def run_usados_geral_scraper_async(history_data, driver_path_param=None):
    scraper_logger_name = f"scraper.{DEBUG_LOG_FILENAME_BASE_USADOS_GERAL}" # Evita pontos extras se base já tiver
    scraper_logger = logging.getLogger(scraper_logger_name)
    
    if not any(isinstance(h, logging.FileHandler) for h in scraper_logger.handlers):
        log_file_path = os.path.join(DEBUG_LOGS_DIR_BASE, f"{DEBUG_LOG_FILENAME_BASE_USADOS_GERAL}.log")
        try:
            file_h = logging.FileHandler(log_file_path, encoding='utf-8', mode='w')
            file_h.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - [%(name)s:%(funcName)s:%(lineno)d] - %(message)s"))
            scraper_logger.addHandler(file_h)
        except Exception as e_fh_scraper:
            logger.error(f"Falha ao criar FileHandler para {scraper_logger_name} em {log_file_path}: {e_fh_scraper}. Logs podem não ir para arquivo.")

    scraper_logger_level_str = os.getenv("WORKER_LOG_LEVEL", "INFO").upper()
    scraper_logger_level = getattr(logging, scraper_logger_level_str, logging.INFO)
    scraper_logger.setLevel(scraper_logger_level)
    # scraper_logger.propagate = False # REMOVIDO para que logs INFO apareçam no console (se o root tiver StreamHandler)

    driver_instance = None
    scraper_logger.info(f"--- [SCRAPER INÍCIO] Fluxo: {NOME_FLUXO_GERAL} ---")
    try:
        scraper_logger.info("Tentando iniciar o driver Selenium...")
        driver_instance = await asyncio.to_thread(iniciar_driver_sync_worker, scraper_logger, driver_path_param)
        scraper_logger.info("Driver Selenium iniciado com sucesso.")

        await process_used_products_geral_async(
            driver=driver_instance,
            base_url_usados=URL_GERAL_USADOS_BASE,
            scraper_logger=scraper_logger,
            history_data=history_data,
            min_desconto_notif=MIN_DESCONTO_USADOS,
            bot_inst=bot_instance_global,
            chat_ids=TELEGRAM_CHAT_IDS_LIST
        )
        scraper_logger.info("Processamento do fluxo de usados geral concluído.")

    except Exception as e_scraper_main:
        scraper_logger.error(f"Erro principal no scraper de usados geral: {e_scraper_main}", exc_info=True)
    finally:
        if driver_instance:
            scraper_logger.info("Tentando fechar o driver Selenium...")
            try:
                await asyncio.to_thread(driver_instance.quit)
                scraper_logger.info("Driver Selenium fechado.")
            except Exception as e_quit_scraper:
                scraper_logger.error(f"Erro ao fechar o driver: {e_quit_scraper}", exc_info=True)
        
        scraper_logger.info(f"--- [SCRAPER FIM] Fluxo: {NOME_FLUXO_GERAL} ---")
        for handler in list(scraper_logger.handlers):
            if isinstance(handler, logging.FileHandler):
                try:
                    handler.close()
                    scraper_logger.removeHandler(handler)
                except Exception as e_close_fh_final :
                     logger.error(f"Erro ao fechar/remover FileHandler final para {scraper_logger.name}: {e_close_fh_final}")

async def orchestrate_usados_geral_scrape_async():
    logger.info("--- INICIANDO ORQUESTRADOR DE SCRAPING DE USADOS (GERAL) ---")
    current_history = load_history_geral()
    installed_driver = None
    try:
        logger.info("Tentando instalar/localizar ChromeDriver (WebDriverManager)...")
        installed_driver = ChromeDriverManager().install()
        logger.info(f"ChromeDriver está em: {installed_driver}")
    except Exception as e_wdm_orch:
        logger.warning(f"Falha WebDriverManager: {e_wdm_orch}. Tentando usar driver no PATH...", exc_info=False)
    
    await run_usados_geral_scraper_async(
        history_data=current_history,
        driver_path_param=installed_driver
    )

    if USAR_HISTORICO:
        logger.info("Salvando histórico de usados geral...")
        save_history_geral(current_history)
    
    logger.info("--- ORQUESTRADOR DE SCRAPING DE USADOS (GERAL) CONCLUÍDO ---")

if __name__ == "__main__":
    script_file_name = os.path.basename(__file__)
    logger.info(f"Scraper de Usados Geral ('{script_file_name}') chamado via __main__.")
    
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_IDS_LIST:
        logger.warning("ALERTA USADOS: Token/Chat IDs Telegram não configurados. Notificações desabilitadas.")
    
    try:
        asyncio.run(orchestrate_usados_geral_scrape_async())
    except KeyboardInterrupt:
        logger.info("Execução interrompida (KeyboardInterrupt).")
    except Exception as e_main_usados:
        logger.critical(f"Erro fatal no orquestrador de usados: {e_main_usados}", exc_info=True)
    finally:
        logger.info(f"Finalizando script '{script_file_name}'.")
