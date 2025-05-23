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
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException, TimeoutException,
    StaleElementReferenceException, InvalidSelectorException, WebDriverException
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
logging.getLogger("bs4").setLevel(logging.WARNING)

logger = logging.getLogger("SCRAPER_USADOS_GERAL")

# --- Configurações do Scraper ---
SELETOR_ITEM_PRODUTO_USADO = "div.s-result-item.s-asin"
SELETOR_INDICADOR_USADO_XPATH = ".//div[contains(@class, 's-price-instructions-style')]//a//span[contains(translate(., 'USADO', 'usado'), 'usado')]"
SELETOR_RESULTADOS_CONT = "div.s-main-slot.s-result-list.s-search-results.sg-row" # Atualizado para maior precisão

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
logger.info(f"Desconto mínimo para notificação de usados: {MIN_DESCONTO_USADOS}% (Observação: este filtro não está sendo aplicado explicitamente no código atual antes da notificação)")


USAR_HISTORICO_STR = os.getenv("USAR_HISTORICO_USADOS", "true").strip().lower()
USAR_HISTORICO = USAR_HISTORICO_STR == "true"
logger.info(f"Usar histórico para produtos usados: {USAR_HISTORICO}")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_IDS_STR = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_CHAT_IDS_LIST = [chat_id.strip() for chat_id in TELEGRAM_CHAT_IDS_STR.split(',') if chat_id.strip()]

MAX_PAGINAS_POR_LINK_GLOBAL = int(os.getenv("MAX_PAGINAS_USADOS_GERAL", "500")) # Será sobrescrito pelo log para 1 ou 2
logger.info(f"Máximo de páginas para busca geral de usados: {MAX_PAGINAS_POR_LINK_GLOBAL}")

HISTORY_DIR_BASE = "history_files_usados"
DEBUG_LOGS_DIR_BASE = "debug_logs_usados"
HISTORY_FILENAME_USADOS_GERAL = "price_history_USADOS_GERAL.json"

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


async def process_used_products_geral_async(driver, base_url, nome_fluxo, history, logger, max_paginas=MAX_PAGINAS_POR_LINK_GLOBAL):
    logger.info(f"--- Iniciando processamento para: {nome_fluxo} --- URL base: {base_url} ---")
    total_produtos_usados_qualificados = []
    pagina_atual = 1
    max_tentativas_pagina = 3
    consecutive_empty_pages = 0
    max_consecutive_empty_pages = 3 # Se 3 páginas seguidas não tiverem itens, para.

    # Atualiza max_paginas se a variável de ambiente foi definida para um valor menor (como visto nos logs)
    # Esta linha assume que MAX_PAGINAS_POR_LINK_GLOBAL já reflete o valor do os.getenv no início do script
    # Se MAX_PAGINAS_USADOS_GERAL é 1 ou 2 nos logs, a variável max_paginas já terá esse valor.
    if max_paginas != MAX_PAGINAS_POR_LINK_GLOBAL:
        logger.info(f"O parâmetro 'max_paginas' ({max_paginas}) é diferente de MAX_PAGINAS_POR_LINK_GLOBAL ({MAX_PAGINAS_POR_LINK_GLOBAL}). Usando {max_paginas}.")


    while pagina_atual <= max_paginas:
        url_pagina = get_url_for_page_worker(base_url, pagina_atual, logger)
        logger.info(f"[{nome_fluxo}] Carregando Página: {pagina_atual}/{max_paginas}, URL: {url_pagina}")

        page_processed_successfully = False
        for tentativa in range(1, max_tentativas_pagina + 1):
            logger.info(f"[{nome_fluxo}] Tentativa {tentativa}/{max_tentativas_pagina} de carregar e processar URL: {url_pagina}")
            try:
                await asyncio.to_thread(driver.get, url_pagina)
                await asyncio.sleep(random.uniform(3, 6))
                await asyncio.to_thread(wait_for_page_load, driver, logger)
                await simulate_scroll(driver, logger)

                try:
                    timestamp_page_dump = datetime.now().strftime('%Y%m%d_%H%M%S')
                    # Adicionado nome do fluxo ao arquivo de dump para clareza se houver múltiplos fluxos no futuro
                    page_dump_filename = f"page_dump_p{pagina_atual}_fluxo_{nome_fluxo.replace(' ', '_')}_{timestamp_page_dump}.html"
                    page_dump_path = os.path.join(DEBUG_LOGS_DIR_BASE, page_dump_filename)
                    with open(page_dump_path, "w", encoding="utf-8") as f_html_dump:
                        f_html_dump.write(driver.page_source)
                    logger.info(f"HTML da página {pagina_atual} salvo em: {page_dump_path}")
                except Exception as e_save_dump:
                    logger.error(f"Erro ao salvar o HTML da página {pagina_atual}: {e_save_dump}")

                if check_captcha_sync_worker(driver, logger):
                    logger.error(f"[{nome_fluxo}] CAPTCHA detectado na página {pagina_atual}. Interrompendo fluxo para esta URL base.")
                    return total_produtos_usados_qualificados

                if check_amazon_error_page_sync_worker(driver, logger):
                    logger.error(f"[{nome_fluxo}] Página de erro da Amazon detectada na página {pagina_atual}.")
                    if tentativa < max_tentativas_pagina:
                        logger.info("Tentando novamente após delay...")
                        await asyncio.sleep(random.uniform(10, 20))
                        continue
                    else:
                        logger.error(f"[{nome_fluxo}] Falha ao carregar página de produtos após {max_tentativas_pagina} tentativas devido a página de erro. Interrompendo.")
                        return total_produtos_usados_qualificados
                
                try:
                    WebDriverWait(driver, 20).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, SELETOR_RESULTADOS_CONT))
                    )
                    logger.info(f"Contêiner de resultados '{SELETOR_RESULTADOS_CONT}' encontrado na página {pagina_atual}.")
                except TimeoutException:
                    logger.warning(f"Contêiner de resultados '{SELETOR_RESULTADOS_CONT}' não encontrado na página {pagina_atual} após timeout. Pode ser página vazia ou com estrutura inesperada.")
                    # Não interrompe aqui, a lógica de items_selenium abaixo verificará.

                items_selenium = driver.find_elements(By.CSS_SELECTOR, SELETOR_ITEM_PRODUTO_USADO)
                logger.info(f"Página {pagina_atual}: Encontrados {len(items_selenium)} elementos com seletor Selenium '{SELETOR_ITEM_PRODUTO_USADO}'.")

                if not items_selenium:
                    logger.info(f"Página {pagina_atual} não contém produtos com o seletor principal. Verificando se é o fim.")
                    next_button_disabled = False
                    try:
                        driver.find_element(By.CSS_SELECTOR, ".s-pagination-item.s-pagination-next.s-pagination-disabled")
                        logger.info("Botão 'Próximo' está desabilitado. Fim da paginação.")
                        next_button_disabled = True
                    except NoSuchElementException:
                        logger.debug("Botão 'Próximo' não está desabilitado ou não foi encontrado.")
                    
                    if next_button_disabled:
                        return total_produtos_usados_qualificados
                    
                    consecutive_empty_pages += 1
                    if consecutive_empty_pages >= max_consecutive_empty_pages:
                        logger.warning(f"{max_consecutive_empty_pages} páginas vazias consecutivas. Considerando fim da busca para {nome_fluxo}.")
                        return total_produtos_usados_qualificados
                    logger.info(f"Página {pagina_atual} vazia, mas não é o fim. Tentativa {consecutive_empty_pages}/{max_consecutive_empty_pages}.")
                    page_processed_successfully = True 
                    break 

                consecutive_empty_pages = 0 
                produtos_processados_e_notificados_na_pagina = 0

                for idx, item_element_selenium in enumerate(items_selenium, 1):
                    item_logger = logging.getLogger(f"{logger.name}.Item_{pagina_atual}_{idx}")
                    item_logger.debug(f"Processando bloco de item {idx} da página {pagina_atual}")
                    
                    nome, link, asin, price = None, None, None, None

                    try:
                        try:
                            indicador_usado_el = item_element_selenium.find_element(By.XPATH, SELETOR_INDICADOR_USADO_XPATH)
                            item_logger.debug(f"Indicador direto de 'usado' encontrado via XPath: '{indicador_usado_el.text.strip()}'")
                        except NoSuchElementException:
                            item_logger.debug(f"Item NÃO é uma listagem direta de 'usado' (XPath '{SELETOR_INDICADOR_USADO_XPATH}' não encontrado). Ignorando este item.")
                            continue

                        item_html = item_element_selenium.get_attribute('outerHTML')
                        item_soup = BeautifulSoup(item_html, 'html.parser')

                        nome_tag = item_soup.find('span', class_=['a-size-base-plus', 'a-color-base', 'a-text-normal'])
                        if not nome_tag:
                             nome_tag = item_soup.find('h2', class_='a-size-base-plus') # Fallback
                        
                        if nome_tag:
                            nome = nome_tag.get_text(strip=True)
                            if not nome: # Nome vazio
                                item_logger.debug("Nome do produto vazio (BS). Ignorando.")
                                continue
                            item_logger.debug(f"Nome (BS): '{nome}'")
                        else:
                            item_logger.warning("Nome não encontrado com BeautifulSoup. Ignorando item.")
                            continue

                        link_tag = item_soup.find('a', class_='a-link-normal s-no-outline', href=re.compile(r'/dp/'))
                        if link_tag and link_tag.has_attr('href'):
                            href_val = link_tag['href']
                            if href_val.startswith("/"):
                                link = f"https://www.amazon.com.br{href_val}"
                            else:
                                link = href_val
                            item_logger.debug(f"Link (BS): '{link}'")

                            asin_match = re.search(r'/dp/([A-Z0-9]{10})', link)
                            if asin_match:
                                asin = asin_match.group(1)
                                item_logger.debug(f"ASIN (BS): '{asin}'")
                            else:
                                item_logger.warning(f"ASIN não encontrado no link '{link}'. Ignorando item.")
                                continue
                        else:
                            item_logger.warning("Link principal do produto não encontrado com BeautifulSoup. Ignorando item.")
                            continue
                        
                        price_text_bs = None
                        price_instructions_div_bs = item_soup.find('div', class_='s-price-instructions-style')
                        if price_instructions_div_bs:
                            price_link_tag_bs = price_instructions_div_bs.find('a')
                            if price_link_tag_bs:
                                price_span_offscreen_bs = price_link_tag_bs.find('span', class_='a-offscreen')
                                if price_span_offscreen_bs:
                                    price_text_bs = price_span_offscreen_bs.get_text(strip=True)
                                    item_logger.debug(f"Preço (BS, via 's-price-instructions-style'): '{price_text_bs}'")
                        
                        if not price_text_bs:
                            price_tag_generic_bs = item_soup.find('span', class_='a-offscreen')
                            if price_tag_generic_bs:
                                price_text_bs = price_tag_generic_bs.get_text(strip=True)
                                item_logger.debug(f"Preço (BS, fallback genérico 'a-offscreen'): '{price_text_bs}'")
                        
                        if price_text_bs:
                            cleaned_price_str = re.sub(r'[^\d,]', '', price_text_bs).replace(',', '.')
                            try:
                                price = float(cleaned_price_str)
                                item_logger.debug(f"Preço final (BS): {price}")
                            except ValueError:
                                item_logger.warning(f"Erro ao converter preço (BS) '{cleaned_price_str}' para float. Ignorando item.")
                                continue
                        else:
                            item_logger.warning(f"Preço 'usado' não encontrado com BeautifulSoup para ASIN {asin if asin else 'desconhecido'}. Ignorando item.")
                            continue

                        produto = {
                            "nome": nome, "asin": asin, "link": link,
                            "preco_usado": price, "timestamp": datetime.now().isoformat(),
                            "fluxo": nome_fluxo
                        }

                        if USAR_HISTORICO:
                            preco_historico_info = history.get(asin)
                            if preco_historico_info:
                                preco_historico_val = preco_historico_info.get("preco_usado")
                                if preco_historico_val and preco_historico_val <= price:
                                    item_logger.info(f"ASIN {asin}: Preço atual (R${price:.2f}) não é menor que histórico (R${preco_historico_val:.2f}). Sem notificação.")
                                    continue
                                else:
                                    item_logger.info(f"ASIN {asin}: Novo preço (R${price:.2f}) melhor que histórico (R${preco_historico_val if preco_historico_val else 'N/A'}).")
                            else:
                                item_logger.info(f"ASIN {asin} não está no histórico. Novo produto 'usado' qualificado.")
                            
                            history[asin] = produto
                            save_history_geral(history)
                        
                        total_produtos_usados_qualificados.append(produto)
                        produtos_processados_e_notificados_na_pagina += 1
                        item_logger.info(f"PRODUTO QUALIFICADO: '{nome}' | Preço: R${price:.2f} | ASIN: {asin}")

                        if bot_instance_global and TELEGRAM_CHAT_IDS_LIST:
                            message = (
                                f"*{escape_md(nome_fluxo)}*\n\n"
                                f"📦 *{escape_md(nome)}*\n"
                                f"💵 Preço Usado: *R${price:.2f}*\n"
                                f"🔗 [Ver na Amazon]({link})\n\n"
                                f"🏷️ ASIN: `{escape_md(asin)}`\n"
                                f"🕒 {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
                            )
                            for chat_id in TELEGRAM_CHAT_IDS_LIST:
                                await send_telegram_message_async(
                                    bot_instance_global, chat_id, message, ParseMode.MARKDOWN_V2, item_logger
                                )
                    
                    except StaleElementReferenceException:
                        item_logger.warning("Elemento Selenium tornou-se obsoleto. Tentando buscar itens novamente na página.")
                        break 
                    except Exception as e_item_proc:
                        item_logger.error(f"Erro inesperado ao processar bloco de item {idx}: {e_item_proc}", exc_info=True)
                        continue

                if produtos_processados_e_notificados_na_pagina > 0:
                    logger.info(f"Página {pagina_atual}: {produtos_processados_e_notificados_na_pagina} produtos 'usados' qualificados, processados e notificados.")
                else:
                    logger.info(f"Página {pagina_atual}: Nenhum produto novo ou com preço melhorado encontrado para notificação (após todas as verificações).")
                
                page_processed_successfully = True
                break 

            except WebDriverException as e_wd:
                logger.error(f"Erro de WebDriver ao carregar página {pagina_atual} (Tentativa {tentativa}): {str(e_wd)[:200]}", exc_info=False)
                if tentativa < max_tentativas_pagina:
                    await asyncio.sleep(random.uniform(15, 30))
                    continue
                else:
                    logger.error(f"Falha crítica após {max_tentativas_pagina} tentativas na página {pagina_atual} (WebDriverException). Interrompendo {nome_fluxo}.")
                    return total_produtos_usados_qualificados
            except Exception as e_page:
                logger.error(f"Erro geral ao processar página {pagina_atual} (Tentativa {tentativa}): {e_page}", exc_info=True)
                if tentativa < max_tentativas_pagina:
                    await asyncio.sleep(random.uniform(10, 20))
                    continue
                else:
                    logger.error(f"Falha crítica após {max_tentativas_pagina} tentativas na página {pagina_atual} (Erro Geral). Interrompendo {nome_fluxo}.")
                    return total_produtos_usados_qualificados
        
        if not page_processed_successfully:
            logger.error(f"Não foi possível processar a página {pagina_atual} de {nome_fluxo} após {max_tentativas_pagina} tentativas. Abortando este fluxo.")
            return total_produtos_usados_qualificados

        pagina_atual += 1
        if pagina_atual <= max_paginas : # Só dorme se houver próxima página
             await asyncio.sleep(random.uniform(5, 10)) 

    logger.info(
        f"--- Concluído Fluxo: {nome_fluxo}. Máximo de páginas ({max_paginas}) atingido ou fim da paginação. "
        f"Total de produtos 'usados' qualificados encontrados nesta execução: {len(total_produtos_usados_qualificados)} ---"
    )
    return total_produtos_usados_qualificados

async def run_usados_geral_scraper_async():
    logger.info(f"--- [SCRAPER INÍCIO] Fluxo: {NOME_FLUXO_GERAL} ---")
    driver = None
    try:
        logger.info("Tentando iniciar o driver Selenium...")
        driver = iniciar_driver_sync_worker(logger) 
        if not driver:
            logger.error("Falha crítica ao iniciar o WebDriver. Abortando scraper.")
            return

        logger.info("Driver Selenium iniciado com sucesso.")
        await get_initial_cookies(driver, logger)
        
        history = {}
        if USAR_HISTORICO:
            history = load_history_geral()
        
        # Passa MAX_PAGINAS_POR_LINK_GLOBAL explicitamente, que já leu do env var no início.
        await process_used_products_geral_async(driver, URL_GERAL_USADOS_BASE, NOME_FLUXO_GERAL, history, logger, MAX_PAGINAS_POR_LINK_GLOBAL)
        logger.info("Processamento do fluxo de usados geral concluído.")

    except Exception as e:
        logger.error(f"Erro catastrófico no fluxo geral de usados (run_usados_geral_scraper_async): {e}", exc_info=True)
    finally:
        if driver:
            logger.info("Tentando fechar o driver Selenium...")
            try:
                driver.quit()
                logger.info("Driver Selenium fechado.")
            except Exception as e_quit:
                logger.error(f"Erro ao fechar o driver: {e_quit}", exc_info=True)
        logger.info(f"--- [SCRAPER FIM] Fluxo: {NOME_FLUXO_GERAL} ---")


# Funções auxiliares (devem ser mantidas como na sua versão funcional ou usar estes exemplos)
def load_proxy_list():
    proxy_list = []
    proxy_hosts = os.getenv("PROXY_HOST", "").strip().split(',')
    proxy_ports = os.getenv("PROXY_PORT", "").strip().split(',')
    proxy_usernames = os.getenv("PROXY_USERNAME", "").strip().split(',')
    proxy_passwords = os.getenv("PROXY_PASSWORD", "").strip().split(',')
    for i in range(min(len(proxy_hosts), len(proxy_ports))):
        host = proxy_hosts[i].strip()
        port = proxy_ports[i].strip()
        username = proxy_usernames[i].strip() if i < len(proxy_usernames) and proxy_usernames[i].strip() else None
        password = proxy_passwords[i].strip() if i < len(proxy_passwords) and proxy_passwords[i].strip() else None
        if host and port: # Host e porta devem existir
            if not host.startswith("http"): # Garante que o host não tenha o protocolo, pois será adicionado
                proxy_url = f'http://{username}:{password}@{host}:{port}' if username and password else f'http://{host}:{port}'
                proxy_list.append(proxy_url)
            else: # Se já tiver o protocolo (improvável para a variável PROXY_HOST, mas por segurança)
                 proxy_list.append(host) # Assume que já está formatado
    
    if not proxy_list:
        logger.warning("Nenhum proxy configurado.")
    else:
        logger.info(f"Carregados {len(proxy_list)} proxies.")
    return proxy_list

def test_proxy(proxy_url, logger_param):
    logger_param.info(f"Testando proxy: {proxy_url}")
    try:
        # Adiciona User-Agent à requisição de teste
        ua_test = UserAgent()
        headers_test = {'User-Agent': ua_test.random}
        response = requests.get("https://www.amazon.com.br", proxies={"http": proxy_url, "https": proxy_url}, timeout=10, headers=headers_test)
        if response.status_code == 200:
            logger_param.info(f"Proxy {proxy_url} testado com sucesso: Status 200")
            return True
        else:
            logger_param.warning(f"Proxy {proxy_url} retornou status inesperado: {response.status_code}")
            return False
    except requests.RequestException as e:
        logger_param.error(f"Erro ao testar proxy {proxy_url}: {e}")
        return False

def get_working_proxy(proxy_list, logger_param):
    if not proxy_list: # Adicionado para evitar erro se a lista estiver vazia
        logger_param.warning("Lista de proxies vazia. Nenhum proxy para testar.")
        return None
    for proxy_url in proxy_list:
        if test_proxy(proxy_url, logger_param):
            return proxy_url
    logger_param.warning("Nenhum proxy funcional encontrado na lista. Prosseguindo sem proxy.")
    return None

def iniciar_driver_sync_worker(current_run_logger, driver_path=None): # Ajustes e logs conforme os seus
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
    chrome_options.add_argument("--disable-popup-blocking"); chrome_options.add_argument("--mute-audio")
    chrome_options.add_argument("--no-first-run"); chrome_options.add_argument("--disable-webgl"); chrome_options.add_argument("--disable-webrtc")
    chrome_options.add_argument("--disable-features=WebRtcHideLocalIpsWithMdns,PrivacySandboxSettings4,OptimizationHints,InterestGroupStorage")
    chrome_options.add_argument("--lang=pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7")
    
    proxies_available = load_proxy_list()
    working_proxy_url = get_working_proxy(proxies_available, current_run_logger) if proxies_available else None
    proxy_actually_configured = False

    if working_proxy_url:
        current_run_logger.info(f"Configurando proxy para Selenium: {working_proxy_url}")
        chrome_options.add_argument(f'--proxy-server={working_proxy_url}') # Selenium espera apenas host:porta ou schema://host:porta
        proxy_actually_configured = True
    else:
        current_run_logger.warning("Nenhum proxy funcional. WebDriver iniciará sem proxy.")
    current_run_logger.info(f"Opções do Chrome: {chrome_options.arguments}")

    service = None; driver = None
    page_load_timeout_val = 120
    try:
        path_from_manager = ChromeDriverManager().install()
        service = Service(path_from_manager)
        current_run_logger.info(f"ChromeDriver via Manager: {path_from_manager}")
        
        driver = webdriver.Chrome(service=service, options=chrome_options)
        current_run_logger.info("WebDriver instanciado.")
        driver.set_page_load_timeout(page_load_timeout_val)
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {'source': "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"})
        return driver
    except WebDriverException as e_wd_init:
        if ("ERR_NO_SUPPORTED_PROXIES" in str(e_wd_init) or "ERR_PROXY_CONNECTION_FAILED" in str(e_wd_init)) and proxy_actually_configured:
            current_run_logger.error(f"Erro de proxy ({working_proxy_url}) ao iniciar WebDriver: {str(e_wd_init)}. Tentando sem proxy.")
            chrome_options.arguments = [arg for arg in chrome_options.arguments if not arg.startswith('--proxy-server')]
            try:
                driver = webdriver.Chrome(service=service, options=chrome_options) # Tenta novamente sem o argumento de proxy
                current_run_logger.info("WebDriver instanciado sem proxy após falha inicial com proxy.")
                driver.set_page_load_timeout(page_load_timeout_val)
                driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {'source': "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"})
                return driver
            except Exception as e_retry_no_proxy:
                current_run_logger.error(f"Falha ao tentar iniciar WebDriver sem proxy após erro de proxy: {e_retry_no_proxy}", exc_info=True)
                if driver: driver.quit()
                raise
        else:
            current_run_logger.error(f"WebDriverException não relacionada a proxy configurado ao iniciar WebDriver: {e_wd_init}", exc_info=True)
            if driver: driver.quit()
            raise
    except Exception as e_init:
        current_run_logger.error(f"Erro geral ao iniciar WebDriver: {e_init}", exc_info=True)
        if driver: driver.quit()
        raise

async def get_initial_cookies(driver, logger_param):
    logger_param.info("Acessando página inicial para obter cookies...")
    try:
        await asyncio.to_thread(driver.get, "https://www.amazon.com.br")
        await asyncio.sleep(random.uniform(3, 5))
        await asyncio.to_thread(wait_for_page_load, driver, logger_param)
        logger_param.info("Cookies iniciais obtidos.")
    except Exception as e:
        logger_param.error(f"Erro ao obter cookies iniciais: {e}", exc_info=True)

async def simulate_scroll(driver, logger_param):
    logger_param.debug("Simulando rolagem na página...")
    try:
        await asyncio.to_thread(driver.execute_script, "window.scrollTo(0, document.body.scrollHeight*0.6);") # Rola 60%
        await asyncio.sleep(random.uniform(1, 2))
        await asyncio.to_thread(driver.execute_script, "window.scrollTo(0, document.body.scrollHeight);") # Rola até o fim
        await asyncio.sleep(random.uniform(0.5, 1.5))
        await asyncio.to_thread(driver.execute_script, "window.scrollTo(0, 0);") # Volta ao topo
        logger_param.debug("Rolagem simulada com sucesso.")
    except Exception as e:
        logger_param.error(f"Erro ao simular rolagem: {e}", exc_info=True)

async def send_telegram_message_async(bot, chat_id, message, parse_mode, msg_logger):
    msg_logger.debug(f"Tentando enviar mensagem para chat_id: {chat_id}")
    if not bot:
        msg_logger.error(f"[{msg_logger.name}] Instância do Bot não fornecida.")
        return False
    try:
        await bot.send_message(chat_id=chat_id, text=message, parse_mode=parse_mode)
        msg_logger.info(f"[{msg_logger.name}] Notificação Telegram enviada para CHAT_ID {chat_id}.")
        return True
    except TelegramError as e_tg:
        msg_logger.error(f"[{msg_logger.name}] Erro Telegram ao enviar para CHAT_ID {chat_id}: {e_tg.message}", exc_info=False)
        return False
    except Exception as e_msg:
        msg_logger.error(f"[{msg_logger.name}] Erro inesperado ao enviar msg para CHAT_ID {chat_id}: {e_msg}", exc_info=True)
        return False

def escape_md(text):
    return re.sub(r'([_\*\[\]\(\)~`>#+\-=|{}.!])', r'\\\1', str(text))

def load_history_geral():
    history_path = os.path.join(HISTORY_DIR_BASE, HISTORY_FILENAME_USADOS_GERAL)
    logger.info(f"Carregando histórico de: {history_path}")
    if os.path.exists(history_path):
        try:
            with open(history_path, 'r', encoding='utf-8') as f:
                history_data = json.load(f)
            logger.info(f"Histórico carregado: {len(history_data)} ASINs.")
            return history_data
        except Exception as e:
            logger.error(f"Erro ao carregar/decodificar histórico de '{history_path}': {e}. Retornando vazio.", exc_info=True)
            return {}
    else:
        logger.info("Arquivo de histórico não encontrado. Retornando histórico vazio.")
        return {}

def save_history_geral(history):
    history_path = os.path.join(HISTORY_DIR_BASE, HISTORY_FILENAME_USADOS_GERAL)
    logger.info(f"Salvando histórico ({len(history)} ASINs) em: {history_path}")
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
        WebDriverWait(driver, 3).until(EC.any_of( # Timeout curto
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
        current_run_logger.debug("Nenhum CAPTCHA detectado (ou timeout curto).")
        return False
    except Exception as e_check_captcha:
        current_run_logger.error(f"Erro inesperado ao verificar CAPTCHA: {e_check_captcha}", exc_info=True)
        return False

def check_amazon_error_page_sync_worker(driver, current_run_logger):
    current_run_logger.debug("Verificando se é página de erro da Amazon.")
    error_page_detected = False
    page_title_lower = ""
    try:
        page_title_lower = driver.title.lower()
        # Palavras chave para erro no título
        error_title_keywords = ["desculpe", "algo deu errado", "sorry", "problema", "serviço indisponível", "error", "não encontrada"]
        if any(keyword in page_title_lower for keyword in error_title_keywords):
            current_run_logger.warning(f"Página de erro detectada pelo título: {driver.title}")
            error_page_detected = True
        
        # Seletores comuns em páginas de erro (incluindo a do "cachorro")
        error_selectors_check = [
            (By.XPATH, "//img[contains(@alt, 'Desculpe') or contains(@alt, 'Sorry')]"), # Imagem do cachorro
            (By.XPATH, "//*[contains(text(), 'Algo deu errado')]"),
            (By.XPATH, "//*[contains(text(), 'Desculpe-nos')]"),
            (By.XPATH, "//*[contains(text(), 'Serviço Indisponível')]"),
            (By.CSS_SELECTOR, "div#g"), 
        ]
        if not error_page_detected: # Só checa seletores se o título não indicou erro claro
            for by, selector in error_selectors_check:
                try:
                    element = driver.find_element(by, selector)
                    current_run_logger.warning(f"Página de erro detectada por elemento: {selector} | Texto (se houver): {element.text[:100] if element.text else 'N/A'}")
                    error_page_detected = True
                    break 
                except NoSuchElementException:
                    continue
                except StaleElementReferenceException:
                     current_run_logger.warning(f"Elemento {selector} ficou obsoleto ao checar página de erro.")
                     continue
        
        # Se não detectou erro e não tem o contêiner principal de resultados, pode ser um erro sutil
        if not error_page_detected:
            try:
                driver.find_element(By.CSS_SELECTOR, SELETOR_RESULTADOS_CONT) # SELETOR_RESULTADOS_CONT precisa estar definido globalmente
                current_run_logger.debug("Contêiner de resultados encontrado. Aparentemente não é página de erro.")
            except NoSuchElementException:
                # Se o título também não indicou erro, pode ser uma página vazia, não necessariamente uma "página de erro da Amazon"
                current_run_logger.warning(f"Contêiner de resultados '{SELETOR_RESULTADOS_CONT}' NÃO encontrado. Pode ser página de resultados vazia ou erro sutil.")
                # Não definir error_page_detected = True automaticamente para não confundir com erro de bloqueio vs. busca sem resultados
        
        return error_page_detected

    except Exception as e:
        current_run_logger.error(f"Erro ao verificar página de erro da Amazon: {e}", exc_info=True)
        return True 
    finally:
        if error_page_detected and driver.current_url:
            timestamp_error = datetime.now().strftime('%Y%m%d_%H%M%S')
            screenshot_path = os.path.join(DEBUG_LOGS_DIR_BASE, f"amazon_error_page_{timestamp_error}.png")
            html_path = os.path.join(DEBUG_LOGS_DIR_BASE, f"amazon_error_page_{timestamp_error}.html")
            try:
                driver.save_screenshot(screenshot_path)
                current_run_logger.info(f"Screenshot da página de erro salvo em: {screenshot_path}")
                with open(html_path, "w", encoding="utf-8") as f_html_err: # Nome de variável diferente
                    f_html_err.write(driver.page_source)
                current_run_logger.info(f"HTML da página de erro salvo em: {html_path}")
            except Exception as e_save_err: # Nome de variável diferente
                current_run_logger.error(f"Erro ao salvar debug da página de erro: {e_save_err}")

def wait_for_page_load(driver, logger_param, timeout=60):
    logger_param.debug(f"Aguardando carregamento completo da página (timeout={timeout}s)...")
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        logger_param.info("Página carregada (document.readyState is 'complete').")
    except TimeoutException:
        logger_param.warning("Timeout ao esperar carregamento completo da página.")
    except Exception as e:
        logger_param.error(f"Erro ao esperar carregamento da página: {e}", exc_info=True)

if __name__ == "__main__":
    # Para testes locais, você pode definir as variáveis de ambiente aqui ou externamente
    # Ex: os.environ["MAX_PAGINAS_USADOS_GERAL"] = "1" 
    # os.environ["TELEGRAM_TOKEN"] = "SEU_TOKEN"
    # os.environ["TELEGRAM_CHAT_ID"] = "SEU_CHAT_ID"
    # os.environ["PROXY_HOST"] = "seu_proxy_host" # opcional para teste
    # os.environ["PROXY_PORT"] = "sua_proxy_porta" # opcional para teste

    # Atualiza MAX_PAGINAS_POR_LINK_GLOBAL se a variável de ambiente foi alterada após a definição inicial
    # Isso é mais para garantir que o valor usado no loop principal seja o mais atual do ambiente
    current_max_pages_env = os.getenv("MAX_PAGINAS_USADOS_GERAL")
    if current_max_pages_env:
        try:
            MAX_PAGINAS_POR_LINK_GLOBAL = int(current_max_pages_env)
            logger.info(f"MAX_PAGINAS_POR_LINK_GLOBAL atualizado para: {MAX_PAGINAS_POR_LINK_GLOBAL} (via env var no __main__)")
        except ValueError:
            logger.warning(f"Valor inválido para MAX_PAGINAS_USADOS_GERAL no __main__: '{current_max_pages_env}'. Usando o valor inicial: {MAX_PAGINAS_POR_LINK_GLOBAL}")
    
    asyncio.run(run_usados_geral_scraper_async())
