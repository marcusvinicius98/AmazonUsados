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
from bs4 import BeautifulSoup # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< IMPORTAR BEAUTIFULSOUP

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
logging.getLogger("bs4").setLevel(logging.WARNING) # Silenciar logs do BS4 se desejar

logger = logging.getLogger("SCRAPER_USADOS_GERAL")

# --- Configurações do Scraper ---
SELETOR_ITEM_PRODUTO_USADO = "div.s-result-item.s-asin" # Usado pelo Selenium para pegar os blocos
# SELETOR_NOME_PRODUTO_USADO = "span.a-size-base-plus.a-color-base.a-text-normal" # Mantido, mas usaremos BS
# SELETOR_PRECO_USADO = "div.s-price-instructions-style a span.a-offscreen" # Mantido, mas usaremos BS

# XPath para identificar diretamente um item como "usado" na listagem
SELETOR_INDICADOR_USADO_XPATH = ".//div[contains(@class, 's-price-instructions-style')]//a//span[contains(translate(., 'USADO', 'usado'), 'usado')]"
SELETOR_RESULTADOS_CONT = "div.s-main-slot.s-result-list"

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

MAX_PAGINAS_POR_LINK_GLOBAL = int(os.getenv("MAX_PAGINAS_USADOS_GERAL", "2"))
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

# ... (funções load_proxy_list, test_proxy, get_working_proxy, iniciar_driver_sync_worker, get_initial_cookies, simulate_scroll, send_telegram_message_async, escape_md permanecem as mesmas) ...

# A função get_price_from_element não será mais usada diretamente se usarmos BS para preço
# def get_price_from_element(element, price_logger): ...

# ... (funções load_history_geral, save_history_geral, get_url_for_page_worker, check_captcha_sync_worker, check_amazon_error_page_sync_worker, wait_for_page_load, check_url_status permanecem as mesmas) ...

async def process_used_products_geral_async(driver, base_url, nome_fluxo, history, logger, max_paginas=MAX_PAGINAS_POR_LINK_GLOBAL):
    logger.info(f"--- Iniciando processamento para: {nome_fluxo} --- URL base: {base_url} ---")
    total_produtos_usados_qualificados = [] # Renomeado para clareza
    pagina_atual = 1
    max_tentativas_pagina = 3
    consecutive_empty_pages = 0
    max_consecutive_empty_pages = 3

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

                # Selenium encontra os blocos principais dos itens
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
                    
                    nome, link, asin, price = None, None, None, None # Resetar variáveis

                    try:
                        # PASSO 1: Verificar se é um item "usado" diretamente listado usando XPath no elemento Selenium
                        try:
                            indicador_usado_el = item_element_selenium.find_element(By.XPATH, SELETOR_INDICADOR_USADO_XPATH)
                            item_logger.debug(f"Indicador direto de 'usado' encontrado via XPath: '{indicador_usado_el.text.strip()}'")
                        except NoSuchElementException:
                            # Se não encontrou o indicador direto de "usado", podemos verificar o padrão "(X ofertas de produtos usados)"
                            # Mas, como discutido, processar isso corretamente exigiria lógica adicional para seguir o link.
                            # Por enquanto, se não for um usado direto, vamos pular para manter a lógica original mais simples.
                            item_logger.debug(f"Item NÃO é uma listagem direta de 'usado' (XPath '{SELETOR_INDICADOR_USADO_XPATH}' não encontrado). Ignorando este item.")
                            continue # Pula para o próximo item_element_selenium

                        # Se chegou aqui, o item foi identificado como "usado" diretamente.
                        # Agora, usamos BeautifulSoup para extrair os detalhes do HTML deste item.
                        item_html = item_element_selenium.get_attribute('outerHTML')
                        item_soup = BeautifulSoup(item_html, 'html.parser')

                        # PASSO 2: Extrair Nome com BeautifulSoup
                        # Usando um seletor comum para o título/nome do produto
                        nome_tag = item_soup.find('span', class_=['a-size-base-plus', 'a-color-base', 'a-text-normal']) # Mais específico
                        if not nome_tag: # Fallback para h2 como sugerido por Grok, se o span falhar
                             nome_tag = item_soup.find('h2', class_='a-size-base-plus')
                        
                        if nome_tag:
                            nome = nome_tag.get_text(strip=True)
                            item_logger.debug(f"Nome (BS): '{nome}'")
                        else:
                            item_logger.warning("Nome não encontrado com BeautifulSoup. Ignorando item.")
                            continue

                        # PASSO 3: Extrair Link e ASIN com BeautifulSoup
                        # O link principal do produto geralmente está num <a> com classes 'a-link-normal s-no-outline'
                        link_tag = item_soup.find('a', class_='a-link-normal s-no-outline', href=re.compile(r'/dp/'))
                        if link_tag and link_tag.has_attr('href'):
                            href_val = link_tag['href']
                            if href_val.startswith("/"):
                                link = f"https://www.amazon.com.br{href_val}"
                            else:
                                link = href_val # Assume que já é completo se não começar com /
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
                        
                        # PASSO 4: Extrair Preço "Usado" com BeautifulSoup
                        # Este é o preço que deve estar associado à oferta "usada" direta
                        # que foi identificada pelo SELETOR_INDICADOR_USADO_XPATH.
                        # O seletor original era: "div.s-price-instructions-style a span.a-offscreen"
                        price_text_bs = None
                        price_instructions_div_bs = item_soup.find('div', class_='s-price-instructions-style')
                        if price_instructions_div_bs:
                            price_link_tag_bs = price_instructions_div_bs.find('a')
                            if price_link_tag_bs:
                                price_span_offscreen_bs = price_link_tag_bs.find('span', class_='a-offscreen')
                                if price_span_offscreen_bs:
                                    price_text_bs = price_span_offscreen_bs.get_text(strip=True)
                                    item_logger.debug(f"Preço (BS, via 's-price-instructions-style'): '{price_text_bs}'")
                        
                        if not price_text_bs: # Fallback para o seletor genérico do Grok, se o específico falhar
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
                            item_logger.warning("Preço 'usado' não encontrado com BeautifulSoup. Ignorando item.")
                            continue

                        # Se todas as extrações foram bem-sucedidas:
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
                                    continue # Pula notificação e adição à lista de qualificados
                                else:
                                    item_logger.info(f"ASIN {asin}: Novo preço (R${price:.2f}) melhor que histórico (R${preco_historico_val if preco_historico_val else 'N/A'}).")
                            else:
                                item_logger.info(f"ASIN {asin} não está no histórico. Novo produto 'usado' qualificado.")
                            
                            history[asin] = produto # Atualiza ou adiciona ao histórico
                            save_history_geral(history)
                        
                        # Adiciona à lista de produtos qualificados desta rodada e incrementa contador
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
                        break # Sai do loop de itens para tentar recarregar a página ou os itens
                    except Exception as e_item_proc:
                        item_logger.error(f"Erro inesperado ao processar bloco de item {idx}: {e_item_proc}", exc_info=True)
                        continue # Pula para o próximo bloco de item

                if produtos_processados_e_notificados_na_pagina > 0:
                    logger.info(f"Página {pagina_atual}: {produtos_processados_e_notificados_na_pagina} produtos 'usados' qualificados, processados e notificados.")
                else:
                    logger.info(f"Página {pagina_atual}: Nenhum produto novo ou com preço melhorado encontrado para notificação (após todas as verificações).")
                
                page_processed_successfully = True
                break # Sai do loop de tentativas da página

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
        driver = iniciar_driver_sync_worker(logger) # Função original para iniciar driver
        if not driver:
            logger.error("Falha crítica ao iniciar o WebDriver. Abortando scraper.")
            return

        logger.info("Driver Selenium iniciado com sucesso.")
        await get_initial_cookies(driver, logger) # Função original
        
        history = {}
        if USAR_HISTORICO:
            history = load_history_geral() # Função original
        
        # A função de processamento foi atualizada para usar BeautifulSoup internamente
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


# As funções que não foram coladas aqui (como iniciar_driver_sync_worker, load_proxy_list, etc.)
# devem ser mantidas como estavam na sua versão anterior do script, pois a adaptação
# com BeautifulSoup é principalmente dentro de `process_used_products_geral_async`.
# Certifique-se de que a função `escape_md` e as outras funções auxiliares estejam presentes.

# Exemplo de como as funções não mostradas aqui seriam (apenas para completude, elas já existem no seu script original):

def load_proxy_list():
    proxy_list = []
    proxy_hosts = os.getenv("PROXY_HOST", "").strip().split(',')
    proxy_ports = os.getenv("PROXY_PORT", "").strip().split(',')
    proxy_usernames = os.getenv("PROXY_USERNAME", "").strip().split(',')
    proxy_passwords = os.getenv("PROXY_PASSWORD", "").strip().split(',')
    for i in range(min(len(proxy_hosts), len(proxy_ports))):
        host = proxy_hosts[i].strip(); port = proxy_ports[i].strip()
        username = proxy_usernames[i].strip() if i < len(proxy_usernames) and proxy_usernames[i].strip() else None
        password = proxy_passwords[i].strip() if i < len(proxy_passwords) and proxy_passwords[i].strip() else None
        if host and port:
            proxy_url = f'http://{username}:{password}@{host}:{port}' if username and password else f'http://{host}:{port}'
            proxy_list.append(proxy_url)
    if not proxy_list:
        logger.warning("Nenhum proxy configurado.")
    else:
        logger.info(f"Carregados {len(proxy_list)} proxies.")
    return proxy_list

def test_proxy(proxy_url, logger_param):
    logger_param.info(f"Testando proxy: {proxy_url}")
    try:
        response = requests.get("https://www.amazon.com.br", proxies={"http": proxy_url, "https": proxy_url}, timeout=10)
        if response.status_code == 200: logger_param.info("Proxy funcional."); return True
        else: logger_param.warning(f"Proxy retornou status: {response.status_code}."); return False
    except requests.RequestException as e: logger_param.error(f"Erro ao testar proxy: {e}"); return False

def get_working_proxy(proxy_list, logger_param):
    for proxy_url in proxy_list:
        if test_proxy(proxy_url, logger_param): return proxy_url
    logger_param.warning("Nenhum proxy funcional encontrado. Prosseguindo sem proxy.")
    return None

def iniciar_driver_sync_worker(current_run_logger, driver_path=None):
    current_run_logger.info("Iniciando configuração do WebDriver...")
    chrome_options = Options()
    chrome_options.add_argument("--headless=new"); chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage"); chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    ua = UserAgent(); user_agent = ua.random; chrome_options.add_argument(f"user-agent={user_agent}")
    current_run_logger.info(f"User-Agent: {user_agent}")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"]); chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument("--disable-extensions"); chrome_options.add_argument("--disable-popup-blocking"); chrome_options.add_argument("--mute-audio")
    chrome_options.add_argument("--no-first-run"); chrome_options.add_argument("--disable-webgl"); chrome_options.add_argument("--disable-webrtc")
    chrome_options.add_argument("--disable-features=WebRtcHideLocalIpsWithMdns,PrivacySandboxSettings4,OptimizationHints,InterestGroupStorage")
    chrome_options.add_argument("--lang=pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7")
    
    proxies = load_proxy_list()
    working_proxy = get_working_proxy(proxies, current_run_logger) if proxies else None
    proxy_configured_for_selenium = False
    if working_proxy:
        current_run_logger.info(f"Configurando proxy para Selenium: {working_proxy}")
        chrome_options.add_argument(f'--proxy-server={working_proxy}')
        proxy_configured_for_selenium = True
    else:
        current_run_logger.warning("Nenhum proxy funcional. WebDriver iniciará sem proxy.")
    current_run_logger.info(f"Opções do Chrome: {chrome_options.arguments}")

    service = None; driver = None
    try:
        path_from_manager = ChromeDriverManager().install()
        service = Service(path_from_manager)
        current_run_logger.info(f"ChromeDriver via Manager: {path_from_manager}")
        driver = webdriver.Chrome(service=service, options=chrome_options)
        current_run_logger.info("WebDriver instanciado.")
        driver.set_page_load_timeout(120)
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {'source': "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"})
        return driver
    except WebDriverException as e:
        if "ERR_NO_SUPPORTED_PROXIES" in str(e) and proxy_configured_for_selenium:
            current_run_logger.error(f"Erro de proxy não suportado ({working_proxy}). Tentando sem proxy.")
            chrome_options.arguments = [arg for arg in chrome_options.arguments if not arg.startswith('--proxy-server')]
            driver = webdriver.Chrome(service=service, options=chrome_options)
            current_run_logger.info("WebDriver instanciado sem proxy após falha com proxy.")
            driver.set_page_load_timeout(120)
            driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {'source': "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"})
            return driver
        else:
            current_run_logger.error(f"Erro WebDriverException: {e}", exc_info=True); raise
    except Exception as e:
        current_run_logger.error(f"Erro ao iniciar WebDriver: {e}", exc_info=True); raise

async def get_initial_cookies(driver, logger_param):
    logger_param.info("Acessando página inicial para obter cookies...")
    try:
        await asyncio.to_thread(driver.get, "https://www.amazon.com.br")
        await asyncio.sleep(random.uniform(3, 5))
        await asyncio.to_thread(wait_for_page_load, driver, logger_param)
        logger_param.info("Cookies iniciais obtidos.")
    except Exception as e: logger_param.error(f"Erro ao obter cookies: {e}", exc_info=True)

async def simulate_scroll(driver, logger_param):
    logger_param.debug("Simulando rolagem...")
    try:
        await asyncio.to_thread(driver.execute_script, "window.scrollTo(0, document.body.scrollHeight);")
        await asyncio.sleep(random.uniform(1, 2))
        await asyncio.to_thread(driver.execute_script, "window.scrollTo(0, 0);")
    except Exception as e: logger_param.error(f"Erro na rolagem: {e}", exc_info=True)

async def send_telegram_message_async(bot, chat_id, message, parse_mode, msg_logger):
    if not bot: msg_logger.error("Bot não fornecido."); return False
    try:
        await bot.send_message(chat_id=chat_id, text=message, parse_mode=parse_mode)
        msg_logger.info(f"Notificação Telegram enviada para CHAT_ID {chat_id}.")
        return True
    except Exception as e: msg_logger.error(f"Erro ao enviar msg Telegram para {chat_id}: {e}", exc_info=True); return False

def escape_md(text): return re.sub(r'([_\*\[\]\(\)~`>#+\-=|{}.!])', r'\\\1', str(text))
def load_history_geral():
    path = os.path.join(HISTORY_DIR_BASE, HISTORY_FILENAME_USADOS_GERAL)
    logger.info(f"Carregando histórico de: {path}")
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f: data = json.load(f)
            logger.info(f"Histórico carregado: {len(data)} ASINs."); return data
        except Exception as e: logger.error(f"Erro ao carregar histórico '{path}': {e}. Retornando vazio."); return {}
    logger.info("Arquivo de histórico não encontrado. Retornando vazio."); return {}

def save_history_geral(history):
    path = os.path.join(HISTORY_DIR_BASE, HISTORY_FILENAME_USADOS_GERAL)
    logger.info(f"Salvando histórico ({len(history)} ASINs) em: {path}")
    try:
        with open(path, 'w', encoding='utf-8') as f: json.dump(history, f, ensure_ascii=False, indent=2)
        logger.info("Histórico salvo.")
    except Exception as e: logger.error(f"Erro ao salvar histórico '{path}': {e}", exc_info=True)

def get_url_for_page_worker(base_url, page_number, current_run_logger):
    parsed = urlparse(base_url); query_params = parse_qs(parsed.query)
    query_params['page'] = [str(page_number)]; query_params['qid'] = [str(int(time.time() * 1000))]
    query_params['ref'] = [f'sr_pg_{page_number}']
    final_url = urlunparse(parsed._replace(query=urlencode(query_params, doseq=True)))
    current_run_logger.debug(f"URL da página {page_number}: {final_url}"); return final_url

def check_captcha_sync_worker(driver, current_run_logger):
    current_run_logger.debug("Verificando CAPTCHA.")
    try:
        WebDriverWait(driver, 3).until(EC.any_of( # Timeout curto
            EC.presence_of_element_located((By.CSS_SELECTOR, "form[action*='captcha'] img")),
            EC.presence_of_element_located((By.XPATH, "//h4[contains(text(), 'Insira os caracteres')]"))
        ))
        current_run_logger.warning(f"CAPTCHA detectado! URL: {driver.current_url}")
        # Salvar screenshot e HTML é uma boa prática aqui (código omitido para brevidade, mas está no seu original)
        return True
    except: current_run_logger.debug("Nenhum CAPTCHA detectado (ou timeout curto)."); return False

def check_amazon_error_page_sync_worker(driver, current_run_logger):
    current_run_logger.debug("Verificando página de erro Amazon.")
    title = driver.title.lower()
    if any(err_word in title for err_word in ["algo deu errado", "sorry", "problema"]):
        current_run_logger.warning(f"Página de erro detectada pelo título: {title}")
        # Salvar screenshot e HTML (código omitido)
        return True
    try: # Checar ausência do container de resultados como um forte indicativo de erro
        driver.find_element(By.CSS_SELECTOR, SELETOR_RESULTADOS_CONT)
    except NoSuchElementException:
        current_run_logger.warning(f"Contêiner de resultados '{SELETOR_RESULTADOS_CONT}' não encontrado. Pode ser página de erro/vazia.")
        # Salvar screenshot e HTML (código omitido)
        return True # Considerar erro se o contêiner principal não existe
    return False

def wait_for_page_load(driver, logger_param, timeout=60): # Timeout reduzido
    logger_param.debug(f"Aguardando carregamento (timeout={timeout}s)...")
    try:
        WebDriverWait(driver, timeout).until(lambda d: d.execute_script("return document.readyState") == "complete")
        logger_param.info("Página carregada (document.readyState 'complete').")
    except TimeoutException: logger_param.warning("Timeout ao esperar carregamento completo.")
    except Exception as e: logger_param.error(f"Erro ao esperar carregamento: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(run_usados_geral_scraper_async())
