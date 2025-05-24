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
for lib_logger_name in ["webdriver_manager", "httpx", "telegram.bot", "telegram.ext", "bs4", "urllib3.connectionpool", "selenium.webdriver.remote.remote_connection"]:
    logging.getLogger(lib_logger_name).setLevel(logging.WARNING)

logger = logging.getLogger("SCRAPER_USADOS_GERAL")

# --- Configurações do Scraper ---
SELETOR_ITEM_PRODUTO_USADO = "div.s-result-item.s-asin"
SELETOR_INDICADOR_USADO_XPATH = (
    ".//span[contains(translate(., 'OFERTA DE PRODUTO USADO', 'oferta de produto usado'), 'oferta de produto usado') or "
    "contains(translate(., 'OFERTAS DE PRODUTOS USADOS', 'ofertas de produtos usados'), 'ofertas de produtos usados') or "
    "contains(translate(., 'USADO COMO NOVO', 'usado como novo'), 'usado como novo') or "
    "(ancestor::div[@data-cy='secondary-offer-recipe'] and (contains(translate(., 'USADO', 'usado'), 'usado') or contains(translate(., 'USADA', 'usada'), 'usada')) ) or "
    "(.//div[contains(@class, 's-price-instructions-style')]//a//span[contains(translate(., 'USADO', 'usado'), 'usado')])"
    "]"
)
logger.info(f"Usando SELETOR_INDICADOR_USADO_XPATH: {SELETOR_INDICADOR_USADO_XPATH}")
SELETOR_RESULTADOS_CONT = "div.s-main-slot.s-result-list.s-search-results.sg-row"

URL_GERAL_USADOS_BASE = (
    "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011"
    "&rh=n%3A24669725011&s=popularity-rank&fs=true&xpid=71AiW8sVquI1l"
)
NOME_FLUXO_BASE = "Amazon Quase Novo"

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

MAX_PAGINAS_POR_FLUXO = int(os.getenv("MAX_PAGINAS_USADOS_POR_FLUXO", "13"))
logger.info(f"Máximo de páginas por fluxo de categoria/ordenação: {MAX_PAGINAS_POR_FLUXO}")

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

def escape_md(text):
    escape_chars = r'([_\*\[\]\(\)~`>#+\-=|{}.!])'
    return re.sub(escape_chars, r'\\\1', str(text))

def apagar_historico_usados():
    """Apaga o arquivo de histórico de produtos usados."""
    history_path = os.path.join(HISTORY_DIR_BASE, HISTORY_FILENAME_USADOS_GERAL)
    try:
        if os.path.exists(history_path):
            os.remove(history_path)
            logger.info(f"Arquivo de histórico '{history_path}' apagado com sucesso.")
        else:
            logger.info(f"Arquivo de histórico '{history_path}' não encontrado. Nada a apagar.")
    except Exception as e:
        logger.error(f"Erro ao tentar apagar o arquivo de histórico '{history_path}': {e}", exc_info=True)

async def extract_category_links(driver, page_url, logger_param):
    logger_param.info(f"Extraindo links de categoria de: {page_url}")
    category_links = []
    try:
        await asyncio.to_thread(driver.get, page_url)
        await asyncio.sleep(random.uniform(4, 7)) 
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        
        department_heading = soup.find('h1', string='Departamento')
        department_list_ul = None
        if department_heading:
            department_group_div = department_heading.find_parent('div', role='group')
            if department_group_div:
                department_list_ul = department_group_div.find('ul', class_=re.compile(r'a-unordered-list'))
        
        if not department_list_ul:
            department_list_ul = soup.select_one('div[id*="departments"] ul.a-nostyle') or \
                                 soup.select_one('div[data-cel-widget*="refinements"] ul#s-refinements')

        if department_list_ul:
            list_items = department_list_ul.find_all('li', class_=re.compile(r'apb-browse-refinements-indent-2|a-spacing-micro|s-navigation-indent-2'))
            
            count = 0
            for item_li in list_items:
                link_tag = item_li.find('a', class_='a-link-normal', href=re.compile(r'/s\?'))
                if link_tag:
                    span_tag = link_tag.find('span', dir='auto')
                    category_name = span_tag.get_text(strip=True) if span_tag else link_tag.get_text(strip=True)
                    
                    href = link_tag.get('href')
                    if href and category_name:
                        if not href.startswith('http'):
                            href = f"https://www.amazon.com.br{href}"
                        
                        parsed_href = urlparse(href)
                        query_params_href = parse_qs(parsed_href.query)
                        
                        parsed_base_url = urlparse(URL_GERAL_USADOS_BASE)
                        base_query_params = parse_qs(parsed_base_url.query)

                        query_params_href['i'] = base_query_params.get('i', ['warehouse-deals'])
                        query_params_href['srs'] = base_query_params.get('srs', ['24669725011'])
                        
                        current_rh_list = query_params_href.get('rh', [])
                        current_rh = current_rh_list[0] if current_rh_list else ''
                        warehouse_node_rh = 'n:24669725011' 
                        
                        if warehouse_node_rh not in current_rh:
                            cat_node_match = re.search(r'n%3A(\d+)', parsed_href.query) 
                            if cat_node_match:
                                specific_cat_node = cat_node_match.group(1)
                                if specific_cat_node != base_query_params.get("bbn", [""])[0]:
                                     query_params_href['rh'] = [f'{base_query_params.get("rh",["n%3A24669725011"])[0].split("%3A")[1]}%2Cn%3A{specific_cat_node}']
                            else: 
                                query_params_href['bbn'] = base_query_params.get('bbn', ['24669725011'])
                        
                        query_params_href.pop('qid', None)
                        query_params_href.pop('ref', None)
                        query_params_href.pop('s', None)
                        query_params_href.pop('page', None)

                        clean_href_query = urlencode(query_params_href, doseq=True)
                        clean_href = urlunparse(parsed_href._replace(query=clean_href_query))

                        if category_name.lower() in ["amazon quase novo", "todas", "departamento"]:
                            logger_param.info(f"Ignorando categoria genérica: {category_name}")
                            continue

                        category_links.append({'name': category_name, 'url': clean_href})
                        logger_param.info(f"Categoria encontrada: {category_name} -> {clean_href}")
                        count +=1
            if count == 0:
                 logger_param.warning(f"Nenhum link de categoria válido encontrado após filtragem.")
        else:
            logger_param.warning("Elemento <ul> da lista de departamentos não encontrado com os seletores tentados.")

    except Exception as e:
        logger_param.error(f"Erro ao extrair links de categoria: {e}", exc_info=True)
    
    if not category_links:
        logger_param.error("Nenhuma categoria foi extraída. Verifique os seletores em `extract_category_links` e o HTML da página de origem.")
    return category_links


async def process_used_products_geral_async(driver, base_url, nome_fluxo, history, logger, max_paginas=MAX_PAGINAS_POR_FLUXO):
    logger.info(f"--- Iniciando processamento para: {nome_fluxo} --- URL base: {base_url} ---")
    total_produtos_usados_qualificados_nesta_execucao_fluxo = 0 
    pagina_atual = 1
    max_tentativas_pagina = 3
    consecutive_empty_pages = 0
    max_consecutive_empty_pages = 3

    logger.info(f"Máximo de páginas para este fluxo '{nome_fluxo}': {max_paginas}")

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
                    page_dump_filename = f"page_dump_p{pagina_atual}_fluxo_{nome_fluxo.replace(' ', '_').replace('/', '-')}_{timestamp_page_dump}.html"
                    page_dump_path = os.path.join(DEBUG_LOGS_DIR_BASE, page_dump_filename)
                    with open(page_dump_path, "w", encoding="utf-8") as f_html_dump:
                        f_html_dump.write(driver.page_source)
                    logger.info(f"HTML da página {pagina_atual} salvo em: {page_dump_path}")
                except Exception as e_save_dump:
                    logger.error(f"Erro ao salvar o HTML da página {pagina_atual}: {e_save_dump}")

                if check_captcha_sync_worker(driver, logger):
                    logger.error(f"[{nome_fluxo}] CAPTCHA detectado na página {pagina_atual}. Interrompendo fluxo para {nome_fluxo}.")
                    return total_produtos_usados_qualificados_nesta_execucao_fluxo

                if check_amazon_error_page_sync_worker(driver, logger):
                    logger.error(f"[{nome_fluxo}] Página de erro da Amazon detectada na página {pagina_atual}.")
                    if tentativa < max_tentativas_pagina:
                        logger.info("Tentando novamente após delay...")
                        await asyncio.sleep(random.uniform(10, 20))
                        continue
                    else:
                        logger.error(f"[{nome_fluxo}] Falha ao carregar página de produtos após {max_tentativas_pagina} tentativas devido a página de erro. Interrompendo {nome_fluxo}.")
                        return total_produtos_usados_qualificados_nesta_execucao_fluxo
                
                try:
                    WebDriverWait(driver, 20).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, SELETOR_RESULTADOS_CONT))
                    )
                    logger.info(f"Contêiner de resultados '{SELETOR_RESULTADOS_CONT}' encontrado na página {pagina_atual}.")
                except TimeoutException:
                    logger.warning(f"Contêiner de resultados '{SELETOR_RESULTADOS_CONT}' não encontrado na página {pagina_atual} após timeout.")
                    
                items_selenium = driver.find_elements(By.CSS_SELECTOR, SELETOR_ITEM_PRODUTO_USADO)
                logger.info(f"Página {pagina_atual}: Encontrados {len(items_selenium)} elementos com seletor Selenium '{SELETOR_ITEM_PRODUTO_USADO}'.")

                if not items_selenium:
                    logger.info(f"Página {pagina_atual} não contém produtos com o seletor principal para {nome_fluxo}. Verificando se é o fim.")
                    next_button_disabled = False
                    try:
                        driver.find_element(By.CSS_SELECTOR, ".s-pagination-item.s-pagination-next.s-pagination-disabled")
                        logger.info(f"Botão 'Próximo' está desabilitado para {nome_fluxo}. Fim da paginação.")
                        next_button_disabled = True
                    except NoSuchElementException:
                        logger.debug("Botão 'Próximo' não está desabilitado ou não foi encontrado.")
                    
                    if next_button_disabled:
                        return total_produtos_usados_qualificados_nesta_execucao_fluxo
                    
                    consecutive_empty_pages += 1
                    if consecutive_empty_pages >= max_consecutive_empty_pages:
                        logger.warning(f"{max_consecutive_empty_pages} páginas vazias consecutivas em {nome_fluxo}. Considerando fim da busca.")
                        return total_produtos_usados_qualificados_nesta_execucao_fluxo
                    logger.info(f"Página {pagina_atual} vazia em {nome_fluxo}, mas não é o fim. Tentativa {consecutive_empty_pages}/{max_consecutive_empty_pages}.")
                    page_processed_successfully = True 
                    break 

                consecutive_empty_pages = 0 
                produtos_processados_e_notificados_na_pagina = 0

                for idx, item_element_selenium in enumerate(items_selenium, 1):
                    item_logger = logging.getLogger(f"{logger.name}.Item_{pagina_atual}_{idx}")
                    item_logger.debug(f"Processando bloco de item {idx} da página {pagina_atual}")
                    
                    nome, link, asin, price = None, None, None, None
                    preco_historico_val_para_msg = None 
                    notificar_este_produto = False

                    try:
                        try:
                            indicador_usado_el = item_element_selenium.find_element(By.XPATH, SELETOR_INDICADOR_USADO_XPATH)
                            item_logger.debug(f"Indicador de 'usado' encontrado via XPath: '{indicador_usado_el.text.strip() if indicador_usado_el.text else 'Indicador presente (sem texto direto no elemento XPath)'}'")
                        except NoSuchElementException:
                            data_asin_sel = item_element_selenium.get_attribute('data-asin')
                            item_logger.debug(f"Item (ASIN Sel: {data_asin_sel if data_asin_sel else 'N/A'}) NÃO é uma listagem direta de 'usado' ou não tem oferta de usado clara (XPath '{SELETOR_INDICADOR_USADO_XPATH}' não encontrado). Ignorando este item.")
                            continue

                        item_html = item_element_selenium.get_attribute('outerHTML')
                        item_soup = BeautifulSoup(item_html, 'html.parser')
                        
                        title_div = item_soup.find('div', {'data-cy': 'title-recipe'})
                        if title_div:
                            h2 = title_div.find('h2')
                            span_nome_tag = h2.find('span') if h2 else None
                            nome = span_nome_tag.get_text(strip=True) if span_nome_tag else None
                        else:
                            nome = None 

                        if not nome:
                            item_logger.debug("Nome do produto vazio (BS). Ignorando.")
                            continue
                        item_logger.debug(f"Nome (BS): '{nome}'")

                        link_tag = item_soup.find('a', href=re.compile(r'/dp/'))
                        if link_tag and link_tag.has_attr('href'):
                            href_val = link_tag['href']
                            link = f"https://www.amazon.com.br{href_val}" if href_val.startswith("/") else href_val
                            item_logger.debug(f"Link (BS): '{link}'")
                        else:
                            item_logger.warning("Link principal do produto não encontrado. Ignorando item.")
                            continue

                        asin_match = re.search(r'/dp/([A-Z0-9]{10})', link)
                        if asin_match:
                            asin = asin_match.group(1)
                            item_logger.debug(f"ASIN (BS): '{asin}'")
                        else:
                            data_asin_value = item_element_selenium.get_attribute('data-asin')
                            if data_asin_value and len(data_asin_value) == 10:
                                asin = data_asin_value
                                item_logger.debug(f"ASIN (BS, fallback de data-asin): '{asin}'")
                            else:
                                item_logger.warning(f"ASIN não encontrado no link '{link}' nem via data-asin. Ignorando item.")
                                continue
                        
                        price_text_bs = None
                        secondary_offer_div = item_soup.find('div', {'data-cy': 'secondary-offer-recipe'})
                        if secondary_offer_div:
                            span_price_in_secondary = secondary_offer_div.find('span', class_='a-color-base')
                            if span_price_in_secondary:
                                price_text_bs = span_price_in_secondary.get_text(strip=True)
                                item_logger.debug(f"Preço (BS, via 'secondary-offer-recipe'): '{price_text_bs}'")
                        
                        if not price_text_bs:
                            price_instructions_div_bs = item_soup.find('div', class_='s-price-instructions-style')
                            if price_instructions_div_bs:
                                price_link_tag_bs = price_instructions_div_bs.find('a', href=re.compile(r'/gp/offer-listing/'))
                                if price_link_tag_bs:
                                    price_span_offscreen_bs = price_link_tag_bs.find('span', class_='a-offscreen')
                                    if price_span_offscreen_bs:
                                        price_text_bs = price_span_offscreen_bs.get_text(strip=True)
                                        item_logger.debug(f"Preço (BS, via 's-price-instructions-style' > 'a-offscreen'): '{price_text_bs}'")
                        
                        if not price_text_bs:
                            item_logger.debug("Preço não encontrado em estruturas específicas. Usando iteração genérica de spans.")
                            for span_tag in item_soup.find_all('span'):
                                text = span_tag.get_text(strip=True)
                                if text.startswith('R$'):
                                    price_text_bs = text
                                    item_logger.debug(f"Preço (BS, via iteração de span): '{price_text_bs}'")
                                    break 

                        if price_text_bs:
                            match = re.search(r'R\$\s?([\d.,]+)', price_text_bs)
                            if match:
                                cleaned_price_str = match.group(1).replace('.', '').replace(',', '.')
                                try:
                                    price = float(cleaned_price_str)
                                    item_logger.debug(f"Preço final (BS): {price}")
                                except ValueError:
                                    item_logger.warning(f"Erro ao converter preço '{cleaned_price_str}' para float.")
                                    continue
                            else:
                                item_logger.warning(f"Formato de preço inesperado: '{price_text_bs}'. Ignorando item.")
                                continue
                        else:
                            item_logger.warning(f"Preço não encontrado para ASIN {asin}. Ignorando item.")
                            continue

                        if not all([nome, asin, link, price is not None]):
                            item_logger.warning(f"Dados incompletos para ASIN {asin if asin else 'desconhecido'} após extração BS. Ignorando.")
                            continue
                        
                        # Lógica de histórico e decisão de notificação
                        if USAR_HISTORICO:
                            preco_historico_info = history.get(asin)
                            if preco_historico_info:
                                preco_historico_val = preco_historico_info.get("preco_usado")
                                if preco_historico_val and preco_historico_val <= price:
                                    item_logger.info(f"ASIN {asin}: Preço atual (R${price:.2f}) não é menor ou é igual ao histórico (R${preco_historico_val:.2f}). Sem nova notificação.")
                                    produto_existente = history[asin]
                                    produto_existente["timestamp"] = datetime.now().isoformat()
                                    if price > preco_historico_val:
                                        produto_existente["preco_usado"] = price
                                    history[asin] = produto_existente
                                    save_history_geral(history)
                                    continue 
                                else: 
                                    item_logger.info(f"ASIN {asin}: Novo preço (R${price:.2f}) melhor que histórico (R${preco_historico_val if preco_historico_val else 'N/A'}). Notificando.")
                                    notificar_este_produto = True
                                    if preco_historico_val: 
                                        preco_historico_val_para_msg = preco_historico_val
                            else: 
                                item_logger.info(f"ASIN {asin} não está no histórico. Novo produto 'usado' qualificado. Notificando.")
                                notificar_este_produto = True
                        else: 
                             notificar_este_produto = True
                             item_logger.info(f"ASIN {asin}: Processando sem verificação de histórico. Notificando.")


                        if notificar_este_produto:
                            produto_atual_para_historico = {
                                "nome": nome, "asin": asin, "link": link,
                                "preco_usado": price, "timestamp": datetime.now().isoformat(),
                                "fluxo": nome_fluxo
                            }
                            if USAR_HISTORICO:
                                history[asin] = produto_atual_para_historico
                                save_history_geral(history)
                            
                            total_produtos_usados_qualificados_nesta_execucao_fluxo += 1
                            produtos_processados_e_notificados_na_pagina += 1
                            item_logger.info(f"PRODUTO QUALIFICADO PARA NOTIFICAÇÃO: '{nome}' | Preço: R${price:.2f} | ASIN: {asin}")

                            if bot_instance_global and TELEGRAM_CHAT_IDS_LIST:
                                categoria_match = re.search(rf"{NOME_FLUXO_BASE} - (.*?) - (Menor Preço|Maior Preço)", nome_fluxo)
                                nome_categoria_para_msg = categoria_match.group(1) if categoria_match else "Geral"
                                if "Geral (Fallback)" in nome_categoria_para_msg:
                                    nome_categoria_para_msg = "Geral"
                                
                                nome_produto_com_categoria = f"{escape_md(str(nome))} ({escape_md(nome_categoria_para_msg)})"
                                preco_atual_formatado = f"R${price:.2f}"
                                
                                mensagem_telegram = ""
                                
                                if preco_historico_val_para_msg and preco_historico_val_para_msg > price:
                                    preco_antigo_formatado = f"R${preco_historico_val_para_msg:.2f}"
                                    desconto_calculado_str = ""
                                    if preco_historico_val_para_msg > 0:
                                        percentual_desconto = ((preco_historico_val_para_msg - price) / preco_historico_val_para_msg) * 100
                                        # Não escapar a string de desconto aqui, pois ela já está formatada e % não é problemático isoladamente
                                        desconto_calculado_str = f"📉 Desconto: {percentual_desconto:.1f}%\n"

                                    titulo_mensagem = escape_md("↘️ PREÇO BAIXOU! ↙️")
                                    mensagem_telegram = (
                                        f"*{titulo_mensagem}*\n\n"
                                        f"🛒 {nome_produto_com_categoria}\n"
                                        f"💰 De: {escape_md(preco_antigo_formatado)}\n"
                                        f"💸 Por: *{escape_md(preco_atual_formatado)}*\n"
                                        f"{desconto_calculado_str}\n" # Inclui a linha de desconto
                                        f"🔗 [Ver produto]({link})\n\n"
                                        f"🏷️ ASIN: `{escape_md(str(asin))}`\n"
                                        f"🕒 {escape_md(datetime.now().strftime('%d/%m/%Y %H:%M:%S'))}"
                                    )
                                else: 
                                    titulo_mensagem = escape_md("🟡 NOVO NO QUASE NOVO! 🟡")
                                    mensagem_telegram = (
                                        f"*{titulo_mensagem}*\n\n"
                                        f"🛒 {nome_produto_com_categoria}\n"
                                        f"💰 Por: *{escape_md(preco_atual_formatado)}*\n\n"
                                        f"🔗 [Ver produto]({link})\n\n"
                                        f"🏷️ ASIN: `{escape_md(str(asin))}`\n"
                                        f"🕒 {escape_md(datetime.now().strftime('%d/%m/%Y %H:%M:%S'))}"
                                    )

                                for chat_id in TELEGRAM_CHAT_IDS_LIST:
                                    await send_telegram_message_async(
                                        bot_instance_global, chat_id, mensagem_telegram, ParseMode.MARKDOWN_V2, item_logger
                                    )
                    
                    except StaleElementReferenceException:
                        item_logger.warning("Elemento Selenium tornou-se obsoleto. Tentando buscar itens novamente na página.")
                        break 
                    except Exception as e_item_proc:
                        item_logger.error(f"Erro inesperado ao processar bloco de item {idx}: {e_item_proc}", exc_info=True)
                        continue

                if produtos_processados_e_notificados_na_pagina > 0:
                    logger.info(f"Página {pagina_atual}: {produtos_processados_e_notificados_na_pagina} produtos qualificados e notificados para o fluxo {nome_fluxo}.")
                else:
                    logger.info(f"Página {pagina_atual}: Nenhum produto novo ou com preço melhorado encontrado para notificação no fluxo {nome_fluxo} (após todas as verificações).")
                
                page_processed_successfully = True
                break 

            except WebDriverException as e_wd:
                logger.error(f"Erro de WebDriver ao carregar página {pagina_atual} (Tentativa {tentativa}) no fluxo {nome_fluxo}: {str(e_wd)[:200]}", exc_info=False)
                if tentativa < max_tentativas_pagina:
                    await asyncio.sleep(random.uniform(15, 30))
                    continue
                else:
                    logger.error(f"Falha crítica após {max_tentativas_pagina} tentativas na página {pagina_atual} (WebDriverException) no fluxo {nome_fluxo}. Interrompendo este fluxo.")
                    return total_produtos_usados_qualificados_nesta_execucao_fluxo
            except Exception as e_page:
                logger.error(f"Erro geral ao processar página {pagina_atual} (Tentativa {tentativa}) no fluxo {nome_fluxo}: {e_page}", exc_info=True)
                if tentativa < max_tentativas_pagina:
                    await asyncio.sleep(random.uniform(10, 20))
                    continue
                else:
                    logger.error(f"Falha crítica após {max_tentativas_pagina} tentativas na página {pagina_atual} (Erro Geral) no fluxo {nome_fluxo}. Interrompendo este fluxo.")
                    return total_produtos_usados_qualificados_nesta_execucao_fluxo
        
        if not page_processed_successfully:
            logger.error(f"Não foi possível processar a página {pagina_atual} do fluxo {nome_fluxo} após {max_tentativas_pagina} tentativas. Abortando este fluxo.")
            return total_produtos_usados_qualificados_nesta_execucao_fluxo

        pagina_atual += 1
        if pagina_atual <= max_paginas : 
             await asyncio.sleep(random.uniform(5, 10)) 

    logger.info(
        f"--- Concluído Fluxo: {nome_fluxo}. Máximo de páginas ({max_paginas}) atingido ou fim da paginação. "
        f"Total de produtos qualificados e notificados neste fluxo específico: {total_produtos_usados_qualificados_nesta_execucao_fluxo} ---"
    )
    return total_produtos_usados_qualificados_nesta_execucao_fluxo


async def run_usados_geral_scraper_async():
    logger.info(f"--- [SCRAPER INÍCIO GERAL] ---")
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
        
        logger.info(f"Tentando extrair categorias da URL base: {URL_GERAL_USADOS_BASE}")
        category_urls_data = await extract_category_links(driver, URL_GERAL_USADOS_BASE, logger)

        if not category_urls_data:
            logger.warning("Nenhuma categoria foi extraída. O scraper prosseguirá apenas com a URL geral de 'Quase Novo'.")
            category_urls_data.append({'name': 'Geral (Fallback)', 'url': URL_GERAL_USADOS_BASE})
        
        for cat_data in category_urls_data:
            cat_name = cat_data['name']
            cat_url_base = cat_data['url']
            
            ordenacoes = [
                {'s_param': 'price-asc-rank', 'label': 'Menor Preço'},
                {'s_param': 'price-desc-rank', 'label': 'Maior Preço'}
            ]

            for ordenacao in ordenacoes:
                parsed_cat_url = urlparse(cat_url_base)
                query_params_cat = parse_qs(parsed_cat_url.query)
                query_params_cat['s'] = [ordenacao['s_param']]
                query_params_cat.pop('page', None)
                query_params_cat.pop('qid', None)
                query_params_cat.pop('ref', None)
                
                ordered_cat_url_query = urlencode(query_params_cat, doseq=True)
                ordered_cat_url = urlunparse(parsed_cat_url._replace(query=ordered_cat_url_query))
                
                fluxo_nome_atual = f"{NOME_FLUXO_BASE} - {cat_name} - {ordenacao['label']}"
                
                logger.info(f"Iniciando scraper para: {fluxo_nome_atual} - URL: {ordered_cat_url}")
                await process_used_products_geral_async(
                    driver, ordered_cat_url, fluxo_nome_atual, history, logger, MAX_PAGINAS_POR_FLUXO
                )
                await asyncio.sleep(random.uniform(5, 10))

        logger.info(f"Processamento de todos os fluxos de categoria concluído. Total de ASINs no histórico final: {len(history)}.")

    except Exception as e:
        logger.error(f"Erro catastrófico no scraper geral de usados (run_usados_geral_scraper_async): {e}", exc_info=True)
    finally:
        if driver:
            logger.info("Tentando fechar o driver Selenium...")
            try:
                driver.quit()
                logger.info("Driver Selenium fechado.")
            except Exception as e_quit:
                logger.error(f"Erro ao fechar o driver: {e_quit}", exc_info=True)
        logger.info(f"--- [SCRAPER FIM GERAL] ---")

# ... (demais funções auxiliares: load_proxy_list, test_proxy, get_working_proxy, iniciar_driver_sync_worker, etc. permanecem iguais) ...
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
        if host and port: 
            if not host.startswith("http"): 
                proxy_url = f'http://{username}:{password}@{host}:{port}' if username and password else f'http://{host}:{port}'
                proxy_list.append(proxy_url)
            else: 
                 proxy_list.append(host) 
    
    if not proxy_list:
        logger.warning("Nenhum proxy configurado.")
    else:
        logger.info(f"Carregados {len(proxy_list)} proxies.")
    return proxy_list

def test_proxy(proxy_url, logger_param):
    logger_param.info(f"Testando proxy: {proxy_url}")
    try:
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
    if not proxy_list: 
        logger_param.warning("Lista de proxies vazia. Nenhum proxy para testar.")
        return None
    for proxy_url in proxy_list:
        if test_proxy(proxy_url, logger_param):
            return proxy_url
    logger_param.warning("Nenhum proxy funcional encontrado na lista. Prosseguindo sem proxy.")
    return None

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
    chrome_options.add_argument("--disable-popup-blocking"); chrome_options.add_argument("--mute-audio")
    chrome_options.add_argument("--no-first-run"); chrome_options.add_argument("--disable-webgl"); chrome_options.add_argument("--disable-webrtc")
    chrome_options.add_argument("--disable-features=WebRtcHideLocalIpsWithMdns,PrivacySandboxSettings4,OptimizationHints,InterestGroupStorage")
    chrome_options.add_argument("--lang=pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7")
    
    proxies_available = load_proxy_list()
    working_proxy_url = get_working_proxy(proxies_available, current_run_logger) if proxies_available else None
    proxy_actually_configured = False

    if working_proxy_url:
        current_run_logger.info(f"Configurando proxy para Selenium: {working_proxy_url}")
        chrome_options.add_argument(f'--proxy-server={working_proxy_url}') 
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
                driver = webdriver.Chrome(service=service, options=chrome_options) 
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
        await asyncio.to_thread(driver.execute_script, "window.scrollTo(0, document.body.scrollHeight*0.6);")
        await asyncio.sleep(random.uniform(1, 2))
        await asyncio.to_thread(driver.execute_script, "window.scrollTo(0, document.body.scrollHeight);") 
        await asyncio.sleep(random.uniform(0.5, 1.5))
        await asyncio.to_thread(driver.execute_script, "window.scrollTo(0, 0);") 
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
    query_params['qid'] = [str(int(time.time() * 1000))] 
    query_params['ref'] = [f'sr_pg_{page_number}']
    
    new_query = urlencode(query_params, doseq=True)
    final_url = urlunparse(parsed_url._replace(query=new_query))
    current_run_logger.debug(f"URL da página gerada: {final_url}")
    return final_url

def check_captcha_sync_worker(driver, current_run_logger):
    current_run_logger.debug("Verificando a presença de CAPTCHA.")
    try:
        WebDriverWait(driver, 3).until(EC.any_of( 
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
        error_title_keywords = ["desculpe", "algo deu errado", "sorry", "problema", "serviço indisponível", "error", "não encontrada"]
        if any(keyword in page_title_lower for keyword in error_title_keywords):
            current_run_logger.warning(f"Página de erro detectada pelo título: {driver.title}")
            error_page_detected = True
        
        error_selectors_check = [
            (By.XPATH, "//img[contains(@alt, 'Desculpe') or contains(@alt, 'Sorry')]"), 
            (By.XPATH, "//*[contains(text(), 'Algo deu errado')]"),
            (By.XPATH, "//*[contains(text(), 'Desculpe-nos')]"),
            (By.XPATH, "//*[contains(text(), 'Serviço Indisponível')]"),
            (By.CSS_SELECTOR, "div#g"), 
        ]
        if not error_page_detected: 
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
        
        if not error_page_detected:
            try:
                driver.find_element(By.CSS_SELECTOR, SELETOR_RESULTADOS_CONT) 
                current_run_logger.debug("Contêiner de resultados encontrado. Aparentemente não é página de erro.")
            except NoSuchElementException:
                current_run_logger.warning(f"Contêiner de resultados '{SELETOR_RESULTADOS_CONT}' NÃO encontrado. Pode ser página de resultados vazia ou erro sutil.")
        
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
                with open(html_path, "w", encoding="utf-8") as f_html_err: 
                    f_html_err.write(driver.page_source)
                current_run_logger.info(f"HTML da página de erro salvo em: {html_path}")
            except Exception as e_save_err: 
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
    if os.getenv("APAGAR_HISTORICO_USADOS", "false").lower() == "true":
        logger.info("Variável APAGAR_HISTORICO_USADOS definida como true. Apagando histórico...")
        apagar_historico_usados()

    current_max_pages_env = os.getenv("MAX_PAGINAS_USADOS_POR_FLUXO")
    if current_max_pages_env:
        try:
            MAX_PAGINAS_POR_FLUXO = int(current_max_pages_env)
            logger.info(f"MAX_PAGINAS_POR_FLUXO atualizado para: {MAX_PAGINAS_POR_FLUXO} (via env var no __main__)")
        except ValueError:
            logger.warning(f"Valor inválido para MAX_PAGINAS_USADOS_POR_FLUXO no __main__: '{current_max_pages_env}'. Usando o valor padrão: {MAX_PAGINAS_POR_FLUXO}")
    
    asyncio.run(run_usados_geral_scraper_async())
