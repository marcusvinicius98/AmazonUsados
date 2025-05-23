import os
import re
import logging
import asyncio
import json
import unicodedata
import glob
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
# !!! ATENÇÃO: VERIFIQUE E AJUSTE ESTES SELETORES CUIDADOSAMENTE !!!
# Estes seletores foram baseados no HTML fornecido anteriormente para "Um defeito de cor".
# A extração da CONDIÇÃO específica ("Como Novo", "Bom") é o mais crítico e pode variar.

SELETOR_ITEM_PRODUTO_USADO = "div.s-result-item[data-asin]"

# Nome e Link (baseado no snippet HTML fornecido)
SELETOR_NOME_PRODUTO_USADO = "div[data-cy='title-recipe'] h2.a-size-base-plus > span"
SELETOR_LINK_PRODUTO_USADO = "div[data-cy='title-recipe'] > a.a-link-normal"

# Preço (baseado no snippet, dentro de 'secondary-offer-recipe')
SELETOR_PRECO_USADO_DENTRO_DO_ITEM = "div[data-cy='secondary-offer-recipe'] span.a-color-base"

# Condição
# Este seletor pega o texto do link que indica o número de ofertas usadas (ex: "(1 oferta de produto usado)").
# Se a CONDIÇÃO ESPECÍFICA (ex: "Usado - Bom") estiver visível na listagem para cada item,
# DESCOMENTE E PREENCHA o SELETOR_CONDICAO_ESPECIFICA_USADO abaixo.
SELETOR_INDICADOR_USADO_TEXTO = "div[data-cy='secondary-offer-recipe'] a"
# SELETOR_CONDICAO_ESPECIFICA_USADO = "SEU_SELETOR_AQUI_PARA_CONDICAO_EXATA_VISIVEL_NA_LISTAGEM"


# Link fornecido pelo usuário para produtos USADOS da Amazon Warehouse Deals
USED_PRODUCTS_LINK = "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011&rh=n%3A24669725011&s=popularity-rank&fs=true&page=1&qid=1747998790&xpid=M2soDZTyDMNhF&ref=sr_pg_1"

CATEGORIES = [ # Para o script de usados, teremos apenas uma "categoria" que é a fonte de usados
    {"name": "Amazon Usados - Warehouse", "safe_name": "Amazon_Usados_Warehouse", "url": USED_PRODUCTS_LINK},
]

MIN_DESCONTO_USADOS_STR = os.getenv("MIN_DESCONTO_PERCENTUAL_USADOS", "40").strip()
try:
    MIN_DESCONTO_USADOS = int(MIN_DESCONTO_USADOS_STR)
    if not (0 <= MIN_DESCONTO_USADOS <= 100): MIN_DESCONTO_USADOS = 40
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
if TELEGRAM_TOKEN and TELEGRAM_CHAT_IDS_LIST:
    try:
        bot_instance_global = Bot(token=TELEGRAM_TOKEN)
        logger.info(f"Instância global do Bot Telegram criada para USADOS. IDs de Chat: {TELEGRAM_CHAT_IDS_LIST}")
    except Exception as e:
        logger.error(f"Falha ao inicializar Bot global para USADOS: {e}", exc_info=True)
else:
    logger.warning("TELEGRAM_TOKEN ou TELEGRAM_CHAT_ID(s) globais não configurados ou inválidos. Notificações Telegram para USADOS desabilitadas.")

def create_safe_filename(name_str):
    normalized_name = unicodedata.normalize('NFKD', name_str).encode('ascii', 'ignore').decode('ascii')
    safe_name = re.sub(r'[^\w\s-]', '', normalized_name).strip()
    safe_name = re.sub(r'[-\s]+', '_', safe_name)
    return safe_name

async def send_telegram_message_async(bot, chat_id, text, parse_mode=None, specific_logger=None):
    local_logger = specific_logger if specific_logger else logger
    if not bot:
        local_logger.error("Bot não inicializado ao tentar enviar mensagem.")
        return False
    try:
        await bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode)
        return True
    except TelegramError as te:
        local_logger.error(f"Erro Telegram ao enviar para {chat_id}: {te.message}", exc_info=False)
        if "Too Many Requests" in te.message or "retry after" in te.message.lower():
            retry_after_match = re.search(r"retry after (\d+)", te.message.lower())
            wait_time = int(retry_after_match.group(1)) + 1 if retry_after_match else 5
            local_logger.warning(f"Rate limit do Telegram para {chat_id}, aguardando {wait_time}s.")
            await asyncio.sleep(wait_time)
            try:
                await bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode)
                local_logger.info(f"Reenvio para {chat_id} após rate limit bem-sucedido.")
                return True
            except TelegramError as te_retry:
                local_logger.error(f"Erro Telegram no REENVIO para {chat_id} após rate limit: {te_retry.message}")
                return False
        return False
    except Exception as e:
        local_logger.error(f"Erro GERAL ao enviar msg para {chat_id}: {e}", exc_info=True)
        return False

def escape_md(text):
    if not isinstance(text, str): text = str(text)
    escape_chars = r'([_*[\]()~`>#+\-=|{}.!])'
    return re.sub(escape_chars, r'\\\1', text)

def iniciar_driver_sync_worker(specific_logger, driver_executable_path_param=None):
    options = Options()
    options.add_argument("--headless=new"); options.add_argument("--disable-gpu"); options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage"); options.add_argument("--window-size=1920,1080"); options.add_argument("--lang=pt-BR,en-US;q=0.9,en;q=0.8")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
    options.add_experimental_option("excludeSwitches", ["enable-automation"]); options.add_experimental_option('useAutomationExtension', False)
    service = None
    if driver_executable_path_param and os.path.exists(driver_executable_path_param) and os.access(driver_executable_path_param, os.X_OK):
        specific_logger.info(f"Usando chromedriver globalmente fornecido em: {driver_executable_path_param}")
        service = Service(executable_path=driver_executable_path_param)
    else:
        if driver_executable_path_param: specific_logger.warning(f"Caminho do chromedriver global fornecido ({driver_executable_path_param}) é inválido ou inacessível.")
        service_path_local = None
        common_paths = ["/usr/bin/chromedriver", "/usr/local/bin/chromedriver", os.path.expanduser("~/bin/chromedriver")]
        for path_check in common_paths:
            if os.path.exists(path_check) and os.access(path_check, os.X_OK): service_path_local = path_check; break
        if service_path_local:
            specific_logger.info(f"Usando chromedriver local encontrado em: {service_path_local}")
            service = Service(executable_path=service_path_local)
        else:
            try:
                specific_logger.info("Nenhum chromedriver pré-configurado/local encontrado. WebDriverManager (fallback no worker) iniciando.")
                path_from_manager = ChromeDriverManager().install()
                specific_logger.info(f"WebDriverManager (fallback no worker) configurou o driver em: {path_from_manager}")
                service = Service(executable_path=path_from_manager)
            except Exception as e_wdm_worker: specific_logger.error(f"Falha crítica ao tentar usar WebDriverManager como fallback no worker: {e_wdm_worker}", exc_info=True); raise
    if not service: specific_logger.critical("Não foi possível configurar o Service do ChromeDriver."); raise RuntimeError("Falha ao inicializar o Service do ChromeDriver.")
    driver = webdriver.Chrome(service=service, options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def check_captcha_sync_worker(driver, category_name_for_log, specific_logger):
    try:
        WebDriverWait(driver, 3).until(EC.any_of(
            EC.presence_of_element_located((By.CSS_SELECTOR, "form[action*='captcha'] img")),
            EC.presence_of_element_located((By.XPATH, "//h4[contains(text(), 'Insira os caracteres')]")),
            EC.presence_of_element_located((By.CSS_SELECTOR, "iframe[src*='captcha']")) ))
        specific_logger.warning(f"CAPTCHA detectado em {category_name_for_log}. URL: {driver.current_url}")
        return True
    except (TimeoutException, NoSuchElementException): return False

def get_price_from_direct_text(element_raiz, selector_para_span_de_preco, specific_logger):
    try:
        price_span_list = element_raiz.find_elements(By.CSS_SELECTOR, selector_para_span_de_preco)
        if price_span_list:
            raw_text = price_span_list[0].text 
            if not raw_text: return None
            cleaned_text = re.sub(r'[^\d,]', '', raw_text) 
            if not cleaned_text: return None
            
            cleaned_text = cleaned_text.replace(',', '.') 
            
            if re.match(r'^\d+(\.\d{1,2})?$', cleaned_text):
                return float(cleaned_text)
            if re.match(r'^\d+$', cleaned_text): 
                 return float(cleaned_text)
            specific_logger.warning(f"Texto de preço '{raw_text}' não resultou em float válido após limpeza para '{cleaned_text}'. Seletor: {selector_para_span_de_preco}")
        else:
            specific_logger.debug(f"Nenhum elemento de preço encontrado com seletor '{selector_para_span_de_preco}' dentro do elemento raiz.")
        return None
    except Exception as e:
        specific_logger.error(f"Exceção em get_price_from_direct_text com seletor '{selector_para_span_de_preco}': {e}", exc_info=True)
        return None

def get_url_for_page_worker(base_url, page_number):
    url_parts = list(urlparse(base_url))
    query = dict(parse_qs(url_parts[4]))
    query['page'] = [str(page_number)]
    # Adiciona qid único e ref padrão. Verifique se os parâmetros da URL de usados precisam de algo diferente.
    query['qid'] = [str(int(asyncio.get_event_loop().time() * 1000))] 
    query['ref'] = [f'sr_pg_{page_number}']
    # Remove parâmetros que podem ser específicos da primeira página ou de sessão anterior
    for p in ['xpid', 'srs', 'bbn']: query.pop(p, None)

    url_parts[4] = urlencode(query, doseq=True)
    return urlunparse(url_parts)


async def processar_pagina_real_async(
    driver, url_inicial_categoria, nome_fonte_atual,
    specific_logger,
    price_history_data,
    min_desconto_comparativo, bot_inst, chat_ids_list ):

    history_changed_in_this_run = False
    # Constrói a URL base para paginação a partir da URL inicial da "categoria" (fonte de usados)
    parsed_initial_url = urlparse(url_inicial_categoria)
    query_params_base = parse_qs(parsed_initial_url.query)
    # Mantém apenas os parâmetros essenciais para a busca de usados.
    # 'i' (index), 'rh' (refinement handle), 's' (sort), 'fs' (full-store) parecem importantes.
    # Outros como qid, ref, page, xpid, bbn, srs são geralmente para sessão/pagina/tracking.
    essential_params = {}
    for essential_key in ['i', 'rh', 's', 'fs']: # Adicione outros se necessário
        if essential_key in query_params_base:
            essential_params[essential_key] = query_params_base[essential_key]
    
    cleaned_query_string = urlencode(essential_params, doseq=True)
    base_url_para_paginacao = urlunparse(parsed_initial_url._replace(path="/s", query=cleaned_query_string, fragment=""))


    specific_logger.info(f"--- Processando Fonte: {nome_fonte_atual} --- URL base para paginação: {base_url_para_paginacao} ---")
    paginas_sem_produtos_consecutivas = 0; loop_broken_flag = False; pagina_atual_numero = 0

    for i_pagina in range(1, MAX_PAGINAS_POR_LINK_GLOBAL + 1):
        pagina_atual_numero = i_pagina
        # A primeira página é a URL fornecida, as subsequentes são construídas
        if pagina_atual_numero == 1:
            url_atual = url_inicial_categoria 
        else:
            url_atual = get_url_for_page_worker(base_url_para_paginacao, pagina_atual_numero)
        
        specific_logger.info(f"[{nome_fonte_atual}] Processando URL: {url_atual} (Página: {pagina_atual_numero}/{MAX_PAGINAS_POR_LINK_GLOBAL})")

        try:
            await asyncio.to_thread(driver.get, url_atual); await asyncio.sleep(5) 
        except Exception as e_load_url: specific_logger.error(f"Erro ao carregar {url_atual}: {e_load_url}"); loop_broken_flag = True; break
        if await asyncio.to_thread(check_captcha_sync_worker, driver, nome_fonte_atual, specific_logger): loop_broken_flag = True; break

        try:
            await asyncio.to_thread(WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, SELETOR_ITEM_PRODUTO_USADO))))
        except TimeoutException:
            specific_logger.warning(f"Timeout esperando por '{SELETOR_ITEM_PRODUTO_USADO}' em {url_atual}.")
            try:
                no_results_elements = await asyncio.to_thread(driver.find_elements, By.XPATH, "//span[contains(text(), 'Nenhum resultado') or contains(text(), 'No results for') or contains(., 'não encontrou nenhum resultado')]")
                if no_results_elements and any(el.is_displayed() for el in no_results_elements): # Checa se algum está visível
                    specific_logger.info(f"Página indica 'Nenhum resultado' em {url_atual}. Fim da paginação para {nome_fonte_atual}.")
                    loop_broken_flag = True; break
            except Exception as e_no_res:
                specific_logger.debug(f"Erro ao checar 'Nenhum resultado': {e_no_res}")

            produtos_elements_check = await asyncio.to_thread(driver.find_elements, By.CSS_SELECTOR, SELETOR_ITEM_PRODUTO_USADO)
            if not produtos_elements_check:
                specific_logger.warning(f"Nenhum item ('{SELETOR_ITEM_PRODUTO_USADO}') encontrado após timeout em {url_atual}. Pulando pág.")
                paginas_sem_produtos_consecutivas += 1
                if paginas_sem_produtos_consecutivas >= 2 and pagina_atual_numero > 1: loop_broken_flag = True; break
                continue
            specific_logger.info(f"[{nome_fonte_atual}] {len(produtos_elements_check)} produtos encontrados mesmo após timeout no container.")


        all_item_elements_on_page = await asyncio.to_thread(driver.find_elements, By.CSS_SELECTOR, SELETOR_ITEM_PRODUTO_USADO)
        specific_logger.info(f"[{nome_fonte_atual}] {len(all_item_elements_on_page)} itens ('{SELETOR_ITEM_PRODUTO_USADO}') encontrados na pág {pagina_atual_numero}.")
        if not all_item_elements_on_page:
            paginas_sem_produtos_consecutivas += 1
            if paginas_sem_produtos_consecutivas >= 2 and pagina_atual_numero > 1: loop_broken_flag = True; break
            if pagina_atual_numero < MAX_PAGINAS_POR_LINK_GLOBAL: await asyncio.sleep(1)
            continue
        else: paginas_sem_produtos_consecutivas = 0


        for p_element in all_item_elements_on_page:
            nome_p, link_p_url, preco_p_atual_val, condicao_p_usado = "N/A", "", None, "N/A"; asin_p = "N/A"
            try:
                asin_p = await asyncio.to_thread(p_element.get_attribute, 'data-asin')
                if not asin_p: specific_logger.debug(f"[{nome_fonte_atual}] Item sem ASIN ignorado."); continue

                try:
                    nome_el_list = await asyncio.to_thread(p_element.find_elements, By.CSS_SELECTOR, SELETOR_NOME_PRODUTO_USADO)
                    if nome_el_list: nome_p = (await asyncio.to_thread(nome_el_list[0].text)).strip()[:150]
                    if not nome_p or nome_p == "N/A": specific_logger.warning(f"ASIN {asin_p}: Nome não encontrado ou inválido com seletor '{SELETOR_NOME_PRODUTO_USADO}'."); continue
                except NoSuchElementException: specific_logger.warning(f"ASIN {asin_p}: Exceção ao buscar nome."); continue
                

                try:
                    link_el_list = await asyncio.to_thread(p_element.find_elements, By.CSS_SELECTOR, SELETOR_LINK_PRODUTO_USADO)
                    if link_el_list:
                        link_url_raw_p = await asyncio.to_thread(link_el_list[0].get_attribute, "href")
                        if not link_url_raw_p.startswith("http"): link_url_raw_p = "https://www.amazon.com.br" + link_url_raw_p
                        parsed_link = urlparse(link_url_raw_p)
                        link_p_url = urlunparse(parsed_link._replace(query="", fragment=""))
                        if "/dp/" not in link_p_url and asin_p: link_p_url = f"https://www.amazon.com.br/dp/{asin_p}"
                    else: specific_logger.warning(f"ASIN {asin_p}: Link não encontrado."); link_p_url = f"https://www.amazon.com.br/dp/{asin_p}"
                except NoSuchElementException: specific_logger.warning(f"ASIN {asin_p}: Exceção ao buscar link."); link_p_url = f"https://www.amazon.com.br/dp/{asin_p}"

                preco_p_atual_val = await asyncio.to_thread(get_price_from_direct_text, p_element, SELETOR_PRECO_USADO_DENTRO_DO_ITEM, specific_logger)
                if preco_p_atual_val is None or preco_p_atual_val <= 0:
                    specific_logger.warning(f"ASIN {asin_p}: Preço USADO inválido (R${preco_p_atual_val}) com seletor '{SELETOR_PRECO_USADO_DENTRO_DO_ITEM}'."); continue
                
                condicao_p_usado = "Usado (condição não especificada)" # Default
                try:
                    # Prioriza um seletor para condição específica se o usuário definir e encontrar
                    # Exemplo: SELETOR_CONDICAO_ESPECIFICA_USADO = "span.minha-classe-de-condicao-exata"
                    if 'SELETOR_CONDICAO_ESPECIFICA_USADO' in globals() and SELETOR_CONDICAO_ESPECIFICA_USADO:
                       condicao_especifica_el_list = await asyncio.to_thread(p_element.find_elements, By.CSS_SELECTOR, SELETOR_CONDICAO_ESPECIFICA_USADO)
                       if condicao_especifica_el_list:
                           texto_cond_especifica = (await asyncio.to_thread(condicao_especifica_el_list[0].text)).strip()
                           if texto_cond_especifica: condicao_p_usado = texto_cond_especifica

                    # Se não encontrou condição específica ou o seletor não está definido, usa o SELETOR_INDICADOR_USADO_TEXTO
                    # e verifica se o texto indica "Usado - Condição"
                    if condicao_p_usado == "Usado (condição não especificada)" or not condicao_p_usado.lower().startswith("usado -"):
                        indicador_el_list = await asyncio.to_thread(p_element.find_elements, By.CSS_SELECTOR, SELETOR_INDICADOR_USADO_TEXTO)
                        if indicador_el_list:
                            texto_indicador = (await asyncio.to_thread(indicador_el_list[0].text)).strip()
                            if texto_indicador.lower().startswith("usado -"): # Ex: "Usado - Bom", "Usado - Como Novo"
                                condicao_p_usado = texto_indicador
                            elif "usado" in texto_indicador.lower(): # Ex: "(1 oferta de produto usado)"
                                condicao_p_usado = "Usado (ver detalhes na oferta)"
                            else:
                                specific_logger.debug(f"ASIN {asin_p}: Texto do indicador '{texto_indicador}' não parece ser uma condição de usado.")
                        else:
                            specific_logger.warning(f"ASIN {asin_p}: Indicador de usado/condição não encontrado com seletor '{SELETOR_INDICADOR_USADO_TEXTO}'.")
                except Exception as e_cond:
                    specific_logger.error(f"ASIN {asin_p}: Exceção ao buscar condição: {e_cond}")

                specific_logger.info(f"[{nome_fonte_atual}] ASIN {asin_p}: Nome='{nome_p[:30]}...', Preço=R${preco_p_atual_val:.2f}, Condição='{condicao_p_usado}'")

                entry_hist_p = price_history_data.get(asin_p)
                should_notify_product = False
                notification_reason = ""
                desconto_calculado_para_msg = 0.0
                preco_anterior_para_msg = preco_p_atual_val

                if entry_hist_p is None: 
                    should_notify_product = True
                    notification_reason = "Novo item usado encontrado (primeira vez)."
                    entry_hist_p = {
                        "name": nome_p, "link": link_p_url,
                        "seen_price": preco_p_atual_val, "condition": condicao_p_usado,
                        "notified_on_first_find": True,
                        "last_notified_price_for_drop": None, 
                        "source_last_seen": nome_fonte_atual
                    }
                    price_history_data[asin_p] = entry_hist_p
                    history_changed_in_this_run = True
                else: 
                    last_seen_price_hist = entry_hist_p.get("seen_price")
                    last_condition_hist = entry_hist_p.get("condition") # Pode ser usado para lógica mais fina
                    last_notified_price_drop_hist = entry_hist_p.get("last_notified_price_for_drop")
                    
                    if entry_hist_p.get("name") != nome_p: entry_hist_p["name"] = nome_p; history_changed_in_this_run = True
                    if entry_hist_p.get("link") != link_p_url: entry_hist_p["link"] = link_p_url; history_changed_in_this_run = True
                    entry_hist_p["source_last_seen"] = nome_fonte_atual
                    
                    if last_seen_price_hist is not None and preco_p_atual_val < last_seen_price_hist:
                        desconto_calc = ((last_seen_price_hist - preco_p_atual_val) / last_seen_price_hist) * 100
                        desconto_calculado_para_msg = desconto_calc
                        preco_anterior_para_msg = last_seen_price_hist
                        if desconto_calc >= min_desconto_comparativo:
                            if last_notified_price_drop_hist is None or preco_p_atual_val < last_notified_price_drop_hist:
                                should_notify_product = True
                                notification_reason = f"Queda de preço de {desconto_calc:.1f}%."
                                entry_hist_p["last_notified_price_for_drop"] = preco_p_atual_val
                                history_changed_in_this_run = True
                            else:
                                notification_reason = f"Queda de {desconto_calc:.1f}% não é menor que última notificada por queda (R${last_notified_price_drop_hist:.2f})."
                        else:
                            notification_reason = f"Queda de {desconto_calc:.1f}% não atingiu {min_desconto_comparativo}%."
                        specific_logger.info(f"ASIN {asin_p}: {notification_reason}")
                    elif last_seen_price_hist is not None and preco_p_atual_val > last_seen_price_hist:
                         specific_logger.info(f"ASIN {asin_p}: Preço aumentou de R${last_seen_price_hist:.2f} para R${preco_p_atual_val:.2f}.")
                    elif last_seen_price_hist is not None and abs(preco_p_atual_val - last_seen_price_hist) < 1e-9 and condicao_p_usado != last_condition_hist:
                        specific_logger.info(f"ASIN {asin_p}: Preço estável R${preco_p_atual_val:.2f}, mas condição mudou de '{last_condition_hist}' para '{condicao_p_usado}'.")
                        # Você pode querer uma lógica de notificação aqui se a condição melhorar


                    if entry_hist_p.get("seen_price") != preco_p_atual_val or entry_hist_p.get("condition") != condicao_p_usado:
                        entry_hist_p["seen_price"] = preco_p_atual_val
                        entry_hist_p["condition"] = condicao_p_usado
                        history_changed_in_this_run = True
                        specific_logger.info(f"ASIN {asin_p}: Seen price/condition atualizado para R${preco_p_atual_val:.2f} / '{condicao_p_usado}'.")
                    
                    if "notified_on_first_find" not in entry_hist_p:
                         entry_hist_p["notified_on_first_find"] = False 
                    price_history_data[asin_p] = entry_hist_p

                if should_notify_product and bot_inst and chat_ids_list:
                    msg_telegram = ""
                    if "Novo item usado encontrado" in notification_reason:
                        msg_telegram = (f"✨ *NOVO ITEM USADO NA ÁREA!*\n\n"
                                       f"🛒 *{escape_md(nome_p)}*\n"
                                       f"⚙️ Condição: *{escape_md(condicao_p_usado)}*\n"
                                       f"💰 Preço: R\\${escape_md(f'{preco_p_atual_val:.2f}')}\n\n"
                                       f"🔗 [Ver produto]({escape_md(link_p_url)})")
                    elif "Queda de preço" in notification_reason:
                        preco_ant_fmt = escape_md(f"{preco_anterior_para_msg:.2f}")
                        desconto_fmt = escape_md(f"{desconto_calculado_para_msg:.1f}")
                        msg_telegram = (f"📉 *QUEDA DE PREÇO EM USADO!*\n\n"
                                       f"🛒 *{escape_md(nome_p)}*\n"
                                       f"⚙️ Condição: *{escape_md(condicao_p_usado)}*\n"
                                       f"💰 Preço Atual: R\\${escape_md(f'{preco_p_atual_val:.2f}')}\n"
                                       f"🏷️ Era: R\\${preco_ant_fmt} (Queda de *{desconto_fmt}\\%*)\n\n"
                                       f"🔗 [Ver produto]({escape_md(link_p_url)})")
                    
                    if msg_telegram:
                        sent_any_telegram = False
                        for chat_id_val in chat_ids_list:
                            if await send_telegram_message_async(bot_inst, chat_id_val, msg_telegram, ParseMode.MARKDOWN_V2, specific_logger):
                                sent_any_telegram = True
                        if sent_any_telegram:
                            specific_logger.info(f"ASIN {asin_p}: Notificação enviada. Razão: {notification_reason}")
                            history_changed_in_this_run = True 
                    else:
                        specific_logger.warning(f"ASIN {asin_p}: `should_notify_product` era True, mas `msg_telegram` vazia. Razão: {notification_reason}")

            except StaleElementReferenceException: specific_logger.warning(f"ASIN {asin_p}: Elemento stale (usado)."); continue
            except Exception as e_det: specific_logger.error(f"ASIN {asin_p}: Erro detalhes do produto (usado): {e_det}", exc_info=True); continue

        if pagina_atual_numero < MAX_PAGINAS_POR_LINK_GLOBAL: await asyncio.sleep(3) 
        if loop_broken_flag: break
    
    processed_pages_count = pagina_atual_numero
    if loop_broken_flag and paginas_sem_produtos_consecutivas > 0 and pagina_atual_numero > 1:
        processed_pages_count = pagina_atual_numero - paginas_sem_produtos_consecutivas
    elif loop_broken_flag and pagina_atual_numero > 0:
        processed_pages_count = max(0, pagina_atual_numero -1)
    specific_logger.info(f"--- Concluída Fonte: {nome_fonte_atual} (aprox. {max(0, processed_pages_count)} pgs processadas) ---")
    return history_changed_in_this_run

async def scrape_source_worker_async( 
    source_details, min_desconto_global_val, bot_global_val,
    chat_ids_global_val, semaphore, concurrency_limit_for_log,
    global_driver_path=None, shared_price_history_data=None):

    source_name = source_details["name"] 
    source_safe_name = source_details["safe_name"] 
    source_url = source_details["url"] 

    worker_logger = logging.getLogger(f"worker_usados.{source_safe_name}") 
    if not worker_logger.handlers:
        log_filename_source = os.path.join(DEBUG_LOGS_DIR_BASE, f"scrape_debug_{source_safe_name}.log")
        file_handler_source = logging.FileHandler(log_filename_source, encoding="utf-8", mode="w")
        formatter_source = logging.Formatter("%(asctime)s - %(levelname)s - [%(module)s.%(funcName)s:%(lineno)d] - %(message)s")
        file_handler_source.setFormatter(formatter_source)
        worker_logger.addHandler(file_handler_source)
        worker_logger.setLevel(logging.INFO)
        worker_logger.propagate = False

    driver_instance = None
    history_was_changed_by_this_worker = False

    async with semaphore:
        try: # Para logging do semáforo
            active_workers_approx = concurrency_limit_for_log - semaphore._value if hasattr(semaphore, '_value') else 'N/A'
            logger.info(f"Semáforo ADQUIRIDO por (USADOS): '{source_name}'. Concorrência (aprox): {active_workers_approx}/{concurrency_limit_for_log}.")
        except Exception:
             logger.info(f"Semáforo ADQUIRIDO por (USADOS): '{source_name}'.")

        worker_logger.info(f"--- [WORKER USADOS INÍCIO] Fonte: {source_name} ---")

        if shared_price_history_data is None:
            worker_logger.error("Histórico compartilhado (global de usados) não foi fornecido ao worker. Saindo.")
            return False

        try:
            driver_instance = await asyncio.to_thread(iniciar_driver_sync_worker, worker_logger, global_driver_path)
            worker_logger.info(f"Driver Selenium iniciado para {source_name}.")

            history_was_changed_by_this_worker = await processar_pagina_real_async(
                driver=driver_instance, url_inicial_categoria=source_url, nome_fonte_atual=source_name,
                specific_logger=worker_logger,
                price_history_data=shared_price_history_data,
                min_desconto_comparativo=min_desconto_global_val,
                bot_inst=bot_global_val,
                chat_ids_list=chat_ids_global_val
            )
        except Exception as e_main_worker:
            worker_logger.error(f"Erro principal no worker para {source_name} (USADOS): {e_main_worker}", exc_info=True)
        finally:
            if driver_instance:
                try:
                    worker_logger.info(f"Fechando driver Selenium para {source_name} (USADOS)...")
                    await asyncio.to_thread(driver_instance.quit)
                except Exception as e_quit: worker_logger.error(f"Erro ao fechar o driver para {source_name} (USADOS): {e_quit}", exc_info=True)

            if history_was_changed_by_this_worker: worker_logger.info(f"Worker para {source_name} (USADOS) MODIFICOU o histórico global.")
            else: worker_logger.info(f"Worker para {source_name} (USADOS) NÃO modificou o histórico global.")
            worker_logger.info(f"--- [WORKER USADOS FIM] Fonte: {source_name} ---")

    logger.info(f"Worker para '{source_name}' (USADOS) LIBEROU semáforo.")
    return history_was_changed_by_this_worker

async def orchestrate_all_usados_scrapes_main_async():
    logger.info("--- INICIANDO ORQUESTRADOR DE SCRAPING DE USADOS ---")
    os.makedirs(HISTORY_DIR_BASE, exist_ok=True)
    logger.info(f"Diretório de histórico de USADOS: {os.path.abspath(HISTORY_DIR_BASE)}")
    os.makedirs(DEBUG_LOGS_DIR_BASE, exist_ok=True)
    logger.info(f"Diretório de logs de debug de USADOS: {os.path.abspath(DEBUG_LOGS_DIR_BASE)}")

    global_price_history_data_usados = {}
    global_history_file_full_path = os.path.join(HISTORY_DIR_BASE, GLOBAL_HISTORY_FILENAME)

    if not USAR_HISTORICO:
        logger.info(f"USAR_HISTORICO_GLOBAL_USADOS é False. Deletando arquivo de histórico de USADOS: {global_history_file_full_path} (se existir)...")
        if os.path.exists(global_history_file_full_path):
            try: os.remove(global_history_file_full_path); logger.info("Arquivo de histórico de USADOS deletado.")
            except Exception as e_del: logger.error(f"Erro ao deletar histórico de USADOS {global_history_file_full_path}: {e_del}")
    else:
        if os.path.exists(global_history_file_full_path):
            try:
                with open(global_history_file_full_path, "r", encoding="utf-8") as f_hist_global:
                    loaded_data = json.load(f_hist_global)
                    if isinstance(loaded_data, dict): global_price_history_data_usados = loaded_data
                    logger.info(f"Histórico GLOBAL de USADOS carregado de '{global_history_file_full_path}' ({len(global_price_history_data_usados)} itens).")
            except json.JSONDecodeError:
                logger.error(f"Erro ao decodificar JSON do histórico GLOBAL de USADOS '{global_history_file_full_path}'. Fazendo backup e iniciando vazio.")
                try: os.rename(global_history_file_full_path, f"{global_history_file_full_path}.corrupted_{int(asyncio.get_event_loop().time())}")
                except Exception as e_backup: logger.error(f"Falha ao fazer backup do arquivo corrompido de USADOS: {e_backup}")
                global_price_history_data_usados = {}
            except Exception as e_load_global_hist:
                 logger.error(f"Erro ao carregar histórico GLOBAL de USADOS: {e_load_global_hist}. Iniciando vazio.")
                 global_price_history_data_usados = {}
        else:
            logger.info(f"Nenhum arquivo de histórico GLOBAL de USADOS ('{global_history_file_full_path}') encontrado. Iniciando vazio.")

    installed_chromedriver_path = None
    try:
        logger.info("Tentando instalar/verificar o ChromeDriver (USADOS)...")
        installed_chromedriver_path = ChromeDriverManager().install()
    except Exception as e_global_wdm:
        logger.error(f"Falha ao instalar ChromeDriver via WebDriverManager (USADOS): {e_global_wdm}", exc_info=False)
        common_paths = ["/usr/bin/chromedriver", "/usr/local/bin/chromedriver", os.path.expanduser("~/bin/chromedriver")]
        for path_check in common_paths:
            if os.path.exists(path_check) and os.access(path_check, os.X_OK): installed_chromedriver_path = path_check; break
        if installed_chromedriver_path: logger.info(f"Usando ChromeDriver de {installed_chromedriver_path} (USADOS).")
        else: logger.warning("Nenhum ChromeDriver global pôde ser configurado (USADOS).")

    CONCURRENCY_LIMIT = 1 
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

    tasks_coroutines = []
    for source_data in CATEGORIES: 
        tasks_coroutines.append(scrape_source_worker_async(
            source_details=source_data,
            min_desconto_global_val=MIN_DESCONTO_USADOS,
            bot_global_val=bot_instance_global,
            chat_ids_global_val=TELEGRAM_CHAT_IDS_LIST,
            semaphore=semaphore,
            concurrency_limit_for_log=CONCURRENCY_LIMIT,
            global_driver_path=installed_chromedriver_path,
            shared_price_history_data=global_price_history_data_usados
        ))

    logger.info(f"Iniciando {len(tasks_coroutines)} tarefa(s) de scraping de USADOS...")
    
    results = []
    if tasks_coroutines:
        active_tasks = [asyncio.create_task(coro) for coro in tasks_coroutines]
        
        while not all(t.done() for t in active_tasks):
            num_done = sum(1 for t in active_tasks if t.done())
            num_total = len(active_tasks)
            try:
                active_selenium_workers = CONCURRENCY_LIMIT - semaphore._value if hasattr(semaphore, '_value') else 'N/A'
                logger.info(f"Orquestrador de USADOS aguardando... {num_done}/{num_total} tarefas concluídas. "
                            f"Workers ativos (aprox.): {active_selenium_workers}/{CONCURRENCY_LIMIT}. Próximo log em 60s.")
            except Exception:
                 logger.info(f"Orquestrador de USADOS aguardando... {num_done}/{num_total} tarefas concluídas. Próximo log em 60s.")

            try:
                await asyncio.wait_for(asyncio.shield(asyncio.gather(*active_tasks, return_exceptions=True)), timeout=60)
            except asyncio.TimeoutError:
                pass 
            except Exception as e_gather_loop:
                logger.error(f"Erro inesperado no loop de keep-alive (USADOS): {e_gather_loop}", exc_info=True)
                break 
        
        results = []
        for task in active_tasks:
            try:
                results.append(task.result())
            except Exception as e_task_res:
                logger.error(f"Exceção ao obter resultado da task (USADOS): {e_task_res}", exc_info=True)
                results.append(e_task_res)

        logger.info("Todas as tarefas de scraping de USADOS foram concluídas ou falharam.")
    else:
        logger.info("Nenhuma tarefa de scraping de USADOS para executar.")


    any_history_modified_overall = False
    successful_tasks, failed_tasks = 0, 0
    for i, res_worker in enumerate(results):
        source_name_res = CATEGORIES[i]['name'] if i < len(CATEGORIES) else f"Tarefa Usados Desconhecida {i}"
        if isinstance(res_worker, Exception):
            logger.error(f"Tarefa para '{source_name_res}' (USADOS) FALHOU: {res_worker}", exc_info=True)
            failed_tasks +=1
        else:
            if res_worker is True: any_history_modified_overall = True
            logger.info(f"Tarefa para '{source_name_res}' (USADOS) concluída (Histórico modificado: {res_worker}).")
            successful_tasks +=1
    logger.info(f"Resumo das tarefas de USADOS: {successful_tasks} OK, {failed_tasks} falharam.")


    if USAR_HISTORICO:
        if any_history_modified_overall or not os.path.exists(global_history_file_full_path):
            try:
                with open(global_history_file_full_path, "w", encoding="utf-8") as f_hist_final_global:
                    json.dump(global_price_history_data_usados, f_hist_final_global, indent=4, ensure_ascii=False)
                logger.info(f"Histórico GLOBAL de USADOS salvo com sucesso em '{global_history_file_full_path}' ({len(global_price_history_data_usados)} itens).")
            except Exception as e_save_final_global:
                logger.error(f"Erro crítico ao salvar histórico GLOBAL final de USADOS: {e_save_final_global}", exc_info=True)
        else:
            logger.info("Histórico GLOBAL de USADOS não modificado e arquivo já existe. Não foi salvo.")
    else:
        logger.info("USAR_HISTORICO_GLOBAL_USADOS é False. Histórico não foi salvo.")

    logger.info("--- ORQUESTRADOR DE SCRAPING DE USADOS CONCLUÍDO ---")

if __name__ == "__main__":
    logger.info(f"Orquestrador de USADOS chamado via __main__ (scripts/{os.path.basename(__file__)})")
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_IDS_LIST:
        logger.warning("ALERTA USADOS: Token do Telegram ou Chat IDs não configurados. Notificações desabilitadas.")
    
    alert_msg_selector = ("!!!!!!!!!! ALERTA CRÍTICO DE SELETORES (USADOS) !!!!!!!!!!\n"
                          "Os seletores CSS para produtos usados PRECISAM ser verificados e ajustados em 'orchestrator_usados.py'.\n"
                          "Inspecione o HTML da página de usados da Amazon (ou a imagem que você enviou) para os valores corretos de:\n"
                          f"  SELETOR_ITEM_PRODUTO_USADO       (atual: '{SELETOR_ITEM_PRODUTO_USADO}')\n"
                          f"  SELETOR_NOME_PRODUTO_USADO       (atual: '{SELETOR_NOME_PRODUTO_USADO}')\n"
                          f"  SELETOR_LINK_PRODUTO_USADO       (atual: '{SELETOR_LINK_PRODUTO_USADO}')\n"
                          f"  SELETOR_PRECO_USADO_DENTRO_DO_ITEM (atual: '{SELETOR_PRECO_USADO_DENTRO_DO_ITEM}')\n"
                          f"  SELETOR_INDICADOR_USADO_TEXTO    (atual: '{SELETOR_INDICADOR_USADO_TEXTO}')\n"
                          "  (Considere também SELETOR_CONDICAO_ESPECIFICA_USADO se a condição exata estiver visível)\n"
                          "O script NÃO FUNCIONARÁ CORRETAMENTE até que os seletores estejam corretos para a página alvo.\n"
                          "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    
    # Adicione uma verificação simples para logar o alerta se os seletores parecerem default ou muito genéricos
    # Esta é uma heurística, ajuste conforme refina seus seletores.
    if "CONFIRME!" in SELETOR_INDICADOR_USADO_TEXTO or \
       SELETOR_PRECO_USADO_DENTRO_DO_ITEM == "div[data-cy='secondary-offer-recipe'] span.a-color-base": # Verifica se um dos seletores chave ainda é o default do exemplo
        logger.critical(alert_msg_selector)


    asyncio.run(orchestrate_all_usados_scrapes_main_async())
