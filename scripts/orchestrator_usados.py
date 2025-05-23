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
# Baseado no HTML fornecido para "Um defeito de cor".
# A extração da CONDIÇÃO específica ("Como Novo", "Bom") é o mais crítico.

SELETOR_ITEM_PRODUTO_USADO = "div.s-result-item[data-asin]"

# Nome e Link (baseado no snippet HTML fornecido)
SELETOR_NOME_PRODUTO_USADO = "div[data-cy='title-recipe'] h2.a-size-base-plus > span"
SELETOR_LINK_PRODUTO_USADO = "div[data-cy='title-recipe'] > a.a-link-normal"

# Preço (baseado no snippet, dentro de 'secondary-offer-recipe')
# A função get_price_sync_worker espera o seletor do elemento que CONTÉM o texto do preço.
# O texto do preço "R$ 61,98" está em <span class="a-color-base">.
# Então o seletor para get_price_sync_worker deve ser para este span.
SELETOR_PRECO_USADO_DENTRO_DO_ITEM = "div[data-cy='secondary-offer-recipe'] span.a-color-base"

# Condição (Esta é a parte que mais precisa de SUA VALIDAÇÃO)
# No HTML de "Um defeito de cor", a condição específica não está clara, apenas "(X oferta de produto usado)".
# Este seletor pega o texto do link que indica o número de ofertas usadas.
# A lógica do script tentará usar isso para marcar como "Usado".
# SE A IMAGEM 'image_7818ed.png' MOSTRAR UMA CONDIÇÃO ESPECÍFICA (ex: "Usado - Bom")
# DIRETAMENTE NA LISTAGEM PARA CADA ITEM, VOCÊ PRECISA DE UM SELETOR PARA *ESSE TEXTO ESPECÍFICO*.
SELETOR_INDICADOR_USADO_TEXTO = "div[data-cy='secondary-offer-recipe'] a" # Pega o texto tipo "(1 oferta de produto usado)"
# Se houver um seletor melhor para a condição EXATA (Ex: "Usado - Como Novo"), substitua:
# SELETOR_CONDICAO_ESPECIFICA_USADO = "SEU_SELETOR_AQUI_PARA_CONDICAO_EXATA" # Ex: "span.condicao-texto" (hipotético)


# Link fornecido pelo usuário para produtos USADOS da Amazon Warehouse Deals
USED_PRODUCTS_LINK = "https://www.amazon.com.br/s?i=warehouse-deals&srs=24669725011&bbn=24669725011&rh=n%3A24669725011&s=popularity-rank&fs=true&page=1&qid=1747998790&xpid=M2soDZTyDMNhF&ref=sr_pg_1"

CATEGORIES = [
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
# (O resto das inicializações globais, funções auxiliares como send_telegram_message_async, escape_md,
# iniciar_driver_sync_worker, check_captcha_sync_worker, get_url_for_page_worker são as mesmas
# da última versão completa do orchestrator_usados.py que te enviei. Vou omiti-las aqui para brevidade,
# mas elas devem estar presentes no seu script.)

# Função get_price_sync_worker modificada para extrair de texto direto "R$ XX,YY"
def get_price_from_direct_text(element_raiz, selector_para_span_de_preco, specific_logger):
    try:
        price_span_list = element_raiz.find_elements(By.CSS_SELECTOR, selector_para_span_de_preco)
        if price_span_list:
            raw_text = price_span_list[0].text # Usar .text para pegar "R$ 61,98"
            if not raw_text: return None
            # Limpeza para extrair apenas números e o separador decimal
            cleaned_text = re.sub(r'[^\d,]', '', raw_text) # Remove 'R$', espaços, etc., mantém vírgula
            if not cleaned_text: return None
            
            cleaned_text = cleaned_text.replace(',', '.') # Converte vírgula para ponto
            
            if re.match(r'^\d+(\.\d{1,2})?$', cleaned_text):
                return float(cleaned_text)
            if re.match(r'^\d+$', cleaned_text): # Caso seja um inteiro
                 return float(cleaned_text)
            specific_logger.warning(f"Texto de preço '{raw_text}' não resultou em float válido após limpeza para '{cleaned_text}'.")
        return None
    except Exception as e:
        specific_logger.error(f"Exceção em get_price_from_direct_text com seletor '{selector_para_span_de_preco}': {e}")
        return None


async def processar_pagina_real_async(
    driver, url_inicial_categoria, nome_fonte_atual,
    specific_logger,
    price_history_data,
    min_desconto_comparativo, bot_inst, chat_ids_list ):

    history_changed_in_this_run = False
    parsed_initial_url = urlparse(url_inicial_categoria)
    query_params = parse_qs(parsed_initial_url.query)
    for param in ['page', 'qid', 'ref', 'xpid', 'bbn', 'srs']:
        query_params.pop(param, None)
    cleaned_query_string = urlencode(query_params, doseq=True)
    base_url_para_paginacao = urlunparse(parsed_initial_url._replace(path="/s", query=cleaned_query_string))

    specific_logger.info(f"--- Processando Fonte: {nome_fonte_atual} --- URL base para paginação: {base_url_para_paginacao} ---")
    paginas_sem_produtos_consecutivas = 0; loop_broken_flag = False; pagina_atual_numero = 0

    for i_pagina in range(1, MAX_PAGINAS_POR_LINK_GLOBAL + 1):
        pagina_atual_numero = i_pagina
        if pagina_atual_numero == 1: url_atual = url_inicial_categoria
        else: url_atual = get_url_for_page_worker(base_url_para_paginacao, pagina_atual_numero)
        
        specific_logger.info(f"[{nome_fonte_atual}] Processando URL: {url_atual} (Página: {pagina_atual_numero}/{MAX_PAGINAS_POR_LINK_GLOBAL})")

        try:
            await asyncio.to_thread(driver.get, url_atual); await asyncio.sleep(5)
        except Exception as e_load_url: specific_logger.error(f"Erro ao carregar {url_atual}: {e_load_url}"); loop_broken_flag = True; break
        if await asyncio.to_thread(check_captcha_sync_worker, driver, nome_fonte_atual, specific_logger): loop_broken_flag = True; break

        try:
            await asyncio.to_thread(WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, SELETOR_ITEM_PRODUTO_USADO))))
        except TimeoutException: # (Lógica de tratamento de timeout e 'nenhum resultado' como antes)
            specific_logger.warning(f"Timeout esperando por '{SELETOR_ITEM_PRODUTO_USADO}' em {url_atual}.")
            try:
                no_results_msg = await asyncio.to_thread(driver.find_elements, By.XPATH, "//span[contains(text(), 'Nenhum resultado') or contains(text(), 'No results for') or contains(., 'não encontrou nenhum resultado')]")
                if no_results_msg and no_results_msg[0].is_displayed():
                    specific_logger.info(f"Página indica 'Nenhum resultado' em {url_atual}. Fim da paginação para {nome_fonte_atual}."); loop_broken_flag = True; break
            except: pass
            produtos_elements_check = await asyncio.to_thread(driver.find_elements, By.CSS_SELECTOR, SELETOR_ITEM_PRODUTO_USADO)
            if not produtos_elements_check:
                specific_logger.warning(f"Nenhum item ('{SELETOR_ITEM_PRODUTO_USADO}') encontrado após timeout em {url_atual}. Pulando pág.")
                paginas_sem_produtos_consecutivas += 1
                if paginas_sem_produtos_consecutivas >= 2 and pagina_atual_numero > 1: loop_broken_flag = True; break
                continue
            specific_logger.info(f"[{nome_fonte_atual}] {len(produtos_elements_check)} produtos encontrados mesmo após timeout no container.")


        all_item_elements_on_page = await asyncio.to_thread(driver.find_elements, By.CSS_SELECTOR, SELETOR_ITEM_PRODUTO_USADO)
        specific_logger.info(f"[{nome_fonte_atual}] {len(all_item_elements_on_page)} itens ('{SELETOR_ITEM_PRODUTO_USADO}') encontrados na pág {pagina_atual_numero}.")
        # (Lógica de pular página se não encontrar itens, como antes)
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
                    else: specific_logger.warning(f"ASIN {asin_p}: Nome não encontrado com seletor '{SELETOR_NOME_PRODUTO_USADO}'."); continue
                except NoSuchElementException: specific_logger.warning(f"ASIN {asin_p}: Exceção ao buscar nome."); continue
                if not nome_p or nome_p == "N/A": specific_logger.warning(f"ASIN {asin_p}: Nome extraído inválido."); continue

                try:
                    link_el_list = await asyncio.to_thread(p_element.find_elements, By.CSS_SELECTOR, SELETOR_LINK_PRODUTO_USADO)
                    if link_el_list:
                        link_url_raw_p = await asyncio.to_thread(link_el_list[0].get_attribute, "href")
                        # Construir link canônico da Amazon
                        if not link_url_raw_p.startswith("http"): link_url_raw_p = "https://www.amazon.com.br" + link_url_raw_p
                        parsed_link = urlparse(link_url_raw_p)
                        link_p_url = urlunparse(parsed_link._replace(query="", fragment="")) # Limpa query e fragmento
                        if "/dp/" not in link_p_url and asin_p: # Adiciona /dp/ASIN se não estiver
                            link_p_url = f"https://www.amazon.com.br/dp/{asin_p}"

                    else: specific_logger.warning(f"ASIN {asin_p}: Link não encontrado."); link_p_url = f"https://www.amazon.com.br/dp/{asin_p}"
                except NoSuchElementException: specific_logger.warning(f"ASIN {asin_p}: Exceção ao buscar link."); link_p_url = f"https://www.amazon.com.br/dp/{asin_p}"

                # PREÇO: Usando a nova função para extrair de texto como "R$ 61,98"
                preco_p_atual_val = await asyncio.to_thread(get_price_from_direct_text, p_element, SELETOR_PRECO_USADO_DENTRO_DO_ITEM, specific_logger)
                if preco_p_atual_val is None or preco_p_atual_val <= 0:
                    specific_logger.warning(f"ASIN {asin_p}: Preço USADO inválido (R${preco_p_atual_val}) com seletor '{SELETOR_PRECO_USADO_DENTRO_DO_ITEM}'."); continue
                
                # CONDIÇÃO:
                try:
                    # Tenta primeiro um seletor para condição específica, se você definir um SELETOR_CONDICAO_ESPECIFICA_USADO
                    # if 'SELETOR_CONDICAO_ESPECIFICA_USADO' in globals() and SELETOR_CONDICAO_ESPECIFICA_USADO:
                    #    condicao_el_list = await asyncio.to_thread(p_element.find_elements, By.CSS_SELECTOR, SELETOR_CONDICAO_ESPECIFICA_USADO)
                    #    if condicao_el_list: condicao_p_usado = (await asyncio.to_thread(condicao_el_list[0].text)).strip()

                    # Se não encontrou condição específica ou o seletor não está definido, usa o SELETOR_INDICADOR_USADO_TEXTO
                    if condicao_p_usado == "N/A" or not condicao_p_usado:
                        indicador_el_list = await asyncio.to_thread(p_element.find_elements, By.CSS_SELECTOR, SELETOR_INDICADOR_USADO_TEXTO)
                        if indicador_el_list:
                            texto_indicador = (await asyncio.to_thread(indicador_el_list[0].text)).strip()
                            if "usado" in texto_indicador.lower(): # Ex: "(1 oferta de produto usado)"
                                condicao_p_usado = "Usado (detalhes na oferta)" # Genérico
                                # Se o texto do indicador já for "Usado - Bom", por exemplo, isso já pegaria.
                                if texto_indicador.lower().startswith("usado -"):
                                     condicao_p_usado = texto_indicador
                            else: # Se o seletor pegar algo que não indica usado
                                condicao_p_usado = "Condição não clara"
                        else:
                            specific_logger.warning(f"ASIN {asin_p}: Indicador de usado/condição não encontrado com seletor '{SELETOR_INDICADOR_USADO_TEXTO}'.")
                            condicao_p_usado = "Condição não obtida"
                except Exception as e_cond:
                    specific_logger.error(f"ASIN {asin_p}: Exceção ao buscar condição: {e_cond}"); condicao_p_usado = "Erro Condição"


                specific_logger.info(f"[{nome_fonte_atual}] ASIN {asin_p}: Nome='{nome_p[:30]}...', Preço=R${preco_p_atual_val:.2f}, Condição='{condicao_p_usado}'")

                # Lógica de Histórico e Notificação para USADOS (como definida antes)
                entry_hist_p = price_history_data.get(asin_p)
                should_notify_product = False
                notification_reason = ""
                desconto_calculado_para_msg = 0.0
                preco_anterior_para_msg = preco_p_atual_val

                if entry_hist_p is None: # Produto novo no histórico
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
                else: # Produto já existe no histórico (lógica de desconto e atualização como antes)
                    last_seen_price_hist = entry_hist_p.get("seen_price")
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
                                notification_reason = f"Queda de {desconto_calc:.1f}% não é menor que última notificada por queda ({last_notified_price_drop_hist})."
                        else:
                            notification_reason = f"Queda de {desconto_calc:.1f}% não atingiu {min_desconto_comparativo}%."
                        specific_logger.info(f"ASIN {asin_p}: {notification_reason}")
                    elif last_seen_price_hist is not None and preco_p_atual_val > last_seen_price_hist:
                         specific_logger.info(f"ASIN {asin_p}: Preço aumentou de R${last_seen_price_hist:.2f} para R${preco_p_atual_val:.2f}.")

                    if entry_hist_p.get("seen_price") != preco_p_atual_val or entry_hist_p.get("condition") != condicao_p_usado:
                        entry_hist_p["seen_price"] = preco_p_atual_val
                        entry_hist_p["condition"] = condicao_p_usado
                        history_changed_in_this_run = True
                        specific_logger.info(f"ASIN {asin_p}: Seen price/condition atualizado para R${preco_p_atual_val:.2f} / '{condicao_p_usado}'.")
                    
                    if "notified_on_first_find" not in entry_hist_p: # Para compatibilidade com histórico antigo
                         entry_hist_p["notified_on_first_find"] = False 
                    price_history_data[asin_p] = entry_hist_p


                if should_notify_product and bot_inst and chat_ids_list:
                    # (Lógica de formatação da mensagem do Telegram como antes)
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

# Funções:
# create_safe_filename (já existe)
# send_telegram_message_async (já existe)
# escape_md (já existe)
# iniciar_driver_sync_worker (já existe)
# check_captcha_sync_worker (já existe)
# get_url_for_page_worker (já existe - verificar se 'ref' e outros params precisam mudar para warehouse)

# O restante das funções (scrape_source_worker_async, orchestrate_all_usados_scrapes_main_async, __main__)
# permanecem como na versão anterior que te enviei (com as devidas atualizações de nomes de variáveis/loggers
# para "_usados" onde aplicável). A principal mudança foi concentrada em `processar_pagina_real_async`
# e nos seletores no topo do arquivo. Vou incluir o restante abaixo para completude.

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
        slots_ocupados_agora = concurrency_limit_for_log - semaphore._value
        logger.info(f"Semáforo ADQUIRIDO por (USADOS): '{source_name}'. Slots ocupados: {slots_ocupados_agora}/{concurrency_limit_for_log}.")
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

    tasks = []
    for source_data in CATEGORIES:
        tasks.append(scrape_source_worker_async(
            source_details=source_data,
            min_desconto_global_val=MIN_DESCONTO_USADOS,
            bot_global_val=bot_instance_global,
            chat_ids_global_val=TELEGRAM_CHAT_IDS_LIST,
            semaphore=semaphore,
            concurrency_limit_for_log=CONCURRENCY_LIMIT,
            global_driver_path=installed_chromedriver_path,
            shared_price_history_data=global_price_history_data_usados
        ))

    logger.info(f"Iniciando {len(tasks)} tarefa(s) de scraping de USADOS...")
    results_from_workers = await asyncio.gather(*tasks, return_exceptions=True)

    any_history_modified_overall = False
    successful_tasks, failed_tasks = 0, 0
    for i, res_worker in enumerate(results_from_workers):
        source_name_res = CATEGORIES[i]['name']
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
    
    # Validação crítica dos seletores
    alert_msg_selector = "!!!!!!!!!! ALERTA CRÍTICO !!!!!!!!!!\nOs seletores CSS para produtos usados PRECISAM ser verificados e ajustados em 'orchestrator_usados.py'.\nInspecione o HTML da página de usados da Amazon para os valores corretos de:\nSELETOR_ITEM_PRODUTO_USADO\nSELETOR_NOME_PRODUTO_USADO\nSELETOR_LINK_PRODUTO_USADO\nSELETOR_PRECO_USADO_DENTRO_DO_ITEM\nSELETOR_INDICADOR_USADO_TEXTO (e/ou defina um SELETOR_CONDICAO_ESPECIFICA_USADO)\nO script NÃO FUNCIONARÁ CORRETAMENTE até que isso seja feito.\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    # Esta é uma verificação simples. Você pode querer remover os "CONFIRME!" dos seletores
    # no topo do script depois de ajustá-los para não ver este alerta.
    if "CONFIRME!" in SELETOR_ITEM_PRODUTO_USADO or \
       SELETOR_NOME_PRODUTO_USADO == "h2 a span.a-text-normal" or \
       SELETOR_PRECO_USADO_DENTRO_DO_ITEM == "div[data-cy='secondary-offer-recipe'] span.a-color-base" and logger.level <= logging.WARNING: # Exemplo de checagem mais específica
        logger.critical(alert_msg_selector)


    asyncio.run(orchestrate_all_usados_scrapes_main_async())
