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

logger = logging.getLogger("SCRAPER_USADOS_GERAL")

# --- Configurações do Scraper ---
SELETOR_ITEM_PRODUTO_USADO = "div.s-result-item.s-asin"
SELETOR_NOME_PRODUTO_USADO = "span.a-size-base-plus.a-color-base.a-text-normal"
SELETOR_PRECO_USADO = "div.s-price-instructions-style a span.a-offscreen"
# SELETOR_INDICADOR_USADO = "div.s-price-instructions-style a span[contains(text(), 'usado')]" # Linha original comentada
# Abaixo, a versão corrigida usando XPath.
# O translate(., 'USADO', 'usado') torna a busca por 'usado' insensível a maiúsculas/minúsculas.
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

def load_proxy_list():
    """Carrega uma lista de proxies das variáveis de ambiente, sem autenticação se não fornecida."""
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
            if username and password:
                proxy_url = f'http://{username}:{password}@{host}:{port}'
            else:
                proxy_url = f'http://{host}:{port}'
            proxy_list.append(proxy_url)
    
    if not proxy_list:
        logger.warning("Nenhum proxy configurado nas variáveis de ambiente.")
    else:
        logger.info(f"Carregados {len(proxy_list)} proxies.")
    return proxy_list

def test_proxy(proxy_url, logger):
    """Testa se o proxy é funcional."""
    logger.info(f"Testando proxy: {proxy_url}")
    try:
        response = requests.get("https://www.amazon.com.br", proxies={"http": proxy_url, "https": proxy_url}, timeout=10)
        if response.status_code == 200:
            logger.info("Proxy testado com sucesso: Status 200")
            return True
        else:
            logger.warning(f"Proxy retornou status inesperado: {response.status_code}")
            return False
    except requests.RequestException as e:
        if "NameResolutionError" in str(e):
            logger.error(f"Erro de resolução de nome para o proxy: {e}")
        else:
            logger.error(f"Erro ao testar proxy: {e}")
        return False

def get_working_proxy(proxy_list, logger):
    """Retorna o primeiro proxy funcional da lista ou None se todos falharem."""
    for proxy_url in proxy_list:
        if test_proxy(proxy_url, logger):
            return proxy_url
    logger.warning("Nenhum proxy funcional encontrado. Prosseguindo sem proxy.")
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
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--mute-audio")
    chrome_options.add_argument("--no-first-run")
    chrome_options.add_argument("--disable-webgl")
    chrome_options.add_argument("--disable-webrtc")
    chrome_options.add_argument("--disable-features=WebRtcHideLocalIpsWithMdns,PrivacySandboxSettings4,OptimizationHints,InterestGroupStorage")
    chrome_options.add_argument("--lang=pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7")
    
    proxy_list = load_proxy_list()
    proxy_url = get_working_proxy(proxy_list, current_run_logger) if proxy_list else None
    proxy_configured = False
    
    if proxy_url:
        current_run_logger.info(f"Configurando proxy: {proxy_url}")
        chrome_options.add_argument(f'--proxy-server={proxy_url}')
        proxy_configured = True
    else:
        current_run_logger.warning("Nenhum proxy funcional disponível. Prosseguindo sem proxy.")
    
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
    except WebDriverException as e:
        if "ERR_NO_SUPPORTED_PROXIES" in str(e) and proxy_configured: # Adicionado "and proxy_configured" para garantir que o erro é realmente sobre o proxy que tentamos configurar
            current_run_logger.error(f"Erro: Proxy não suportado ({proxy_url}). Tentando instanciar WebDriver sem proxy.")
            # Remove o argumento do proxy e tenta novamente
            chrome_options.arguments = [arg for arg in chrome_options.arguments if not arg.startswith('--proxy-server')]
            driver = webdriver.Chrome(service=service, options=chrome_options) # Tenta instanciar sem proxy
            current_run_logger.info("WebDriver instanciado sem proxy após falha com proxy configurado.")
            driver.set_page_load_timeout(page_load_timeout)
            driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            })
            return driver
        else:
            current_run_logger.error(f"Erro ao instanciar WebDriver: {e}", exc_info=True)
            if driver:
                driver.quit()
            raise
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
        return False # Retorna False em caso de erro inesperado para não parar o fluxo indevidamente

def check_amazon_error_page_sync_worker(driver, current_run_logger):
    current_run_logger.debug("Verificando se é página de erro da Amazon.")
    error_page_detected = False
    try:
        page_title = driver.title.lower()
        if any(keyword in page_title for keyword in ["algo deu errado", "sorry", "problema", "serviço indisponível", "error"]):
            current_run_logger.warning(f"Página de erro detectada pelo título: {page_title}")
            error_page_detected = True
        
        error_selectors = [
            (By.XPATH, "//*[contains(text(), 'Algo deu errado')]"),
            (By.XPATH, "//*[contains(text(), 'Desculpe-nos')]"),
            (By.XPATH, "//*[contains(text(), 'Serviço Indisponível')]"),
            (By.CSS_SELECTOR, "div#centerContent div.a-box-inner h1"), # Títulos de erro comuns
            (By.CSS_SELECTOR, "div.a-alert-content"), # Caixas de alerta de erro
            (By.ID, "g") # Elemento comum em páginas de erro "genéricas"
        ]
        if not error_page_detected: # Só checa seletores se o título não indicou erro
            for by, selector in error_selectors:
                try:
                    element = driver.find_element(by, selector)
                    element_text = element.text.lower() if element.text else ""
                    if any(keyword in element_text for keyword in ["erro", "problem", "indisponível", "sorry", "não encontrado"]):
                        current_run_logger.warning(f"Página de erro detectada por elemento: {selector} | Texto: {element.text[:100]}")
                        error_page_detected = True
                        break
                except NoSuchElementException:
                    continue
                except StaleElementReferenceException:
                    current_run_logger.warning(f"Elemento {selector} ficou obsoleto ao verificar página de erro.")
                    continue


        # Se não detectou erro e não tem o contêiner principal de resultados, pode ser um erro sutil
        if not error_page_detected:
            try:
                driver.find_element(By.CSS_SELECTOR, SELETOR_RESULTADOS_CONT)
                current_run_logger.debug("Contêiner de resultados encontrado. Não é página de erro.")
            except NoSuchElementException:
                current_run_logger.warning("Contêiner de resultados NÃO encontrado. Considerando como página de erro.")
                error_page_detected = True
        
        return error_page_detected

    except Exception as e:
        current_run_logger.error(f"Erro ao verificar página de erro: {e}", exc_info=True)
        return True # Em caso de dúvida ou erro na verificação, assume que é uma página de erro para segurança
    finally:
        if error_page_detected and driver.current_url: # Salva debug apenas se erro foi detectado
            timestamp_error = datetime.now().strftime('%Y%m%d_%H%M%S')
            screenshot_path = os.path.join(DEBUG_LOGS_DIR_BASE, f"error_check_usados_geral_{timestamp_error}.png")
            html_path = os.path.join(DEBUG_LOGS_DIR_BASE, f"error_check_usados_geral_{timestamp_error}.html")
            try:
                driver.save_screenshot(screenshot_path)
                current_run_logger.info(f"Screenshot da página de erro salvo em: {screenshot_path}")
                with open(html_path, "w", encoding="utf-8") as f_html:
                    f_html.write(driver.page_source)
                current_run_logger.info(f"HTML da página de erro salvo em: {html_path}")
            except Exception as e_save:
                current_run_logger.error(f"Erro ao salvar debug da página de erro: {e_save}")


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

def check_url_status(url, logger, max_retries=5, backoff_factor=3):
    logger.debug(f"Verificando status HTTP da URL: {url}")
    proxy_list = load_proxy_list()
    proxies = None
    
    if proxy_list:
        # Não chamar get_working_proxy aqui para não gastar os testes de proxy apenas no HEAD
        # Se houver proxies, tentar usá-los na requisição HEAD diretamente.
        # A lógica de seleção de proxy para o WebDriver já foi feita.
        # Poderíamos rodar a lista ou pegar um aleatório. Por simplicidade, vamos pegar o primeiro se houver.
        # Ou melhor, não usar proxy para HEAD, pois o objetivo é apenas checar a URL.
        # A checagem de proxy funcional é feita antes de iniciar o driver.
        # logger.debug("Proxies disponíveis. Verificando status sem proxy específico para HEAD.")
        # proxies = {"http": proxy_list[0], "https": proxy_list[0]} # Exemplo, não ideal
        pass # Deixar proxies como None, para usar conexão direta para o HEAD
    
    if not proxies: # Se não há proxies (ou decidimos não usar para HEAD)
        logger.debug("Verificando URL sem proxy para a requisição HEAD.")


    for attempt in range(1, max_retries + 1):
        try:
            # Usar um User-Agent aleatório também para requests
            ua_req = UserAgent()
            headers = {'User-Agent': ua_req.random}
            response = requests.head(url, timeout=15, allow_redirects=True, proxies=proxies, headers=headers) # Adicionado headers
            logger.info(f"Status HTTP da URL ({url}): {response.status_code}")
            
            # Se a Amazon retornar 404 para uma página que não seja a primeira, pode ser o fim da paginação.
            # No entanto, é mais seguro verificar a ausência de itens na página.
            # Por ora, qualquer status diferente de 200 ou 503 (que já é tratado) é um aviso.
            if response.status_code == 200:
                return response.status_code
            elif response.status_code == 503: # Service Unavailable
                logger.warning(f"URL retornou status 503. Tentativa {attempt}/{max_retries}.")
                if attempt < max_retries:
                    sleep_time = backoff_factor ** attempt
                    logger.info(f"Aguardando {sleep_time}s antes da próxima tentativa para status 503...")
                    time.sleep(sleep_time)
                # Não retorna aqui, continua o loop para nova tentativa
            elif response.status_code == 404: # Not Found
                logger.warning(f"URL retornou status 404. Pode ser o fim da paginação ou URL inválida.")
                return response.status_code # Retorna 404 para ser tratado pelo chamador
            else: # Outros códigos de erro (403 Forbidden, etc.)
                logger.warning(f"URL retornou status inesperado: {response.status_code}. Tentativa {attempt}/{max_retries}.")
                # Para outros erros, também podemos tentar novamente
                if attempt < max_retries:
                    sleep_time = backoff_factor ** attempt
                    logger.info(f"Aguardando {sleep_time}s antes da próxima tentativa para status {response.status_code}...")
                    time.sleep(sleep_time)
                else: # Se for a última tentativa e ainda um erro diferente de 503
                    return response.status_code # Retorna o último status de erro obtido
        except requests.RequestException as e:
            logger.error(f"Erro de requisição ao verificar status da URL ({url}): {e}. Tentativa {attempt}/{max_retries}.")
            if attempt < max_retries:
                sleep_time = backoff_factor ** attempt
                logger.info(f"Aguardando {sleep_time}s antes da próxima tentativa devido à exceção...")
                time.sleep(sleep_time)
            else: # Última tentativa falhou com exceção
                logger.error(f"Falha final ao verificar status da URL ({url}) após {max_retries} tentativas com exceção.")
                return None # Retorna None se todas as tentativas falharem com exceção

    logger.error(f"Falha ao obter status HTTP 200 para ({url}) após {max_retries} tentativas (último status pode não ser 200).")
    return None # Retorna None se esgotar retries e não for 200

async def process_used_products_geral_async(driver, base_url, nome_fluxo, history, logger, max_paginas=MAX_PAGINAS_POR_LINK_GLOBAL):
    logger.info(f"--- Iniciando processamento para: {nome_fluxo} --- URL base: {base_url} ---")
    total_produtos_usados = []
    pagina_atual = 1
    max_tentativas_pagina = 3 # Reduzido para evitar loops longos em páginas problemáticas
    consecutive_empty_pages = 0
    max_consecutive_empty_pages = 3


    while pagina_atual <= max_paginas:
        url_pagina = get_url_for_page_worker(base_url, pagina_atual, logger)
        logger.info(f"[{nome_fluxo}] Carregando Página: {pagina_atual}/{max_paginas}, URL: {url_pagina}")

        # status_code = check_url_status(url_pagina, logger) # Opcional, pode ser útil mas adiciona requisições
        # if status_code is not None and status_code != 200:
        #     logger.warning(f"URL {url_pagina} retornou status {status_code} no HEAD check. Tentando carregar com Selenium mesmo assim.")
            # if status_code == 404:
            #     logger.info(f"Status 404 para {url_pagina}. Considerando fim da paginação.")
            #     break # Sai do loop while

        page_processed_successfully = False
        for tentativa in range(1, max_tentativas_pagina + 1):
            logger.info(f"[{nome_fluxo}] Tentativa {tentativa}/{max_tentativas_pagina} de carregar e processar URL: {url_pagina}")
            try:
                await asyncio.to_thread(driver.get, url_pagina)
                await asyncio.sleep(random.uniform(3, 6)) # Aumentar um pouco o delay inicial
                await asyncio.to_thread(wait_for_page_load, driver, logger)
                await simulate_scroll(driver, logger) # Simular rolagem após carregamento

                if check_captcha_sync_worker(driver, logger):
                    logger.error(f"[{nome_fluxo}] CAPTCHA detectado na página {pagina_atual}. Interrompendo fluxo para esta URL base.")
                    return total_produtos_usados # Interrompe o fluxo para esta base_url

                if check_amazon_error_page_sync_worker(driver, logger):
                    logger.error(f"[{nome_fluxo}] Página de erro da Amazon detectada na página {pagina_atual}.")
                    if tentativa < max_tentativas_pagina:
                        logger.info("Tentando novamente após delay...")
                        await asyncio.sleep(random.uniform(10, 20)) # Maior delay para erro
                        continue
                    else:
                        logger.error(f"[{nome_fluxo}] Falha ao carregar página de produtos após {max_tentativas_pagina} tentativas devido a página de erro. Interrompendo fluxo para esta URL base.")
                        return total_produtos_usados

                try:
                    WebDriverWait(driver, 20).until( # Aumentar timeout para presença do container
                        EC.presence_of_element_located((By.CSS_SELECTOR, SELETOR_RESULTADOS_CONT))
                    )
                    logger.info(f"Contêiner de resultados '{SELETOR_RESULTADOS_CONT}' encontrado na página {pagina_atual}.")
                except TimeoutException:
                    logger.warning(f"Contêiner de resultados '{SELETOR_RESULTADOS_CONT}' não encontrado na página {pagina_atual} após timeout. Verificando se há itens mesmo assim.")
                    # Não necessariamente interrompe, pode ser uma página vazia ou com estrutura diferente

                items = driver.find_elements(By.CSS_SELECTOR, SELETOR_ITEM_PRODUTO_USADO)
                logger.info(f"Página {pagina_atual}: Encontrados {len(items)} elementos com seletor principal '{SELETOR_ITEM_PRODUTO_USADO}'.")

                if not items:
                    logger.info(f"Página {pagina_atual} não contém produtos com o seletor principal. Verificando se é o fim.")
                    # Lógica para detectar fim da paginação
                    next_button_disabled = False
                    try:
                        # Tenta encontrar o botão "Próximo" e verifica se está desabilitado
                        # O seletor para o botão "Próximo" desabilitado pode ser '.s-pagination-item.s-pagination-next.s-pagination-disabled'
                        driver.find_element(By.CSS_SELECTOR, ".s-pagination-item.s-pagination-next.s-pagination-disabled")
                        logger.info("Botão 'Próximo' está desabilitado. Fim da paginação.")
                        next_button_disabled = True
                    except NoSuchElementException:
                        logger.debug("Botão 'Próximo' não está desabilitado ou não foi encontrado com o seletor de desabilitado.")
                    
                    if next_button_disabled:
                        return total_produtos_usados # Fim real

                    consecutive_empty_pages += 1
                    if consecutive_empty_pages >= max_consecutive_empty_pages:
                        logger.warning(f"{max_consecutive_empty_pages} páginas vazias consecutivas. Considerando fim da busca para {nome_fluxo}.")
                        return total_produtos_usados
                    # Se não for o fim, apenas loga e continua para a próxima página
                    logger.info(f"Página {pagina_atual} vazia (sem itens), mas não é o fim definitivo. Tentativa {consecutive_empty_pages}/{max_consecutive_empty_pages} de páginas vazias.")
                    page_processed_successfully = True # Considera processada para avançar página
                    break # Sai do loop de tentativas da página e vai para a próxima página


                consecutive_empty_pages = 0 # Reseta contador se encontrar itens
                produtos_na_pagina = 0
                for idx, item_element in enumerate(items, 1):
                    try:
                        item_logger = logging.getLogger(f"{logger.name}.Item_{pagina_atual}_{idx}")
                        item_logger.debug(f"Processando item {idx} da página {pagina_atual}")

                        # CORREÇÃO APLICADA AQUI: Usando XPath para SELETOR_INDICADOR_USADO_XPATH
                        try:
                            used_indicator = item_element.find_element(By.XPATH, SELETOR_INDICADOR_USADO_XPATH)
                            item_logger.debug(f"Indicador 'usado' encontrado: {used_indicator.text}")
                        except NoSuchElementException:
                            item_logger.debug("Item não identificado como 'usado' pelo seletor XPath. Ignorando.")
                            continue

                        try:
                            nome_element = item_element.find_element(By.CSS_SELECTOR, SELETOR_NOME_PRODUTO_USADO)
                            nome = nome_element.text.strip()
                            if not nome: # Adiciona verificação de nome vazio
                                item_logger.debug("Nome do produto vazio. Ignorando.")
                                continue
                            item_logger.debug(f"Nome do produto: {nome}")
                        except NoSuchElementException:
                            item_logger.debug(f"Nome do produto não encontrado com seletor '{SELETOR_NOME_PRODUTO_USADO}'. Ignorando.")
                            continue

                        try:
                            link_element = item_element.find_element(By.CSS_SELECTOR, "a.a-link-normal.s-no-outline")
                            link = link_element.get_attribute("href")
                            if not link or not link.startswith("http"): # Adiciona verificação de link válido
                                item_logger.debug(f"Link do produto inválido ou não encontrado: '{link}'. Ignorando.")
                                continue
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

                        price = get_price_from_element(item_element, item_logger)
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
                            preco_historico_info = history.get(asin)
                            if preco_historico_info:
                                preco_historico = preco_historico_info.get("preco_usado")
                                if preco_historico and preco_historico <= price:
                                    item_logger.info(f"ASIN {asin}: Preço atual (R${price:.2f}) não é menor que o histórico (R${preco_historico:.2f}). Ignorando notificação e atualização do histórico.")
                                    continue # Não envia notificação nem atualiza o histórico se o preço não for melhor
                                else:
                                    item_logger.info(f"ASIN {asin}: Novo preço (R${price:.2f}) é menor que o histórico (R${preco_historico if preco_historico else 'N/A'}). Atualizando histórico e notificando.")
                            else:
                                item_logger.info(f"ASIN {asin} não encontrado no histórico. Adicionando e notificando.")
                            
                            # Atualiza o histórico apenas se o preço for melhor ou se o item for novo
                            history[asin] = produto 
                            save_history_geral(history) # Salva o histórico após cada atualização bem-sucedida

                        total_produtos_usados.append(produto)
                        produtos_na_pagina +=1
                        item_logger.info(f"Produto 'usado' qualificado adicionado: {nome} | Preço: R${price:.2f} | ASIN: {asin}")

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
                        item_logger.warning("Elemento tornou-se obsoleto (StaleElementReferenceException) durante o processamento do item. Tentando buscar itens novamente.")
                        break # Sai do loop de itens e tenta recarregar a página/itens
                    except InvalidSelectorException as e_sel:
                        item_logger.error(f"Erro de seletor inválido ao processar item: {e_sel}", exc_info=False)
                        # Não interrompe, apenas loga e continua para o próximo item
                        continue
                    except Exception as e_item:
                        item_logger.error(f"Erro inesperado ao processar item: {e_item}", exc_info=True)
                        continue # Continua para o próximo item
                
                if produtos_na_pagina > 0:
                    logger.info(f"Página {pagina_atual}: {produtos_na_pagina} produtos 'usados' novos ou com preço melhorado processados e notificados.")
                else:
                    logger.info(f"Página {pagina_atual}: Nenhum produto novo ou com preço melhorado encontrado para notificação.")

                page_processed_successfully = True
                break # Sai do loop de tentativas da página, pois foi processada

            except WebDriverException as e_wd:
                if "ERR_PROXY_CONNECTION_FAILED" in str(e_wd) or "ERR_TUNNEL_CONNECTION_FAILED" in str(e_wd) or "ERR_NAME_NOT_RESOLVED" in str(e_wd):
                    logger.error(f"Erro de WebDriver relacionado a proxy/conexão na página {pagina_atual}: {str(e_wd)[:200]}.")
                elif "ERR_NO_SUPPORTED_PROXIES" in str(e_wd): # Este erro pode vir do Chrome quando o proxy falha
                     logger.error(f"Chrome reportou 'ERR_NO_SUPPORTED_PROXIES'. Provável falha no proxy. URL: {url_pagina}")
                else:
                    logger.error(f"Erro de WebDriver ao carregar página {pagina_atual}: {str(e_wd)[:200]}", exc_info=False) # Log mais curto para não poluir muito
                
                if tentativa < max_tentativas_pagina:
                    logger.info(f"Tentando novamente a página {pagina_atual} após delay...")
                    await asyncio.sleep(random.uniform(15, 30)) # Delay maior para erro de WebDriver
                    # Poderia tentar reiniciar o driver aqui em casos extremos, mas aumenta complexidade
                    # Ex: if driver: driver.quit(); driver = iniciar_driver_sync_worker(logger) etc.
                    continue
                else:
                    logger.error(f"Falha crítica após {max_tentativas_pagina} tentativas na página {pagina_atual} devido a WebDriverException. Interrompendo fluxo para {nome_fluxo}.")
                    return total_produtos_usados # Interrompe para esta base_url
            except Exception as e_page:
                logger.error(f"Erro geral e inesperado ao processar página {pagina_atual}: {e_page}", exc_info=True)
                if tentativa < max_tentativas_pagina:
                    await asyncio.sleep(random.uniform(10, 20))
                    continue
                else:
                    logger.error(f"Falha crítica após {max_tentativas_pagina} tentativas na página {pagina_atual} devido a erro geral. Interrompendo fluxo para {nome_fluxo}.")
                    return total_produtos_usados # Interrompe para esta base_url
        
        if not page_processed_successfully:
            logger.error(f"Não foi possível processar a página {pagina_atual} de {nome_fluxo} após {max_tentativas_pagina} tentativas. Abortando este fluxo.")
            return total_produtos_usados


        pagina_atual += 1
        await asyncio.sleep(random.uniform(5, 10)) # Delay entre páginas

    logger.info(
        f"--- Concluído Fluxo: {nome_fluxo}. Máximo de páginas ({max_paginas}) atingido ou fim da paginação. "
        f"Total de produtos 'usados' qualificados encontrados: {len(total_produtos_usados)} ---"
    )
    return total_produtos_usados

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

if __name__ == "__main__":
    asyncio.run(run_usados_geral_scraper_async())
