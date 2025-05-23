from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import InvalidSelectorException, TimeoutException
import logging
import time

async def process_used_products_geral_async(driver, url_base, max_pages, history_data, telegram_bot):
    logging.info(f"--- Iniciando processamento para: Amazon Quase Novo (Geral) --- URL base: {url_base} ---")
    SELETOR_INDICADOR_USADO = "span.a-size-small.a-color-secondary"  # Exemplo: Atualize para o seletor correto
    page = 1
    
    while page <= max_pages:
        page_url = f"{url_base}&page={page}&qid={int(time.time() * 1000)}&ref=sr_pg_{page}"
        logging.info(f"[Amazon Quase Novo (Geral)] Carregando Página: {page}/{max_pages}, URL: {page_url}")
        
        try:
            # Verificar status da URL
            status = check_url_status(page_url)
            if status != 200:
                logging.warning(f"URL retornou status não-200 ({status}). Tentando carregar mesmo assim.")
            
            # Carregar página
            driver.get(page_url)
            wait_for_page_load(driver)
            
            # Verificar página de erro da Amazon
            check_amazon_error_page_sync_worker(driver)
            
            # Encontrar contêiner de resultados
            container = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.s-main-slot.s-result-list"))
            )
            logging.info(f"Contêiner de resultados encontrado na página {page}.")
            
            # Encontrar todos os itens de produtos
            items = container.find_elements(By.CSS_SELECTOR, "div.s-result-item")
            logging.info(f"Página {page}: Encontrados {len(items)} elementos com seletor principal.")
            
            # Processar cada item
            for idx, item in enumerate(items, 1):
                try:
                    # Verificar indicador de item usado
                    used_indicator = WebDriverWait(item, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, SELETOR_INDICADOR_USADO))
                    )
                    logging.info(f"Item {idx} na página {page}: Indicador de usado encontrado: {used_indicator.text}")
                    
                    # Continuar processamento (ex.: extrair preço, título, etc.)
                    # Adicione sua lógica aqui
                    
                except (InvalidSelectorException, TimeoutException) as e:
                    logging.error(f"Erro ao processar item {idx} na página {page}: {str(e)}")
                    continue
                
            page += 1
            
        except Exception as e:
            logging.error(f"Erro ao processar página {page}: {str(e)}")
            break
    
    logging.info("Processamento concluído.")
