version: 2.1

parameters:
  min_desconto_usados:
    type: string
    default: "40"
  usar_historico_usados:
    type: boolean
    default: true

jobs:
  executar_scraper_usados:
    docker:
      - image: cimg/python:3.11-browsers
    environment:
      PYTHONUNBUFFERED: "1"
      MIN_DESCONTO_PERCENTUAL_USADOS: << pipeline.parameters.min_desconto_usados >>
      USAR_HISTORICO_GLOBAL_USADOS: << pipeline.parameters.usar_historico_usados >>
    steps:
      - checkout
      - run:
          name: Instalar dependências Python
          command: |
            python -m pip install --upgrade pip
            pip install selenium python-telegram-bot==20.3 webdriver-manager
      - restore_cache:
          name: Restaurar histórico
          keys:
            - v1-usados-historico-cache-{{ .Branch }}-
            - v1-usados-historico-cache-
      - run:
          name: Executar Script Orquestrador
          no_output_timeout: 30m
          command: python scripts/orchestrator_usados.py
      - run:
          name: Verificar arquivos gerados
          command: |
            ls -la history_files_usados/ || echo "Diretório vazio."
            ls -la debug_logs_usados/ || echo "Diretório vazio."
      - save_cache:
          name: Salvar histórico
          key: v1-usados-historico-cache-{{ .Branch }}-{{ epoch }}
          paths:
            - "history_files_usados"
      - store_artifacts:
          path: "history_files_usados"
          destination: history_files_usados
          when: always
      - store_artifacts:
          path: "debug_logs_usados"
          destination: debug_logs_usados
          when: always

workflows:
  workflow_usados:
    jobs:
      - executar_scraper_usados
