import logging
import pandas as pd
import sqlite3
import requests
import os
import time

# Configurar o logging para exibir no console
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Adicione sua chave de API aqui
API_KEY = '0cabb4023745402794659df728ca8813'  # Insira a sua chave de API

# Buscar dados da API do CTT
def buscar_dados_ctt(codigo_postal):
    try:
        logging.info(f"Consultando API para o código postal: {codigo_postal}")
        url = f"https://www.cttcodigopostal.pt/api/v1/{API_KEY}/{codigo_postal}"
        response = requests.get(url)

        if response.status_code == 200:
            dados_json = response.json()
            if not dados_json:  # Verificar se a resposta foi um array vazio
                logging.warning(f"A API retornou um array vazio para o código postal {codigo_postal}")
                return None
            logging.info(f"Dados recebidos da API: {dados_json}")  # Exibir os dados retornados pela API no log
            return dados_json  # Retorna os dados em formato JSON
        else:
            logging.error(f"Erro {response.status_code}: Problema ao processar o código postal {codigo_postal}.")
            return None
    except Exception as e:
        logging.error(f"Erro ao buscar dados para o código postal {codigo_postal}: {e}")
        return None

# Função para enriquecer o banco de dados SQLite
def enriquecer_banco_dados(caminho_csv, caminho_db):
    total_codigos = 0
    codigos_nao_guardados = 0
    array_vazio = 0
    outros_erros = 0

    # Verifica se o arquivo CSV existe
    if not os.path.exists(caminho_csv):
        logging.error(f"Arquivo CSV '{caminho_csv}' não encontrado.")
        return

    # Conectar ao banco de dados SQLite
    conn = sqlite3.connect(caminho_db)
    cursor = conn.cursor()

    # Criar a tabela se ela não existir
    cursor.execute('''CREATE TABLE IF NOT EXISTS codigos_postais (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigo_postal TEXT UNIQUE,
        concelho TEXT,
        distrito TEXT
    )''')
    conn.commit()

    # Ler o CSV original
    try:
        df = pd.read_csv(caminho_csv)
        logging.info(f"Colunas no CSV: {df.columns.tolist()}")  # Exibir as colunas do CSV
        logging.info("Exibindo as primeiras linhas do CSV:")
        logging.info(df.head().to_string())  # Exibir as primeiras linhas no log
    except Exception as e:
        logging.error(f"Erro ao ler o CSV: {e}")
        return

    # Total de códigos postais no CSV
    total_codigos = len(df)

    # Iterar sobre cada linha e buscar dados baseados no código postal
    for index, row in df.iterrows():
        codigo_postal_raw = str(row['cp7']).replace("-", "")
        
        # Verifica se o código postal está no formato correto (7 dígitos)
        if len(codigo_postal_raw) == 7:
            codigo_postal = f"{codigo_postal_raw[:4]}-{codigo_postal_raw[4:]}"
            logging.info(f"Código postal formatado: {codigo_postal}")
        else:
            logging.warning(f"Formato inválido para o código postal na linha {index + 1}: {codigo_postal_raw}")
            codigos_nao_guardados += 1
            outros_erros += 1
            continue

        # Buscar dados da API do CTT
        dados_cep = buscar_dados_ctt(codigo_postal)
        
        if dados_cep:
            # Se for uma lista, pegar o primeiro elemento
            if isinstance(dados_cep, list) and len(dados_cep) > 0:
                dados_cep = dados_cep[0]
            
            # Verifica se `dados_cep` é um dicionário após o ajuste
            if isinstance(dados_cep, dict):
                # Extrair o concelho e o distrito da resposta da API
                concelho = dados_cep.get('concelho', 'N/A')
                distrito = dados_cep.get('distrito', 'N/A')
                logging.info(f"Concelho: {concelho}, Distrito: {distrito}")
                
                # Inserir os dados no banco de dados
                try:
                    cursor.execute('''INSERT OR IGNORE INTO codigos_postais (codigo_postal, concelho, distrito)
                                      VALUES (?, ?, ?)''', (codigo_postal, concelho, distrito))
                    conn.commit()
                except sqlite3.Error as e:
                    logging.error(f"Erro ao inserir dados para o código postal {codigo_postal}: {e}")
                    codigos_nao_guardados += 1
                    outros_erros += 1
            else:
                logging.error(f"Formato inesperado de dados para o código postal {codigo_postal}: {dados_cep}")
                codigos_nao_guardados += 1
                outros_erros += 1
        else:
            logging.warning(f"Nenhum dado retornado da API para o código postal {codigo_postal}.")
            codigos_nao_guardados += 1
            array_vazio += 1

        # Aguardar 2 segundos entre as requisições para evitar o limite da API
        time.sleep(2)

    logging.info("Enriquecimento concluído.")
    
    # Verificar o estado final da tabela
    cursor.execute("SELECT COUNT(*) FROM codigos_postais")
    total_registros = cursor.fetchone()[0]
    logging.info(f"Total de registros na tabela 'codigos_postais': {total_registros}")

    # Exibir os resultados finais
    logging.info(f"Total de códigos postais no CSV: {total_codigos}")
    logging.info(f"Total de códigos postais não guardados: {codigos_nao_guardados}")
    logging.info(f"Motivo - Respostas vazias da API: {array_vazio}")
    logging.info(f"Motivo - Outros erros: {outros_erros}")

    # Fechar a conexão
    conn.close()

# Caminho para o CSV original e para o banco de dados
caminho_csv = r'C:\Users\Alzira\Desktop\Specialisterne\7 de Outubro\codigos_postais.csv'
caminho_db = r'C:\Users\Alzira\Desktop\Specialisterne\7 de Outubro\codigos_postais.db'

# Chamar a função para processar o CSV e enriquecer o banco de dados
enriquecer_banco_dados(caminho_csv, caminho_db)
