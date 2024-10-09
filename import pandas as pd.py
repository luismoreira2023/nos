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
API_KEY = '0cabb4023745402794659df728ca8813'  # Insira a sua chave de API aqui

# Função para buscar dados da API do CTT
def buscar_dados_ctt(codigo_postal):
    try:
        logging.info(f"Consultando API para o código postal: {codigo_postal}")
        response = requests.get(f"https://www.cttcodigopostal.pt/api/v1/{API_KEY}/{codigo_postal}")

        if response.status_code == 200:
            dados_json = response.json()
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

    total_cps = 0
    cps_nao_guardados = 0

    # Iterar sobre cada linha e buscar dados baseados no código postal
    for index, row in df.iterrows():
        total_cps += 1
        codigo_postal_raw = str(row['cp7']).replace("-", "")
        
        # Verifica se o código postal está no formato correto (7 dígitos)
        if len(codigo_postal_raw) == 7:
            codigo_postal = f"{codigo_postal_raw[:4]}-{codigo_postal_raw[4:]}"
            logging.info(f"Código postal formatado: {codigo_postal}")
        else:
            logging.warning(f"Formato inválido para o código postal na linha {index + 1}: {codigo_postal_raw}")
            cps_nao_guardados += 1
            continue

        # Buscar dados da API do CTT
        dados_cep = buscar_dados_ctt(codigo_postal)
        
        # Verificar se o retorno é válido e não vazio
        if dados_cep and isinstance(dados_cep, list) and len(dados_cep) > 0:
            dados_cep = dados_cep[0]  # Usar o primeiro resultado

            # Verifica se `dados_cep` é um dicionário após o ajuste
            if isinstance(dados_cep, dict):
                # Extrair o concelho e o distrito da resposta da API
                concelho = dados_cep.get('concelho', 'N/A')
                distrito = dados_cep.get('distrito', 'N/A')
                logging.info(f"Concelho: {concelho}, Distrito: {distrito}")

                # Inserir ou ignorar se já existir na tabela
                cursor.execute('''
                    INSERT OR IGNORE INTO codigos_postais (codigo_postal, concelho, distrito)
                    VALUES (?, ?, ?)
                ''', (codigo_postal, concelho, distrito))
                conn.commit()
            else:
                logging.error(f"Formato inesperado de dados: {dados_cep}")
                cps_nao_guardados += 1
        else:
            logging.warning(f"Nenhum dado válido retornado para o código postal {codigo_postal}.")
            cps_nao_guardados += 1
        
        time.sleep(2)  # Respeitar limite de requisições

    logging.info(f"Enriquecimento concluído. Total de códigos postais processados: {total_cps}.")
    logging.info(f"Número de códigos postais não guardados: {cps_nao_guardados}")

    # Fechar a conexão
    conn.close()

# Função para buscar informações do banco de dados com base no código postal
def buscar_dados_por_codigo_postal(codigo_postal, caminho_db):
    # Conectar ao banco de dados SQLite
    conn = sqlite3.connect(caminho_db)
    cursor = conn.cursor()

    try:
        # Formatar o código postal
        codigo_postal_formatado = codigo_postal if '-' in codigo_postal else f"{codigo_postal[:4]}-{codigo_postal[4:]}"

        logging.info(f"Buscando dados para o código postal: {codigo_postal_formatado}")

        # Consultar o banco de dados
        cursor.execute('''
            SELECT concelho, distrito FROM codigos_postais WHERE codigo_postal = ?
        ''', (codigo_postal_formatado,))
        
        resultado = cursor.fetchone()  # Pega o primeiro resultado
        
        if resultado:
            concelho, distrito = resultado
            logging.info(f"Concelho: {concelho}, Distrito: {distrito}")
            return {
                'codigo_postal': codigo_postal_formatado,
                'concelho': concelho,
                'distrito': distrito
            }
        else:
            logging.warning(f"Nenhum dado encontrado para o código postal {codigo_postal_formatado}.")
            return None

    except Exception as e:
        logging.error(f"Erro ao buscar dados para o código postal {codigo_postal}: {e}")
        return None
    finally:
        # Fechar a conexão
        conn.close()

# Caminho para o CSV original e para o banco de dados
caminho_csv = r'C:\Users\Alzira\Desktop\Specialisterne\7 de Outubro\codigos_postais.csv'
caminho_db = r'C:\Users\Alzira\Desktop\Specialisterne\7 de Outubro\codigos_postais.db'

# Chamar a função para processar o CSV e enriquecer o banco de dados
enriquecer_banco_dados(caminho_csv, caminho_db)

# Solicitar o código postal ao usuário para buscar no banco de dados
codigo_postal_escolhido = input("Digite o código postal que deseja buscar: ")

# Buscar e exibir as informações baseadas no código postal fornecido
resultado = buscar_dados_por_codigo_postal(codigo_postal_escolhido, caminho_db)

if resultado:
    print(f"Código Postal: {resultado['codigo_postal']}")
    print(f"Concelho: {resultado['concelho']}")
    print(f"Distrito: {resultado['distrito']}")
else:
    print(f"Não foi possível encontrar informações para o código postal {codigo_postal_escolhido}.")
