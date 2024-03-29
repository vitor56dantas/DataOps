"Script de ingestão e preparação - projeto dataops MBADE04"

import os
import uuid
import logging
from datetime import datetime

import requests
from dotenv import load_dotenv
import pandas as pd

import utils
from config import configs

load_dotenv()
config_file = configs
logging.basicConfig(level=logging.INFO)

def ingestion():
    """
    Função de ingestão dos dados
    Outputs: Salva base raw em local específico e retorna o nome do arquivo
    """

    logging.info(f"Iniciando a ingestão")
    api_url = os.getenv('URL')
    response = requests.get(api_url, timeout=10).json()

    try:
        data = response['results']
    except Exception as exception_error:
        utils.error_handler(exception_error, 'read_api')

    df = pd.json_normalize(data)

    df['load_date'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    file = f"{config_file['raw_path']}{str(uuid.uuid4())}.csv"
    df.to_csv(file, sep=";", index=False)
    return file

def preparation(file):
    """
    Função de preparação dos dados: renomeia, tipagem, normaliza strings
    Arguments: file -> nome do arquivo raw
    Outputs: Salva base limpa em local específico
    """

    logging.info(f"Iniciando a preparação")
    df = pd.read_csv(file, sep=";")
    san = utils.Saneamento(df, config_file)
    try:
        san.rename()
        logging.info(f"Dados renomeados e selecionados")
    except Exception as exception_error:
        utils.error_handler(exception_error, 'rename_coluns')
    san.tipagem()
    logging.info(f"Dados tipados")
    san.normalize_str()
    logging.info(f"Dados normalizados")
    san.save_work()

if __name__ == '__main__':
    file_name = ingestion()
    preparation(file_name)
    