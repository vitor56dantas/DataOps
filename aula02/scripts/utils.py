from datetime import date
import os
import re
import pandas as pd
from datetime import datetime

class Saneamento:
    
    def __init__(self, data, configs):
        self.data = data
        self.metadado =  pd.read_excel(configs["meta_path"])
        self.len_cols = max(list(self.metadado["id"]))
        self.colunas = list(self.metadado['nome_original'])
        self.colunas_new = list(self.metadado['nome'])
        self.path_work = configs["work_path"]        

    def rename(self):
        self.data = self.data.loc[:, self.colunas] 
        for i in range(self.len_cols):
            self.data.rename(columns={self.colunas[i]:self.colunas_new[i]}, inplace = True)

    def tipagem(self):
        for col in self.colunas_new:
            tipo = self.metadado.loc[self.metadado['nome'] == col]['tipo'].item()
            if tipo == "int":
                tipo = self.data[col].astype(int)
            elif tipo == "float":
                self.data[col].replace(",", ".", regex=True, inplace = True)
                self.data[col] = self.data[col].astype(float)
            elif tipo == "date":
                self.data[col] = pd.to_datetime(self.data[col]).dt.strftime('%Y-%m-%d')
    
    def normalize_str(self):
        for col in self.colunas_new:
            tipo = self.metadado.loc[self.metadado['nome'] == col]['tipo'].item()
            if tipo == "string":
                self.data[col] = self.data[col].apply(
                    lambda x: x.encode('ASCII', 'ignore')\
                        .decode("utf-8").lower() if x != None else None)
    
    def save_work(self):
        self.data['load_date'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        if not os.path.exists(self.path_work):
            self.data.to_csv(self.path_work, index=False, sep = ";")
        else:
            self.data.to_csv(self.path_work, index=False, mode='a', header=False, sep = ";")


def error_handler(exception_error, stage):
    
    log = [stage, type(exception_error).__name__, exception_error,datetime.now()]
    logdf = pd.DataFrame(log).T
    
    if not os.path.exists("logs_file.txt"):
        logdf.columns = ['stage', 'type', 'error', 'datetime']
        logdf.to_csv("logs_file.txt", index=False,sep = ";")
    else:
        logdf.to_csv("logs_file.txt", index=False, mode='a', header=False, sep = ";")
