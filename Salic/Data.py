import pandas as pd
from pandas.io.json import json_normalize
import sqlite3




class Dataframe:
    def __init__(self):
        self.ANOS = ['09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20']

        self.AREAS = ['Artes Cênicas', 'Audiovisual', 'Música', 
                    'Artes Visuais', 'Patrimônio Cultural', 'Humanidades',
                    'Artes Integradas', 'Museus e Memória']

        self.REGIOES = {'AC': 'NORTE', 'AM': 'NORTE', 'AP': 'NORTE', 'PA': 'NORTE', 
                    'RO': 'NORTE', 'RR': 'NORTE', 'TO': 'NORTE', 'AL': 'NORDESTE', 'BA': 'NORDESTE', 
                    'CE': 'NORDESTE', 'MA': 'NORDESTE', 'PB': 'NORDESTE', 'PE': 'NORDESTE', 'PI': 'NORDESTE',
                    'RN': 'NORDESTE', 'SE': 'NORDESTE', 'DF': 'CENTRO-OESTE', 'GO': 'CENTRO-OESTE', 'MS': 'CENTRO-OESTE',
                    'MT': 'CENTRO-OESTE', 'ES': 'SUDESTE', 'MG': 'SUDESTE', 'RJ': 'SUDESTE', 'SP': 'SUDESTE',
                    'PR': 'SUL', 'RS': 'SUL', 'SC': 'SUL'}

        self.TABLES = {'09': [], '10': [], '11': [], '12': [], '13': [], '14': [],
                        '15': [], '16': [], '17': [], '18': [], '19': [], '20': []}



    def normalize(self, data, ano, area, regiao, uf):
        normal = json_normalize(data=data, record_path=[ano, area, regiao, uf])

        return normal
        


    def normalize_data(self, data):
        df = []
        
        for key in data.keys():
            if key in self.ANOS:
                ano = key

                for i in range(len(self.AREAS)):
                    area = self.AREAS[i]

                    for k, v in self.REGIOES.items():
                        uf, regiao = k, v

                        frame = self.normalize(data, ano, area, regiao, uf)

                        if not frame.empty:
                            df.append(frame)
            else: 
                pass

        dataframe = pd.concat(df)
        return ano, dataframe




    def tables(self, data):
        table = dict()



        for key, value in data.items():
            data_ = {key: value}
            ano, table[ano] = self.normalize_data(data_)

        return table



    def SQLTable(self, data, connection):
        table = self.tables(data)

        for ano, tab in table.items():
            tab.to_sql(ano, connection)
        
        connection.commit()


    
    def SQLConnect(self, database):
        connection = sqlite3.connect(database)

        return connection

    
    def base(self, data, database, keep_alive=False):
        connection = self.SQLConnect(database)

        self.SQLTable(data, connection)

        if not keep_alive:
            connection.close()

        else:
            return connection
            



