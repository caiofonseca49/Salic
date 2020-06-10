import params_
import requests
import sqlite3



class Salic:
    def __init__(self, url):
        self.URL = url
        self.count = 0
        self.PROJETOS_KEYS = params_.PROJETOS_KEYS.copy()
        self.AREAS = params_.AREAS.copy()
        self.TOTAL = params_.TOTAL.copy()
        self.REGIOES = {'AC': 'NORTE', 'AM': 'NORTE', 'AP': 'NORTE', 'PA': 'NORTE', 
        'RO': 'NORTE', 'RR': 'NORTE', 'TO': 'NORTE', 'AL': 'NORDESTE', 'BA': 'NORDESTE', 
        'CE': 'NORDESTE', 'MA': 'NORDESTE', 'PB': 'NORDESTE', 'PE': 'NORDESTE', 'PI': 'NORDESTE',
        'RN': 'NORDESTE', 'SE': 'NORDESTE', 'DF': 'CENTRO-OESTE', 'GO': 'CENTRO-OESTE', 'MS': 'CENTRO-OESTE',
        'MT': 'CENTRO-OESTE', 'ES': 'SUDESTE', 'MG': 'SUDESTE', 'RJ': 'SUDESTE', 'SP': 'SUDESTE',
        'PR': 'SUL', 'RS': 'SUL', 'SC': 'SUL'}




    def req(self, session, url):
        r = session.get(url)

        return r.json()

        
    def session_request(self, url_):
        session_get = self.TOTAL.copy()

        with requests.Session() as session:
            url = url_

            while True:
                re = self.req(session, url)

                if not self.count:
                    self.count = re['total']
                else:
                    pass

                lista_projetos = re['_embedded']['projetos']

                for n in range(len(lista_projetos)):
                    projeto = lista_projetos[n]
                    projeto_ = {key:projeto[key] for key in list(projeto.keys()) if key in self.PROJETOS_KEYS}
                    session_get[projeto_['ano_projeto']][projeto_['area']][self.REGIOES[projeto_['UF']]][projeto_['UF']].append(projeto_)

                try:
                    url = re['_links']['next']
                except:
                    break
            
        return session_get



class Salic_SQL:
    def __init__(self, database, session_data):
        self.database = database
        self.DATA = session_data

    
    def database_connect(self, filename):
        connection = sqlite3.connect(filename)
        
        return connection

    
    





                        



            