import sqlite3


class SQLData:
    def __init__(self, filename):
        self.filename = filename
        self.TABLE = """CREATE TABLE {table_name} (
                        {table_content});"""
        self.INSERT = """INSERT INTO {table_name}
                        ({keys})
                        VALUES
                        ({values});"""
        self.SELECT = """SELECT {selection} FROM {table_name}"""
        self.SELECT_ = self.SELECT + """WHERE {selected}"""
        self.TYPES = {int: 'INTEGER', str: 'TEXT', float: 'REAL'}




    def command_table_create(self, table_name, **kwargs):
        content = []

        for key, value in kwargs.items():
            item = '{v} {k}'.format(v=key, key=self.TYPES[type(value)])
            content.append(item)

        content_ = ',\n'.join(content)
        
        table = self.TABLE.format(table_name=table_name, table_content=content_)

        return table



    def command_table_insert(self, table_name, **kwargs):
        keys, values = [], []

        for key, value in kwargs.items():
            keys.append(key)
            if type(value) == str:
                values.append('"{}"'.format(value))
            
            else:
                values.append(value)

        table = self.INSERT.format(table_name=table_name, keys=', '.join(keys), values=', '.join(values))

        return table


    def command_table_select(self, table_name, ALL=True, **kwargs):
        if ALL == True:
            selection = '*'
        elif type(ALL) == str:
            selection = ALL
        elif type(ALL) == list:
            selection = ', '.join(ALL)
        
        if not kwargs:
            select = self.SELECT.format(selection=selection, table_name=table_name)

            return select
        
        elif kwargs:
            s = '{key} = {value}'
            if len(kwargs.items()) == 1:
                for k, v in kwargs.items():
                    selected = s.format(key=k, value=v)
                    select = self.SELECT_.format(selection=selection, table_name=table_name, selected=selected)

                    return select
                    
            else:
                selected = []
                for k, v in kwargs.items():
                    selected.append(s.format(key=k, value=v))
                
                select = self.SELECT.format(selection=selection, table_name=table_name, selected='AND '.join(selected))
                
                return select


                









