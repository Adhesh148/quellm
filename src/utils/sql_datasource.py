from utils.datasource import Datasource
from langchain.sql_database import SQLDatabase  # to replace with custom class

import logging

logging.basicConfig(level=logging.INFO)

class SQLDatasource(Datasource):
    """
    Implementation of SQL datasource
    """

    def __init__(self, config):
        self.name = config['name']
        self.type = config['type']
        self.info = config['info']
        self.host = config['host']
        self.port = config['port']
        self.user = config['user']
        self.password = config['pwd']
        self.database = config['db']
        uri = self.get_uri()
        self.engine = SQLDatabase.from_uri(uri, sample_rows_in_table_info=0)
    

    def get_uri(self):
        if self.type == 'mysql':
            return f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        elif self.type == 'postgres':
            return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        elif self.type == 'mssql':
            return f'mssql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'


    def get_table_names(self):
        self.engine.get_usable_table_names()

    def get_table_schemas(self):
        return self.engine.get_table_info()

    def run_query(self, query: str):
        return self.engine._execute(query)