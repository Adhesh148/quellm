from abc import abstractmethod

class Datasource:
    """
    A base class for all data sources. Provides a uniform interface for accessing data.
    
    Parameters
    ----------
    name : str
        The name of the data source.
    type : str
        The type of the data source. (Example: MySQL, PostgreSQL, MongoDB, ElasticSearch, etc.)
    """

    def __init__(self, name, type, info):
        self.name = name
        self.type = type
        self.info = info

    def get_name(self):
        return self.name

    def get_type(self):
        return self.type

    def get_info(self):
        return self.info

    @abstractmethod
    def get_table_names(self):
        pass

    @abstractmethod
    def get_table_schemas(self):
        pass

    @abstractmethod
    def run_query(self, query:str):
        pass

    
    