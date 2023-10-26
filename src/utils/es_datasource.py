from elasticsearch import Elasticsearch
from utils.datasource import Datasource
import re

SYSTEM_INDICES = ['.kibana', '.internal', 'metrics-endpoint']

class ElasticSearchDataSource(Datasource):

    def __init__(self, config):
        self.name = config['name']
        self.type = config['type']
        self.info = config['info']
        self.host = config['host']
        self.user = config['user']
        self.pwd = config['pwd']
        self.client = self.createEsClient(self.host, self.user, self.pwd)
    
    def createEsClient(self, host, user, pwd):
        """returns an elasticsearch client"""
        try:
            client = Elasticsearch(hosts=host, http_auth=(user, pwd))
            return client
        except Exception as e:
            raise e

    def get_usable_indices(self):
        indices = list(self.client.indices.get_alias(index="*").keys())
        # filter out internal and metrics indices
        usable_indices =list(filter(lambda x: not any(indx in x for indx in SYSTEM_INDICES), indices))
        return usable_indices

    def get_table_names(self):
        return self.get_usable_indices()

    def get_table_schemas(self):
        usable_indices = self.get_usable_indices()
        schemas = '\n'
        for indx in usable_indices:
            mapping = self.client.indices.get_mapping(index=indx, request_timeout=30)
            schemas = schemas + str(mapping)  + '\n'
        return schemas.replace("{", "{{").replace("}", "}}")

    def parse_json_query(self, json_query: str):
        # extract index and body from json_query
        index_match = re.search(r'POST\s+/(\w+)', json_query)
        query_type_match = re.search(r'POST\s+/\w+/(.+)\n{', json_query)

        if index_match:
            index = index_match.group(1)
        else:
            raise Exception('Invalid index')

        if query_type_match:
            query_type = query_type_match.group(1)
        else:
            raise Exception('Invalid query type')

        # Use regular expression to extract the JSON body
        match = re.search(r'{\s*".+}', json_query, re.DOTALL)
        if match:
            extracted_json = match.group()
        else:
            raise Exception('Invalid body')

        return [index, query_type, extracted_json]

    def parse_query_dict(self, json_dict: str):
        """temporary parsing function"""
        return [json_dict['index'], '_search', '{' + str(json_dict['body']) + '}']

    def run_query(self, json_query: str):
        index, query_type, body = self.parse_json_query(json_query)
        try:
            if query_type == "_search":
                response = self.client.search(index=index, body=body)
                return response
            else:
                raise Exception('Invalid query type')
        except Exception as e:
            raise e