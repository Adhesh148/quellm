import yaml
from utils.sql_datasource import SQLDatasource

PATH_TO_DATASOURCE_CONFIG = 'config/datasource.yaml'

class DataSourceLoader():
    """Loads datasource configurations and instantiates datasources"""

    def __init__(self, datasource_path=PATH_TO_DATASOURCE_CONFIG):
        self.datasource_config_list = self.get_datasource_config(datasource_path)

    @staticmethod
    def validate_config(config_list):
        """validates the datasource configurations loaded from yaml file
        Args:
            config_list (list): list of datasource configurations
        Raises:
            Exception: Invalid datasource configuration
        Note:
            Currently only supports SQL Datasource
        Example:
            datasource_config_list = [
                {
                    'name': 'sql_datasource',
                    'type': 'sql',
                    'host': 'localhost',
                    'port': '5432',
                    'user': 'postgres',
                    'password': 'postgres',
                    'database': 'postgres'
                }
            ]
        Returns:
            None
        """

        for config in config_list:
            config_keys = config.keys()
            if 'name' not in config_keys or 'type' not in config_keys:      # basic config-check - TODO: extend further
                raise Exception('Invalid datasource configuration')


    def get_datasource_config(self, datasource_path):
        """Returns datasource configuration"""

        with open(datasource_path, 'r') as f:
            datasource_config = yaml.safe_load(f)

        self.validate_config(datasource_config)
        return datasource_config

    def load_datasources(self):
        """loads the datasource configurations and instantiates datasources
        Returns:
            list: list of datasource objects
        Raises:
            Exception: Invalid datasource type
        Note:
            Currently only supports SQL Datasource
        """
        datasources = {}
        for config in self.datasource_config_list:
            if config['type'] == 'mysql':
                datasources[config['name']] = {
                    'info': config['info'],
                    'datasource':  SQLDatasource(config)
                }
            else:
                raise Exception('Invalid datasource type')

        return datasources