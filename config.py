import yaml
from os.path import isfile

class Configuration:
    __config = None

    def __init__(self, config_file = './config.yml'):
        assert isfile(config_file), 'Configuration file is missing.'

        with open('config.yml') as config_file:
            self.__config = yaml.load(config_file)

        for parameter in ['mysql', 'redis', 'max_workers']:
            assert parameter in self.__config, 'Configuration for "%s" is missing.' % (parameter,)

    def get_mysql_parameters(self, connection_name):
        assert connection_name in self.__config['mysql'], \
            'Connection settings "%s" not found.' % (connection_name,)

        return self.__config['mysql'][connection_name]

    def get_redis_parameters(self):
        return self.__config['redis']

    def get_max_workers(self):
        return self.__config['max_workers']
