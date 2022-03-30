import os
import unittest
from node_to_csv import BrokerNodeResourceFetcher
from common import load_properties_file_as_environment

from BrokerNodeDummy import BrokerNodeDummy
from BrokerNodeDummy import BrokerNodeVersions
from BrokerNodeDummy import BrokerNodeRscript
from BrokerNodeDummy import BrokerNodePython
from BrokerNodeDummy import BrokerNodeImportScripts


class TestBrokerNodeResourceFetcher(unittest.TestCase):
    __DEFAULT_NODE_ID: str = '0'
    __DEFAULT_API_KEY: str = 'xxxApiKey123'
    __DIR_WORKING: str = None

    @classmethod
    def setUpClass(cls):
        load_properties_file_as_environment('settings.json')
        cls.__DIR_WORKING = os.environ['ROOT_DIR'] if os.environ['ROOT_DIR'] else os.getcwd()
        cls.__FETCHER = BrokerNodeResourceFetcher(cls.__DEFAULT_NODE_ID, cls.__DIR_WORKING)
        cls.__DUMMY = BrokerNodeDummy(cls.__DEFAULT_API_KEY)

    def tearDown(self):
        [os.remove(name) for name in os.listdir(self.__DIR_WORKING) if '.txt' in name]

    def test_fetch_broker_node_versions(self):
        versions = BrokerNodeVersions('1', '2', '3', '4')
        self.__DUMMY.put_resource_on_broker(versions, 'versions')
        self.__FETCHER.fetch_broker_node_versions()
        content = self.__get_text_file_in_working_dir('0_versions.txt')
        self.assertEqual('{"java": "1", "os": "2", "apache2": "3", "postgres": "4"}', content)

    def test_fetch_broker_node_empty_rscript(self):
        rscript = BrokerNodeRscript('', '', '')
        self.__DUMMY.put_resource_on_broker(rscript, 'rscript')
        self.__FETCHER.fetch_broker_node_rscript()
        content = self.__get_text_file_in_working_dir('0_rscript.txt')
        self.assertEqual('{"r-base-core": "-", "r-cran-tidyverse": "-", "r-cran-lattice": "-"}', content)

    def test_fetch_broker_node_python(self):
        python = BrokerNodePython('python1', 'python2', '')
        self.__DUMMY.put_resource_on_broker(python, 'python')
        self.__FETCHER.fetch_broker_node_python()
        content = self.__get_text_file_in_working_dir('0_python.txt')
        self.assertEqual('{"python3": "python1", "python3-numpy": "python2", "python3-pandas": "-"}', content)

    def test_fetch_broker_node_import_scripts(self):
        scripts = BrokerNodeImportScripts('1.5')
        self.__DUMMY.put_resource_on_broker(scripts, 'import-scripts')
        self.__FETCHER.fetch_broker_node_import_scripts()
        content = self.__get_text_file_in_working_dir('0_import-scripts.txt')
        self.assertEqual('{"p21": "1.5"}', content)

    def __get_text_file_in_working_dir(self, filename: str) -> str:
        path_file = os.path.join(self.__DIR_WORKING, filename)
        with open(path_file, 'r') as file:
            content = file.read()
        return content


if __name__ == '__main__':
    unittest.main()
