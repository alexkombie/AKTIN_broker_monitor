import os
import unittest
from datetime import datetime
from shutil import rmtree

from common import PropertiesReader
from dateutil import parser
from node_to_csv import NodeResourceFetcher
from pytz import timezone

from BrokerNodeDummy import BrokerNodeDummy, BrokerNodeImportScripts, BrokerNodePython, BrokerNodeRscript, BrokerNodeVersions, BrokerNodeVersions2


class TestNodeResourceFetcher(unittest.TestCase):
    __DEFAULT_NODE_ID: str = '0'
    __DEFAULT_API_KEY: str = 'xxxApiKey123'
    __DIR_ROOT: str = None
    __DEFAULT_LOG_FILENAME: str = '0_log_versions.log'

    @classmethod
    def setUpClass(cls):
        PropertiesReader().load_properties_as_env_vars('settings.json')
        cls.__DIR_ROOT = os.environ['ROOT_DIR'] if os.environ['ROOT_DIR'] else os.getcwd()
        cls.__FETCHER = NodeResourceFetcher()
        cls.__DUMMY = BrokerNodeDummy(cls.__DEFAULT_API_KEY)

    def tearDown(self):
        [rmtree(name) for name in os.listdir(self.__DIR_ROOT) if os.path.isdir(name) and len(name) <= 2]

    def test_fetch_broker_node_versions(self):
        versions = BrokerNodeVersions('1', '2')
        self.__DUMMY.put_resource_on_broker(versions, 'versions')
        self.__FETCHER.fetch_broker_data_to_file(self.__DEFAULT_NODE_ID)
        content = self.__get_content_of_file_in_working_dir('0_versions.txt')
        self.assertEqual('{"java": "1", "os": "2"}', content)

    def test_fetch_broker_node_empty_rscript(self):
        rscript = BrokerNodeRscript('', '', '')
        self.__DUMMY.put_resource_on_broker(rscript, 'rscript')
        self.__FETCHER.fetch_broker_data_to_file(self.__DEFAULT_NODE_ID)
        content = self.__get_content_of_file_in_working_dir('0_rscript.txt')
        self.assertEqual('{"r-base-core": "-", "r-cran-tidyverse": "-", "r-cran-lattice": "-"}', content)

    def test_fetch_broker_node_python(self):
        python = BrokerNodePython('python1', 'python2', '')
        self.__DUMMY.put_resource_on_broker(python, 'python')
        self.__FETCHER.fetch_broker_data_to_file(self.__DEFAULT_NODE_ID)
        content = self.__get_content_of_file_in_working_dir('0_python.txt')
        self.assertEqual('{"python3": "python1", "python3-numpy": "python2", "python3-pandas": "-"}', content)

    def test_fetch_broker_node_import_scripts(self):
        scripts = BrokerNodeImportScripts('1.5')
        self.__DUMMY.put_resource_on_broker(scripts, 'import-scripts')
        self.__FETCHER.fetch_broker_data_to_file(self.__DEFAULT_NODE_ID)
        content = self.__get_content_of_file_in_working_dir('0_import-scripts.txt')
        self.assertEqual('{"p21": "1.5"}', content)

    def __get_content_of_file_in_working_dir(self, filename: str) -> str:
        path_file = os.path.join(self.__DIR_ROOT, self.__DEFAULT_NODE_ID, filename)
        with open(path_file, 'r') as file:
            content = file.read()
        return content

    def __check_file_existance_in_working_dir(self, filename: str) -> bool:
        path_file = os.path.join(self.__DIR_ROOT, self.__DEFAULT_NODE_ID, filename)
        return os.path.exists(path_file)

    def test_logging_broker_node_versions(self):
        self.__no_log_creation_on_no_changes()
        self.__DUMMY.put_resource_on_broker(BrokerNodeVersions('3\n', '4\n'), 'versions')
        self.__FETCHER.fetch_broker_data_to_file(self.__DEFAULT_NODE_ID)
        self.__DUMMY.put_resource_on_broker(BrokerNodeVersions2('3', '4', '1', '2'), 'versions')
        self.__FETCHER.fetch_broker_data_to_file(self.__DEFAULT_NODE_ID)
        self.__DUMMY.put_resource_on_broker(BrokerNodeVersions('3', '4'), 'versions')
        self.__FETCHER.fetch_broker_data_to_file(self.__DEFAULT_NODE_ID)
        self.__check_versions_log()

    def __no_log_creation_on_no_changes(self):
        versions = BrokerNodeVersions('1', '2')
        self.__DUMMY.put_resource_on_broker(versions, 'versions')
        self.__FETCHER.fetch_broker_data_to_file(self.__DEFAULT_NODE_ID)
        self.assertFalse(self.__check_file_existance_in_working_dir(self.__DEFAULT_LOG_FILENAME))
        self.__DUMMY.put_resource_on_broker(versions, 'versions')
        self.__FETCHER.fetch_broker_data_to_file(self.__DEFAULT_NODE_ID)
        self.assertFalse(self.__check_file_existance_in_working_dir(self.__DEFAULT_LOG_FILENAME))

    def __check_versions_log(self):
        """
        It is uncertain in which order changes are logged
        """
        self.assertTrue(self.__check_file_existance_in_working_dir(self.__DEFAULT_LOG_FILENAME))
        log_content = self.__get_content_of_file_in_working_dir(self.__DEFAULT_LOG_FILENAME)
        content = self.__extract_timestamp_and_operation_from_log_content(log_content)
        current_hour = datetime.now(timezone('Europe/Berlin')).strftime('%Y-%m-%d %H')
        self.assertEqual(current_hour, content[0][0])
        self.assertEqual(current_hour, content[1][0])
        try:
            self.assertEqual('[os] 2 --> 4', content[0][1])
            self.assertEqual('[java] 1 --> 3', content[1][1])
        except:
            self.assertEqual('[java] 1 --> 3', content[0][1])
            self.assertEqual('[os] 2 --> 4', content[1][1])
        self.assertEqual(current_hour, content[2][0])
        self.assertEqual(current_hour, content[3][0])
        try:
            self.assertEqual('[postgres] NEW --> 2', content[2][1])
            self.assertEqual('[apache2] NEW --> 1', content[3][1])
        except:
            self.assertEqual('[apache2] NEW --> 1', content[2][1])
            self.assertEqual('[postgres] NEW --> 2', content[3][1])
        self.assertEqual(current_hour, content[4][0])
        self.assertEqual(current_hour, content[5][0])
        try:
            self.assertEqual('[postgres] 2 --> DELETED', content[4][1])
            self.assertEqual('[apache2] 1 --> DELETED', content[5][1])
        except:
            self.assertEqual('[apache2] 1 --> DELETED', content[4][1])
            self.assertEqual('[postgres] 2 --> DELETED', content[5][1])

    @staticmethod
    def __extract_timestamp_and_operation_from_log_content(log_content: str) -> list:
        """
        Remove last item from log_split as it is an empty line
        """
        log_split = log_content.split('\n')
        log_split = log_split[:-1]
        list_content = []
        [list_content.append(item.split(' : ')) for item in log_split]
        for idx, item in enumerate(list_content.copy()):
            d = parser.parse(item[0]).astimezone(timezone('Europe/Berlin'))
            list_content[idx][0] = d.strftime('%Y-%m-%d %H')
        return list_content


if __name__ == '__main__':
    unittest.main()
