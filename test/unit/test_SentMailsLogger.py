import os
import sys
import unittest
from pathlib import Path
from shutil import rmtree

this_path = Path(os.path.realpath(__file__))
path_src = os.path.join(this_path.parents[2], 'src')
sys.path.insert(0, path_src)

from common import ConfigReader
from email_service import SentMailsLogger


class TestSentMailsLogger(unittest.TestCase):
    __DEFAULT_NODE_ID = '5'
    __WORKING_DIR: str

    @classmethod
    def setUpClass(cls):
        path_settings = os.path.join(this_path.parents[1], 'resources', 'settings.toml')
        ConfigReader().load_config_as_env_vars(path_settings)
        cls.__LOGGER = SentMailsLogger()
        cls.__WORKING_DIR = os.environ['DIR.WORKING'] if os.environ['DIR.WORKING'] else os.getcwd()
        cls.__DEFAULT_FILEPATH = os.path.join(cls.__WORKING_DIR, cls.__DEFAULT_NODE_ID, '_'.join([cls.__DEFAULT_NODE_ID, 'sent_mails.log']))

    @classmethod
    def tearDownClass(cls):
        rmtree(cls.__WORKING_DIR)

    def test_main(self):
        self.__check_file_initialization()
        self.__check_status_1()
        self.__check_status_2()

    def __check_file_initialization(self):
        self.__init_node_dir_if_nonexisting('1')
        path_json = os.path.join(os.environ['DIR.WORKING'], '1', '1_sent_mails.log')
        self.assertFalse(os.path.exists(path_json))
        self.__LOGGER.log_sent_mail_for_node('1', 'TEST')
        self.assertTrue(os.path.exists(path_json))

    def __check_status_1(self):
        self.__init_node_dir_if_nonexisting(self.__DEFAULT_NODE_ID)
        self.__LOGGER.log_sent_mail_for_node(self.__DEFAULT_NODE_ID, 'STATUS1')
        result = self.__load_default_file()
        self.assertEqual('Sent mail for status STATUS1 to node id 5', result[0])

    def __check_status_2(self):
        self.__init_node_dir_if_nonexisting(self.__DEFAULT_NODE_ID)
        self.__LOGGER.log_sent_mail_for_node(self.__DEFAULT_NODE_ID, 'STATUS2')
        result = self.__load_default_file()
        self.assertEqual('Sent mail for status STATUS2 to node id 5', result[1])

    def __load_default_file(self) -> dict:
        with open(self.__DEFAULT_FILEPATH, 'r', encoding='utf-8') as file:
            result = file.read().split('\n')[:-1]
            result = [x.split(' : ')[1].lstrip() for x in result]
            return result

    def __init_node_dir_if_nonexisting(self, node_id: str):
        node_working_dir = os.path.join(self.__WORKING_DIR, node_id)
        if not os.path.exists(node_working_dir):
            os.makedirs(node_working_dir)


if __name__ == '__main__':
    unittest.main()
