import os
import unittest

from common import PropertiesReader
from email_service import SentMailsLogger


class TestSentMailsLogger(unittest.TestCase):
    __DEFAULT_NODE_ID = '5'

    @classmethod
    def setUpClass(cls):
        PropertiesReader().load_properties_as_env_vars('settings.json')
        cls.__LOGGER = SentMailsLogger()
        cls.__DEFAULT_FILEPATH = os.path.join(os.environ['ROOT_DIR'], cls.__DEFAULT_NODE_ID, '_'.join([cls.__DEFAULT_NODE_ID, 'sent_mails.log']))

    def test_main(self):
        self.__check_file_initialization()
        self.__check_status_1()
        self.__check_status_2()

    def __check_file_initialization(self):
        path_json = os.path.join(os.environ['ROOT_DIR'], '1', '1_sent_mails.log')
        self.assertFalse(os.path.exists(path_json))
        self.__LOGGER.log_sent_mail_for_node('1', 'TEST')
        self.assertTrue(os.path.exists(path_json))

    def __check_status_1(self):
        self.__LOGGER.log_sent_mail_for_node(self.__DEFAULT_NODE_ID, 'STATUS1')
        result = self.__load_default_file()
        self.assertEqual('Sent mail for status STATUS1 to node id 5', result[0])

    def __check_status_2(self):
        self.__LOGGER.log_sent_mail_for_node(self.__DEFAULT_NODE_ID, 'STATUS2')
        result = self.__load_default_file()
        self.assertEqual('Sent mail for status STATUS2 to node id 5', result[1])

    def __load_default_file(self) -> dict:
        with open(self.__DEFAULT_FILEPATH, 'r', 'a') as file:
            result = file.read().split('\n')[:-1]
            result = [x.split(':')[1].lstrip() for x in result]
            return result


if __name__ == '__main__':
    unittest.main()
