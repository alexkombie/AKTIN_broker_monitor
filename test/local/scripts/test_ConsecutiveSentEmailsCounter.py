import json
import os
import unittest
from datetime import datetime, timedelta

from common import PropertiesReader
from email_service import ConsecutiveSentEmailsCounter


class TestConsecutiveSentEmailsCounter(unittest.TestCase):
    __DEFAULT_FILENAME: str = 'testfile'

    @classmethod
    def setUpClass(cls):
        PropertiesReader().load_properties_as_env_vars('settings.json')
        cls.__COUNTER = ConsecutiveSentEmailsCounter(cls.__DEFAULT_FILENAME)
        cls.__DEFAULT_FILEPATH = os.path.join(os.environ['ROOT_DIR'], ''.join([cls.__DEFAULT_FILENAME, 'json']))

    def test_main(self):
        self.__check_file_initialization()
        self.__check_adding_new_key()
        self.__check_deleting_existing_key()
        self.__check_adding_and_updating_key()
        self.__check_deleting_unknown_key()

    def __check_file_initialization(self):
        path_json = os.path.join(os.environ['ROOT_DIR'], 'test_file.json')
        self.assertFalse(os.path.exists(path_json))
        _ = ConsecutiveSentEmailsCounter('test file')
        self.assertTrue(os.path.exists(path_json))

    def __check_adding_new_key(self):
        id_node = '1'
        file = self.__load_default_file()
        self.assertFalse(id_node in file.keys())
        self.__COUNTER.create_or_update_node_entry('1')
        self.assertTrue(id_node in file.keys())

    def __check_deleting_existing_key(self):
        id_node = '1'
        file = self.__load_default_file()
        self.assertTrue(id_node in file.keys())
        self.__COUNTER.delete_entry_for_node_if_exists('1')
        self.assertFalse(id_node in file.keys())

    def __check_adding_and_updating_key(self):
        file = self.__load_default_file()
        self.assertEqual(1, len(file))
        self.__COUNTER.create_or_update_node_entry('2')
        self.assertEqual(2, len(file))
        self.__COUNTER.create_or_update_node_entry('2')
        self.assertEqual(2, len(file))

    def __check_deleting_unknown_key(self):
        file = self.__load_default_file()
        self.assertEqual(2, len(file))
        self.__COUNTER.delete_entry_for_node_if_exists('3')
        self.assertEqual(2, len(file))

    def __check_for_reached_waiting_threshold(self):
        file = self.__load_default_file()
        file['2'] = self.__get_last_weeks_timestamp()
        self.assertTrue(self.__COUNTER.is_waiting_threshold_reached_for_node('2'))

    def __check_for_not_reached_waiting_threshold(self):
        """
        WEEKS_NOTIFICATION_INTERVAL for id_node=3 in mapping.json is set to 2 weeks
        """
        file = self.__load_default_file()
        file['3'] = self.__get_last_weeks_timestamp()
        self.assertFalse(self.__COUNTER.is_waiting_threshold_reached_for_node('3'))

    def __load_default_file(self) -> dict:
        with open(self.__DEFAULT_FILEPATH, 'r', encoding='utf-8') as file:
            return json.load(file)

    @staticmethod
    def __get_last_weeks_timestamp():
        date = datetime.now('Europe/Berlin')
        date = date - timedelta(days=7, hours=1)
        return str(date)


if __name__ == '__main__':
    unittest.main()
