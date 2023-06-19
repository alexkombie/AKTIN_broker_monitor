import json
import os
import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from shutil import rmtree

from pytz import timezone

this_path = Path(os.path.realpath(__file__))
path_src = os.path.join(this_path.parents[2], 'src')
sys.path.insert(0, path_src)

from common import ConfigReader
from email_service import ConsecutiveSentEmailsCounter


class TestConsecutiveSentEmailsCounter(unittest.TestCase):
    __DEFAULT_FILENAME: str = 'testfile'

    @classmethod
    def setUpClass(cls):
        path_settings = os.path.join(this_path.parents[1], 'resources', 'settings.toml')
        ConfigReader().load_config_as_env_vars(path_settings)
        cls.__WORKING_DIR = cls.__init_working_dir()
        cls.__COUNTER = ConsecutiveSentEmailsCounter(cls.__DEFAULT_FILENAME)
        cls.__DEFAULT_FILEPATH = os.path.join(os.environ['DIR.WORKING'], '.'.join([cls.__DEFAULT_FILENAME, 'json']))

    @staticmethod
    def __init_working_dir():
        working_dir = os.environ['DIR.WORKING'] if os.environ['DIR.WORKING'] else os.getcwd()
        if not os.path.exists(working_dir):
            os.makedirs(working_dir)
        return working_dir

    def tearDown(self):
        rmtree(self.__WORKING_DIR)

    def test_main(self):
        self.__check_file_initialization()
        self.__check_adding_new_key()
        self.__check_deleting_existing_key()
        self.__check_adding_and_updating_key()
        self.__check_deleting_unknown_key()
        self.__check_for_reached_waiting_threshold()
        self.__check_for_not_reached_waiting_threshold()
        self.__check_for_reached_waiting_threshold_of_unknown_key()

    def __check_file_initialization(self):
        path_json = os.path.join(os.environ['DIR.WORKING'], 'file with spaces in name.json')
        self.assertFalse(os.path.exists(path_json))
        _ = ConsecutiveSentEmailsCounter('file with spaces in name')
        print(path_json)
        self.assertTrue(os.path.exists(path_json))

    def __check_adding_new_key(self):
        id_node = '1'
        file = self.__load_default_file()
        self.assertFalse(id_node in file.keys())
        self.__COUNTER.create_or_update_node_entry('1')
        file = self.__load_default_file()
        self.assertTrue(id_node in file.keys())

    def __check_deleting_existing_key(self):
        id_node = '1'
        file = self.__load_default_file()
        self.assertTrue(id_node in file.keys())
        self.__COUNTER.delete_entry_tracking_for_node('1')
        file = self.__load_default_file()
        self.assertFalse(id_node in file.keys())

    def __check_adding_and_updating_key(self):
        file = self.__load_default_file()
        self.assertEqual(0, len(file))
        self.__COUNTER.create_or_update_node_entry('2')
        file = self.__load_default_file()
        self.assertEqual(1, len(file))
        self.__COUNTER.create_or_update_node_entry('2')
        file = self.__load_default_file()
        self.assertEqual(1, len(file))

    def __check_deleting_unknown_key(self):
        file = self.__load_default_file()
        self.assertEqual(1, len(file))
        self.__COUNTER.delete_entry_tracking_for_node('3')
        file = self.__load_default_file()
        self.assertEqual(1, len(file))

    def __check_for_reached_waiting_threshold(self):
        file = self.__load_default_file()
        file['2'] = self.__get_last_weeks_timestamp()
        self.__save_default_file(file)
        self.__COUNTER = ConsecutiveSentEmailsCounter(self.__DEFAULT_FILENAME)
        self.assertTrue(self.__COUNTER.is_waiting_threshold_reached_for_node('2'))

    def __check_for_not_reached_waiting_threshold(self):
        """
        WEEKS_NOTIFICATION_INTERVAL for id_node=3 in mapping.json is set to 2 weeks
        """
        file = self.__load_default_file()
        file['3'] = self.__get_last_weeks_timestamp()
        self.__save_default_file(file)
        self.__COUNTER = ConsecutiveSentEmailsCounter(self.__DEFAULT_FILENAME)
        self.assertFalse(self.__COUNTER.is_waiting_threshold_reached_for_node('3'))

    def __check_for_reached_waiting_threshold_of_unknown_key(self):
        self.__COUNTER = ConsecutiveSentEmailsCounter(self.__DEFAULT_FILENAME)
        self.assertTrue(self.__COUNTER.is_waiting_threshold_reached_for_node('99'))

    def __load_default_file(self) -> dict:
        with open(self.__DEFAULT_FILEPATH, 'r', encoding='utf-8') as file:
            return json.load(file)

    def __save_default_file(self, d: dict):
        with open(self.__DEFAULT_FILEPATH, 'w', encoding='utf-8') as file:
            json.dump(d, file)

    @staticmethod
    def __get_last_weeks_timestamp():
        tz = timezone('Europe/Berlin')
        date = datetime.now(tz)
        date = date - timedelta(days=8, hours=0)
        return str(date)


if __name__ == '__main__':
    unittest.main()
