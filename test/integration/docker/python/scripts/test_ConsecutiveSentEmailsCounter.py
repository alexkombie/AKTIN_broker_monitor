import os
import unittest

from common import PropertiesReader
from email_service import ConsecutiveSentEmailsCounter


class TestConsecutiveSentEmailsCounter(unittest.TestCase):
    __FILENAME: str = 'tracking_sent_mails.json'

    @classmethod
    def setUpClass(cls):
        PropertiesReader().load_properties_as_env_vars('settings.json')

    def test_main(self):
        self.__check_file_initialization()
        self.__check_adding_new_key()
        self.__check_deleting_existing_key()
        self.__check_adding_and_updating_key()
        self.__check_deleting_unknown_key()

    def __check_file_initialization(self):
        path_json = os.path.join(os.environ['ROOT_DIR'], self.__FILENAME)
        self.assertFalse(os.path.exists(path_json))
        _ = ConsecutiveSentEmailsCounter()
        self.assertTrue(os.path.exists(path_json))

    def __check_adding_new_key(self):
        counter = ConsecutiveSentEmailsCounter()
        self.assertFalse(counter.does_entry_exist_for_node('1'))
        counter.create_or_update_node_entry('1')
        self.assertTrue(counter.does_entry_exist_for_node('1'))

    def __check_deleting_existing_key(self):
        counter = ConsecutiveSentEmailsCounter()
        self.assertTrue(counter.does_entry_exist_for_node('1'))
        counter.delete_entry_for_node_if_exists('1')
        self.assertFalse(counter.does_entry_exist_for_node('1'))

    @staticmethod
    def __check_adding_and_updating_key():
        counter = ConsecutiveSentEmailsCounter()
        counter.create_or_update_node_entry('2')
        counter.create_or_update_node_entry('2')

    @staticmethod
    def __check_deleting_unknown_key():
        counter = ConsecutiveSentEmailsCounter()
        counter.delete_entry_for_node_if_exists('3')


if __name__ == '__main__':
    unittest.main()
