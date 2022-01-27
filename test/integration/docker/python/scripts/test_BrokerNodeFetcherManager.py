import unittest
import os
from node_to_csv import BrokerNodeFetcherManager
from node_to_csv import load_properties_file_as_environment


class TestBrokerNodeFetcherManager(unittest.TestCase):
    BROKER_NODE_FETCHER_MANAGER: BrokerNodeFetcherManager

    def setUp(self):
        load_properties_file_as_environment('settings.json')
        self.BROKER_NODE_FETCHER_MANAGER = BrokerNodeFetcherManager()

    def tearDown(self) -> None:
        if os.path.isdir(os.path.join(os.getcwd(), 'test1')):
            os.removedirs('test1')
        if os.path.isdir(os.path.join(os.getcwd(), 'test2')):
            os.removedirs('test2')

    def test_init_working_dir_for_node(self):
        self.__create_folder_and_check_count('test1', 1)
        self.__create_folder_and_check_count('test1', 1)
        self.__create_folder_and_check_count('test2', 2)

    def __create_folder_and_check_count(self, name_folder: str, count: int):
        path_folder = self.BROKER_NODE_FETCHER_MANAGER.init_working_dir_for_node(name_folder)
        count_folders = self.__count_folder_in_current_directory()
        self.assertEqual(count, count_folders)
        self.assertEqual(name_folder, path_folder)

    @staticmethod
    def __count_folder_in_current_directory():
        """
        Excludes folder __pycache__
        """
        list_name_folders = [name for name in os.listdir(os.getcwd()) if os.path.isdir(os.path.join(os.getcwd(), name))]
        return len(list_name_folders) - 1

    if __name__ == '__main__':
        unittest.main()
