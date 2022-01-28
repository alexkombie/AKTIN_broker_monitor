import os
import unittest
import pandas as pd
from node_to_csv import NodeErrorFetcher
from node_to_csv import load_properties_file_as_environment
from node_to_csv import CSVHandler

from BrokerNodeDummy import BrokerNodeDummy
from BrokerNodeDummy import BrokerNodeError


class TestNodeErrorFetcher(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        load_properties_file_as_environment('settings.json')
        BrokerNodeDummy('xxxApiKey123').reset_stats_on_broker()
        BrokerNodeDummy('xxxApiKey567').reset_stats_on_broker()
        BrokerNodeDummy('xxxApiKey890').reset_stats_on_broker()
        cls.__DIR_WORKING = os.environ['ROOT_DIR']
        cls.__DEFAULT_API_KEY = 'xxxApiKey123'
        cls.__DEFAULT_NODE_ID = '2'

    def tearDown(self):
        [os.remove(name) for name in os.listdir(os.getcwd()) if '.csv' in name]
        BrokerNodeDummy(self.__DEFAULT_API_KEY).reset_stats_on_broker()

    def test_init_working_csv(self):
        self.__init_csv_and_check_count('0', 1)
        self.__init_csv_and_check_count('0', 1)
        self.__init_csv_and_check_count('1', 2)
        self.__init_csv_and_check_name('2')

    def __init_csv_and_check_count(self, id_node: str, count: int):
        self.__init_new_fetcher_and_fetch_to_csv(id_node)
        count_csv = len(self.__get_csv_in_working_directory())
        self.assertEqual(count, count_csv)

    def __get_csv_in_working_directory(self) -> list[str]:
        """
        Case differentation as os.listdir() tries to read from ROOT instead of current folder if input is empty/('')
        """
        dir_working = self.__DIR_WORKING if self.__DIR_WORKING else os.getcwd()
        return [name for name in os.listdir(dir_working) if '.csv' in name]

    def __init_csv_and_check_name(self, id_node: str):
        name_expected = ''.join(['00', id_node, '_errors_', str(pd.Timestamp.now().year), '.csv'])
        list_csv_in_folder = self.__get_csv_in_working_directory()
        self.assertFalse(name_expected in list_csv_in_folder)
        self.__init_new_fetcher_and_fetch_to_csv(id_node)
        list_csv_in_folder = self.__get_csv_in_working_directory()
        self.assertTrue(name_expected in list_csv_in_folder)

    def test_working_csv_columns(self):
        path_csv = self.__init_new_fetcher_and_fetch_to_csv(self.__DEFAULT_NODE_ID)
        df = self.__load_csv_as_dataframe(path_csv)
        header = list(df.columns)
        expected_columns = ['timestamp', 'repeats', 'content']
        self.assertTrue(len(expected_columns), len(header))
        for column in expected_columns:
            self.assertTrue(column in header)

    def test_fetch_default_error_to_csv(self):
        error = self.__create_error1()
        self.__init_new_dummy_and_put_payload_on_node(self.__DEFAULT_API_KEY, error)
        path_csv = self.__init_new_fetcher_and_fetch_to_csv(self.__DEFAULT_NODE_ID)
        df = self.__load_csv_as_dataframe(path_csv)
        self.assertEqual(1, df.shape[0])
        ts_expected = ''.join([str(pd.Timestamp.now().year), '-01-01 00:00:00'])
        self.__check_error_row_from_csv(df.iloc[0], ts_expected, '5', 'TestError')

    def test_update_error_in_csv(self):
        """
        Expected timestamp is a hour later because timezone of script is Europe/Berlin (summer time)
        """
        error = self.__create_error1()
        self.__init_new_dummy_and_put_payload_on_node(self.__DEFAULT_API_KEY, error)
        error = self.__create_error2()
        self.__init_new_dummy_and_put_payload_on_node(self.__DEFAULT_API_KEY, error)
        path_csv = self.__init_new_fetcher_and_fetch_to_csv(self.__DEFAULT_NODE_ID)
        df = self.__load_csv_as_dataframe(path_csv)
        self.assertEqual(1, df.shape[0])
        ts_expected = ''.join([str(pd.Timestamp.now().year), '-10-10 01:00:00'])
        self.__check_error_row_from_csv(df.iloc[0], ts_expected, '10', 'TestError')

    def test_update_same_error_in_csv(self):
        error = self.__create_error1()
        self.__init_new_dummy_and_put_payload_on_node(self.__DEFAULT_API_KEY, error)
        error = self.__create_error1()
        self.__init_new_dummy_and_put_payload_on_node(self.__DEFAULT_API_KEY, error)
        path_csv = self.__init_new_fetcher_and_fetch_to_csv(self.__DEFAULT_NODE_ID)
        df = self.__load_csv_as_dataframe(path_csv)
        self.assertEqual(1, df.shape[0])
        ts_expected = ''.join([str(pd.Timestamp.now().year), '-01-01 00:00:00'])
        self.__check_error_row_from_csv(df.iloc[0], ts_expected, '5', 'TestError')

    def test_fetch_error_from_last_year_to_csv(self):
        error = self.__create_error_last_year()
        self.__init_new_dummy_and_put_payload_on_node(self.__DEFAULT_API_KEY, error)
        path_csv = self.__init_new_fetcher_and_fetch_to_csv(self.__DEFAULT_NODE_ID)
        df = self.__load_csv_as_dataframe(path_csv)
        self.assertEqual(0, df.shape[0])

    def __check_error_row_from_csv(self, row: pd.Series, timestamp: str, repeats: str, content: str):
        self.assertEqual(timestamp, row['timestamp'])
        self.assertEqual(repeats, row['repeats'])
        self.assertEqual(content, row['content'])

    @staticmethod
    def __create_error1():
        ts_error = ''.join([str(pd.Timestamp.now().year), '-01-01T00:00:00+01:00'])
        return BrokerNodeError(ts_error, '5', 'TestError')

    @staticmethod
    def __create_error2():
        ts_error = ''.join([str(pd.Timestamp.now().year), '-10-10T00:00:00+01:00'])
        return BrokerNodeError(ts_error, '10', 'TestError')

    @staticmethod
    def __create_error_last_year():
        ts_error = ''.join([str(pd.Timestamp.now().year - 1), '-01-01T00:00:00+01:00'])
        return BrokerNodeError(ts_error, '1', 'TestError')

    @staticmethod
    def __load_csv_as_dataframe(path_csv: str) -> pd.DataFrame:
        handler = CSVHandler(path_csv)
        return handler.read_csv_as_df()

    @staticmethod
    def __init_new_dummy_and_put_payload_on_node(api_key: str, payload):
        dummy = BrokerNodeDummy(api_key)
        dummy.put_stats_object_on_broker(payload)

    def __init_new_fetcher_and_fetch_to_csv(self, id_node: str) -> str:
        fetcher = NodeErrorFetcher(id_node, self.__DIR_WORKING)
        path_csv = fetcher.init_working_csv()
        fetcher.fetch_to_csv()
        return path_csv


if __name__ == '__main__':
    unittest.main()
