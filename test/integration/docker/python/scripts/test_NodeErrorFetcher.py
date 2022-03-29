import os
import unittest
import pandas as pd
from node_to_csv import NodeErrorFetcher
from common import load_properties_file_as_environment
from common import ErrorCSVHandler

from BrokerNodeDummy import BrokerNodeDummy
from BrokerNodeDummy import BrokerNodeError


class TestNodeErrorFetcher(unittest.TestCase):
    __DEFAULT_NODE_ID = '0'
    __DEFAULT_API_KEY = 'xxxApiKey123'
    __DIR_WORKING = None
    __CSV_HANDLER = ErrorCSVHandler()

    @classmethod
    def setUpClass(cls):
        load_properties_file_as_environment('settings.json')
        cls.__DIR_WORKING = os.environ['ROOT_DIR'] if os.environ['ROOT_DIR'] else os.getcwd()
        name_csv = cls.__CSV_HANDLER.generate_csv_name(cls.__DEFAULT_NODE_ID)
        cls.__DEFAULT_CSV_PATH = os.path.join(cls.__DIR_WORKING, name_csv)
        cls.__FETCHER = NodeErrorFetcher(cls.__DEFAULT_NODE_ID, cls.__DIR_WORKING)
        cls.__DUMMY = BrokerNodeDummy(cls.__DEFAULT_API_KEY)

    def setUp(self):
        stats = self.__create_error1()
        self.__DUMMY.put_import_info_on_broker(stats)
        self.__FETCHER.fetch_to_csv()

    def tearDown(self):
        [os.remove(name) for name in os.listdir(os.getcwd()) if '.csv' in name]

    @staticmethod
    def __create_error1():
        ts_error = ''.join([str(pd.Timestamp.now().year), '-01-01T00:00:00+01:00'])
        return BrokerNodeError(ts_error, '5', 'TestError')

    @staticmethod
    def __create_error1_update():
        ts_error = ''.join([str(pd.Timestamp.now().year), '-10-10T00:00:00+01:00'])
        return BrokerNodeError(ts_error, '10', 'TestError')

    @staticmethod
    def __create_error2():
        ts_error = ''.join([str(pd.Timestamp.now().year), '-05-05T00:00:00+01:00'])
        return BrokerNodeError(ts_error, '5', 'TestError2')

    @staticmethod
    def __create_error_last_year():
        ts_error = ''.join([str(pd.Timestamp.now().year - 1), '-01-01T00:00:00+01:00'])
        return BrokerNodeError(ts_error, '1', 'TestError')

    @staticmethod
    def __create_error_without_repeats():
        ts_error = ''.join([str(pd.Timestamp.now().year), '-01-01T00:00:00+01:00'])
        return BrokerNodeError(ts_error, '', 'TestError')

    def __check_error_row_from_csv(self, row: pd.Series, timestamp: str, repeats: str, content: str):
        self.assertEqual(timestamp, row['timestamp'])
        self.assertEqual(repeats, row['repeats'])
        self.assertEqual(content, row['content'])

    def __put_import_error_on_broker_and_get_fetched_csv_as_df(self, payload):
        self.__DUMMY.put_import_info_on_broker(payload)
        self.__FETCHER.fetch_to_csv()
        return self.__CSV_HANDLER.read_csv_as_df(self.__DEFAULT_CSV_PATH)

    def test_init_working_csv(self):
        self.__init_new_dummy_and_put_default_error_on_node('xxxApiKey123')
        self.__init_new_dummy_and_put_default_error_on_node('xxxApiKey567')
        self.__init_new_fetcher_and_check_csv_count('0', 1)
        self.__init_new_fetcher_and_check_csv_count('0', 1)
        self.__init_new_fetcher_and_check_csv_count('1', 2)

    def __init_new_dummy_and_put_default_error_on_node(self, api_key: str):
        stats = self.__create_error1()
        dummy = BrokerNodeDummy(api_key)
        dummy.put_import_info_on_broker(stats)

    def __init_new_fetcher_and_check_csv_count(self, id_node: str, count: int):
        fetcher = NodeErrorFetcher(id_node, self.__DIR_WORKING)
        fetcher.fetch_to_csv()
        count_csv = len(self.__list_csv_in_working_directory())
        self.assertEqual(count, count_csv)

    def __list_csv_in_working_directory(self) -> list:
        return [name for name in os.listdir(self.__DIR_WORKING) if '.csv' in name]

    def test_csv_columns(self):
        df = self.__CSV_HANDLER.read_csv_as_df(self.__DEFAULT_CSV_PATH)
        header = list(df.columns)
        expected_columns = ['timestamp', 'repeats', 'content']
        self.assertTrue(len(expected_columns), len(header))
        self.assertCountEqual(expected_columns, header)

    def test_fetch_default_error_to_csv(self):
        df = self.__CSV_HANDLER.read_csv_as_df(self.__DEFAULT_CSV_PATH)
        self.assertEqual(1, df.shape[0])
        ts_expected = ''.join([str(pd.Timestamp.now().year), '-01-01 00:00:00'])
        self.__check_error_row_from_csv(df.iloc[0], ts_expected, '5', 'TestError')

    def test_update_error_in_csv(self):
        """
        Expected timestamp is a hour later because timezone of script is Europe/Berlin (summer time)
        """
        error = self.__create_error1_update()
        df = self.__put_import_error_on_broker_and_get_fetched_csv_as_df(error)
        self.assertEqual(1, df.shape[0])
        ts_expected = ''.join([str(pd.Timestamp.now().year), '-10-10 01:00:00'])
        self.__check_error_row_from_csv(df.iloc[0], ts_expected, '10', 'TestError')

    def test_fetch_next_error_to_csv(self):
        error = self.__create_error2()
        df = self.__put_import_error_on_broker_and_get_fetched_csv_as_df(error)
        self.assertEqual(2, df.shape[0])
        ts_expected = ''.join([str(pd.Timestamp.now().year), '-05-05 01:00:00'])
        self.__check_error_row_from_csv(df.iloc[0], ts_expected, '5', 'TestError2')
        ts_expected2 = ''.join([str(pd.Timestamp.now().year), '-01-01 00:00:00'])
        self.__check_error_row_from_csv(df.iloc[1], ts_expected2, '5', 'TestError')

    def test_fetch_default_error_to_csv_with_missing_repeats(self):
        error = self.__create_error_without_repeats()
        df = self.__put_import_error_on_broker_and_get_fetched_csv_as_df(error)
        self.assertEqual(1, df.shape[0])
        ts_expected = ''.join([str(pd.Timestamp.now().year), '-01-01 00:00:00'])
        self.__check_error_row_from_csv(df.iloc[0], ts_expected, '1', 'TestError')

    def test_fetch_identical_error_to_csv(self):
        error = self.__create_error1()
        df = self.__put_import_error_on_broker_and_get_fetched_csv_as_df(error)
        self.assertEqual(1, df.shape[0])
        ts_expected = ''.join([str(pd.Timestamp.now().year), '-01-01 00:00:00'])
        self.__check_error_row_from_csv(df.iloc[0], ts_expected, '5', 'TestError')

    def test_fetch_error_from_last_year_to_csv(self):
        self.tearDown()
        error = self.__create_error_last_year()
        df = self.__put_import_error_on_broker_and_get_fetched_csv_as_df(error)
        self.assertEqual(0, df.shape[0])


if __name__ == '__main__':
    unittest.main()
