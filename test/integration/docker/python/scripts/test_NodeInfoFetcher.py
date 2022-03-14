import os
import unittest
import pandas as pd
from node_to_csv import NodeInfoFetcher
from common import load_properties_file_as_environment
from common import CSVHandler

from BrokerNodeDummy import BrokerNodeDummy
from BrokerNodeDummy import BrokerImportStats


class TestNodeInfoFetcher(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        load_properties_file_as_environment('settings.json')
        BrokerNodeDummy('xxxApiKey123').reset_stats_on_broker()
        BrokerNodeDummy('xxxApiKey567').reset_stats_on_broker()
        BrokerNodeDummy('xxxApiKey890').reset_stats_on_broker()
        cls.__DIR_WORKING = os.environ['ROOT_DIR']
        cls.__DEFAULT_API_KEY = 'xxxApiKey123'
        cls.__DEFAULT_NODE_ID = '0'

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
        name_expected = ''.join(['00', id_node, '_stats_', str(pd.Timestamp.now().year), '.csv'])
        list_csv_in_folder = self.__get_csv_in_working_directory()
        self.assertFalse(name_expected in list_csv_in_folder)
        self.__init_new_fetcher_and_fetch_to_csv(id_node)
        list_csv_in_folder = self.__get_csv_in_working_directory()
        self.assertTrue(name_expected in list_csv_in_folder)

    def test_working_csv_columns(self):
        path_csv = self.__init_new_fetcher_and_fetch_to_csv(self.__DEFAULT_NODE_ID)
        df = self.__load_csv_as_dataframe(path_csv)
        header = list(df.columns)
        expected_columns = ['date', 'last_contact', 'start', 'last_write', 'last_reject', 'imported',
                            'updated', 'invalid', 'failed', 'error_rate', 'daily_imported',
                            'daily_updated', 'daily_invalid', 'daily_failed', 'daily_error_rate']
        self.assertTrue(len(expected_columns), len(header))
        for column in expected_columns:
            self.assertTrue(column in header)

    def test_fetch_default_stats_to_csv(self):
        path_csv = self.__init_new_fetcher_and_fetch_to_csv(self.__DEFAULT_NODE_ID)
        df = self.__load_csv_as_dataframe(path_csv)
        self.assertEqual(1, df.shape[0])
        self.__check_date_stats_in_csv_row(df.iloc[0], self.__get_current_date_YMD(), '2020-01-01 00:00:00', '-', '-')
        self.__check_global_import_stats_in_csv_row(df.iloc[0], '0', '0', '0', '0', '-')
        self.__check_daily_import_stats_in_csv_row(df.iloc[0], '-', '-', '-', '-', '-')

    def test_update_default_stats_in_csv(self):
        path_csv = self.__init_new_fetcher_and_fetch_to_csv(self.__DEFAULT_NODE_ID)
        df = self.__load_csv_as_dataframe(path_csv)
        self.assertEqual(1, df.shape[0])
        stats = self.__create_stats1()
        self.__init_new_dummy_and_put_payload_on_node(self.__DEFAULT_API_KEY, stats)
        path_csv = self.__init_new_fetcher_and_fetch_to_csv(self.__DEFAULT_NODE_ID)
        df = self.__load_csv_as_dataframe(path_csv)
        self.assertEqual(1, df.shape[0])
        self.__check_date_stats_in_csv_row(df.iloc[0], self.__get_current_date_YMD(), '2020-01-01 00:00:00', '2020-02-02 00:00:00', '2020-03-03 00:00:00')
        self.__check_global_import_stats_in_csv_row(df.iloc[0], '500', '400', '250', '200', '33.33')
        self.__check_daily_import_stats_in_csv_row(df.iloc[0], '-', '-', '-', '-', '-')

    def test_fetch_next_stats_in_csv(self):
        stats1 = self.__create_stats1()
        self.__init_new_dummy_and_put_payload_on_node(self.__DEFAULT_API_KEY, stats1)
        path_csv = self.__init_new_fetcher_and_fetch_to_csv(self.__DEFAULT_NODE_ID)
        df = self.__load_csv_as_dataframe(path_csv)
        date_yesterday = self.__get_shifted_date_YMD(1)
        df = self.__change_date_in_first_row_of_df(df, date_yesterday)
        self.__save_df_to_path(df, path_csv)
        self.assertEqual(1, df.shape[0])
        stats2 = self.__create_stats2()
        self.__init_new_dummy_and_put_payload_on_node(self.__DEFAULT_API_KEY, stats2)
        path_csv = self.__init_new_fetcher_and_fetch_to_csv(self.__DEFAULT_NODE_ID)
        df = self.__load_csv_as_dataframe(path_csv)
        self.assertEqual(2, df.shape[0])
        self.__check_date_stats_in_csv_row(df.iloc[1], self.__get_current_date_YMD(), '2020-01-01 00:00:00', '2020-03-03 00:00:00', '2020-03-03 00:00:00')
        self.__check_global_import_stats_in_csv_row(df.iloc[1], '2000', '400', '250', '350', '20.0')
        self.__check_daily_import_stats_in_csv_row(df.iloc[1], '1500', '0', '0', '150', '9.09')

    def test_fetch_next_stats_in_csv_timegap(self):
        stats1 = self.__create_stats1()
        self.__init_new_dummy_and_put_payload_on_node(self.__DEFAULT_API_KEY, stats1)
        path_csv = self.__init_new_fetcher_and_fetch_to_csv(self.__DEFAULT_NODE_ID)
        df = self.__load_csv_as_dataframe(path_csv)
        date_yesterday = self.__get_shifted_date_YMD(5)
        df = self.__change_date_in_first_row_of_df(df, date_yesterday)
        self.__save_df_to_path(df, path_csv)
        self.assertEqual(1, df.shape[0])
        stats2 = self.__create_stats2()
        self.__init_new_dummy_and_put_payload_on_node(self.__DEFAULT_API_KEY, stats2)
        path_csv = self.__init_new_fetcher_and_fetch_to_csv(self.__DEFAULT_NODE_ID)
        df = self.__load_csv_as_dataframe(path_csv)
        self.assertEqual(2, df.shape[0])
        self.__check_date_stats_in_csv_row(df.iloc[1], self.__get_current_date_YMD(), '2020-01-01 00:00:00', '2020-03-03 00:00:00', '2020-03-03 00:00:00')
        self.__check_global_import_stats_in_csv_row(df.iloc[1], '2000', '400', '250', '350', '20.0')
        self.__check_daily_import_stats_in_csv_row(df.iloc[1], '-', '-', '-', '-', '-')

    def test_fetch_next_stats_in_csv_with_dwh_restart(self):
        stats1 = self.__create_stats1()
        self.__init_new_dummy_and_put_payload_on_node(self.__DEFAULT_API_KEY, stats1)
        path_csv = self.__init_new_fetcher_and_fetch_to_csv(self.__DEFAULT_NODE_ID)
        df = self.__load_csv_as_dataframe(path_csv)
        date_yesterday = self.__get_shifted_date_YMD(1)
        df = self.__change_date_in_first_row_of_df(df, date_yesterday)
        self.__save_df_to_path(df, path_csv)
        self.assertEqual(1, df.shape[0])
        stats2 = self.__create_stats2_dwh_restart()
        self.__init_new_dummy_and_put_payload_on_node(self.__DEFAULT_API_KEY, stats2)
        path_csv = self.__init_new_fetcher_and_fetch_to_csv(self.__DEFAULT_NODE_ID)
        df = self.__load_csv_as_dataframe(path_csv)
        self.assertEqual(2, df.shape[0])
        self.__check_date_stats_in_csv_row(df.iloc[1], self.__get_current_date_YMD(), '2020-01-01 12:00:00', '2020-03-03 00:00:00', '2020-03-03 00:00:00')
        self.__check_global_import_stats_in_csv_row(df.iloc[1], '2000', '400', '250', '350', '20.0')
        self.__check_daily_import_stats_in_csv_row(df.iloc[1], '-', '-', '-', '-', '-')

    def test_fetch_new_stats_in_csv_after_year_change(self):
        stats1 = self.__create_stats1()
        self.__init_new_dummy_and_put_payload_on_node(self.__DEFAULT_API_KEY, stats1)
        path_csv = self.__init_new_fetcher_and_fetch_to_csv(self.__DEFAULT_NODE_ID)
        df = self.__load_csv_as_dataframe(path_csv)
        date_yesterday = self.__get_shifted_date_YMD(1)
        df = self.__change_date_in_first_row_of_df(df, date_yesterday)
        self.__save_df_to_path(df, path_csv)
        self.__rename_csv_to_last_years_csv(path_csv)
        stats2 = self.__create_stats2()
        self.__init_new_dummy_and_put_payload_on_node(self.__DEFAULT_API_KEY, stats2)
        path_csv = self.__init_new_fetcher_and_fetch_to_csv(self.__DEFAULT_NODE_ID)
        df = self.__load_csv_as_dataframe(path_csv)
        self.assertEqual(1, df.shape[0])
        self.__check_date_stats_in_csv_row(df.iloc[0], self.__get_current_date_YMD(), '2020-01-01 00:00:00', '2020-03-03 00:00:00', '2020-03-03 00:00:00')
        self.__check_global_import_stats_in_csv_row(df.iloc[0], '2000', '400', '250', '350', '20.0')
        self.__check_daily_import_stats_in_csv_row(df.iloc[0], '1500', '0', '0', '150', '9.09')

    def test_fetch_new_stats_in_csv_after_year_change_timegap(self):
        stats1 = self.__create_stats1()
        self.__init_new_dummy_and_put_payload_on_node(self.__DEFAULT_API_KEY, stats1)
        path_csv = self.__init_new_fetcher_and_fetch_to_csv(self.__DEFAULT_NODE_ID)
        df = self.__load_csv_as_dataframe(path_csv)
        date_yesterday = self.__get_shifted_date_YMD(5)
        df = self.__change_date_in_first_row_of_df(df, date_yesterday)
        self.__save_df_to_path(df, path_csv)
        self.__rename_csv_to_last_years_csv(path_csv)
        stats2 = self.__create_stats2()
        self.__init_new_dummy_and_put_payload_on_node(self.__DEFAULT_API_KEY, stats2)
        path_csv = self.__init_new_fetcher_and_fetch_to_csv(self.__DEFAULT_NODE_ID)
        df = self.__load_csv_as_dataframe(path_csv)
        self.assertEqual(1, df.shape[0])
        self.__check_date_stats_in_csv_row(df.iloc[0], self.__get_current_date_YMD(), '2020-01-01 00:00:00', '2020-03-03 00:00:00', '2020-03-03 00:00:00')
        self.__check_global_import_stats_in_csv_row(df.iloc[0], '2000', '400', '250', '350', '20.0')
        self.__check_daily_import_stats_in_csv_row(df.iloc[0], '-', '-', '-', '-', '-')

    def __check_date_stats_in_csv_row(self, row: pd.Series, date: str, start: str, last_write: str, last_reject: str):
        self.assertEqual(date, row['date'])
        self.assertEqual(start, row['start'])
        self.assertEqual(last_write, row['last_write'])
        self.assertEqual(last_reject, row['last_reject'])

    def __check_global_import_stats_in_csv_row(self, row: pd.Series, imported: str, updated: str, invalid: str, failed: str, error_rate: str):
        self.assertEqual(imported, row['imported'])
        self.assertEqual(updated, row['updated'])
        self.assertEqual(invalid, row['invalid'])
        self.assertEqual(failed, row['failed'])
        self.assertEqual(error_rate, row['error_rate'])

    def __check_daily_import_stats_in_csv_row(self, row: pd.Series, imported: str, updated: str, invalid: str, failed: str, error_rate: str):
        self.assertEqual(imported, row['daily_imported'])
        self.assertEqual(updated, row['daily_updated'])
        self.assertEqual(invalid, row['daily_invalid'])
        self.assertEqual(failed, row['daily_failed'])
        self.assertEqual(error_rate, row['daily_error_rate'])

    @staticmethod
    def __create_stats1():
        return BrokerImportStats('2020-01-01T00:00:00+01:00',
                                 '2020-02-02T00:00:00+01:00',
                                 '2020-03-03T00:00:00+01:00',
                                 '500',
                                 '400',
                                 '250',
                                 '200')

    @staticmethod
    def __create_stats2():
        return BrokerImportStats('2020-01-01T00:00:00+01:00',
                                 '2020-03-03T00:00:00+01:00',
                                 '2020-03-03T00:00:00+01:00',
                                 '2000',
                                 '400',
                                 '250',
                                 '350')

    @staticmethod
    def __create_stats2_dwh_restart():
        return BrokerImportStats('2020-01-01T12:00:00+01:00',
                                 '2020-03-03T00:00:00+01:00',
                                 '2020-03-03T00:00:00+01:00',
                                 '2000',
                                 '400',
                                 '250',
                                 '350')

    @staticmethod
    def __get_current_date_YMD() -> str:
        ts = pd.Timestamp.now().tz_localize('Europe/Berlin')
        return ts.strftime("%Y-%m-%d")

    @staticmethod
    def __get_shifted_date_YMD(days: int) -> str:
        ts = pd.Timestamp.now().tz_localize('Europe/Berlin')
        ts = ts - pd.Timedelta(days=days)
        return ts.strftime("%Y-%m-%d")

    @staticmethod
    def __change_date_in_first_row_of_df(df: pd.DataFrame, date: str) -> pd.DataFrame:
        df.iloc[0]['date'] = date
        return df

    @staticmethod
    def __rename_csv_to_last_years_csv(path_csv: str) -> str:
        last_year = str(pd.Timestamp.now().year - 1)
        path_csv_new = path_csv[:-8] + last_year + path_csv[-4:]
        os.rename(path_csv, path_csv_new)

    @staticmethod
    def __load_csv_as_dataframe(path_csv: str) -> pd.DataFrame:
        handler = CSVHandler(path_csv)
        return handler.read_csv_as_df()

    @staticmethod
    def __save_df_to_path(df: pd.DataFrame, path_csv: str):
        handler = CSVHandler(path_csv)
        handler.save_df_as_csv(df)

    @staticmethod
    def __init_new_dummy_and_put_payload_on_node(api_key: str, payload):
        dummy = BrokerNodeDummy(api_key)
        dummy.put_stats_object_on_broker(payload)

    def __init_new_fetcher_and_fetch_to_csv(self, id_node: str) -> str:
        fetcher = NodeInfoFetcher(id_node, self.__DIR_WORKING)
        path_csv = fetcher.init_working_csv()
        fetcher.fetch_to_csv()
        return path_csv


if __name__ == '__main__':
    unittest.main()
