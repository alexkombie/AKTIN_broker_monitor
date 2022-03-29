import os
import unittest
import pandas as pd
from node_to_csv import NodeInfoFetcher
from common import load_properties_file_as_environment
from common import InfoCSVHandler

from BrokerNodeDummy import BrokerNodeDummy
from BrokerNodeDummy import BrokerNodeImports


class TestNodeInfoFetcher(unittest.TestCase):
    __DEFAULT_NODE_ID = '0'
    __DEFAULT_API_KEY = 'xxxApiKey123'
    __DIR_WORKING = None
    __CSV_HANDLER = InfoCSVHandler()

    @classmethod
    def setUpClass(cls):
        load_properties_file_as_environment('settings.json')
        cls.__DIR_WORKING = os.environ['ROOT_DIR'] if os.environ['ROOT_DIR'] else os.getcwd()
        name_csv = cls.__CSV_HANDLER.generate_csv_name(cls.__DEFAULT_NODE_ID)
        cls.__DEFAULT_CSV_PATH = os.path.join(cls.__DIR_WORKING, name_csv)
        cls.__CURRENT_YMD = pd.Timestamp.now().tz_localize('Europe/Berlin').strftime("%Y-%m-%d")
        cls.__FETCHER = NodeInfoFetcher(cls.__DEFAULT_NODE_ID, cls.__DIR_WORKING)
        cls.__DUMMY = BrokerNodeDummy(cls.__DEFAULT_API_KEY)

    def setUp(self):
        stats = self.__create_stats1()
        self.__DUMMY.put_import_info_on_broker(stats)
        self.__FETCHER.fetch_to_csv()

    def tearDown(self):
        [os.remove(name) for name in os.listdir(self.__DIR_WORKING) if '.csv' in name]

    @staticmethod
    def __create_stats1():
        return BrokerNodeImports('2020-01-01T00:00:00+01:00',
                                 '',
                                 '',
                                 '0',
                                 '0',
                                 '0',
                                 '0')

    @staticmethod
    def __create_stats2():
        return BrokerNodeImports('2020-01-01T00:00:00+01:00',
                                 '2020-03-03T00:00:00+01:00',
                                 '2020-03-03T00:00:00+01:00',
                                 '2000',
                                 '400',
                                 '250',
                                 '350')

    @staticmethod
    def __create_stats2_dwh_restart():
        return BrokerNodeImports('2020-01-01T12:00:00+01:00',
                                 '2020-03-03T00:00:00+01:00',
                                 '2020-03-03T00:00:00+01:00',
                                 '2000',
                                 '400',
                                 '250',
                                 '350')

    def __check_date_stats_in_csv_row(self, row: pd.Series, date: str, start: str, last_write: str, last_reject: str):
        self.assertEqual(date, row['date'])
        self.assertEqual(start, row['last_start'])
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

    def __put_improt_stats_on_broker_and_get_fetched_csv_as_df(self, payload):
        self.__DUMMY.put_import_info_on_broker(payload)
        self.__FETCHER.fetch_to_csv()
        return self.__CSV_HANDLER.read_csv_as_df(self.__DEFAULT_CSV_PATH)

    def __change_date_of_current_csv_to_past_days(self, days: int):
        df = self.__CSV_HANDLER.read_csv_as_df(self.__DEFAULT_CSV_PATH)
        ts_current = pd.Timestamp.now().tz_localize('Europe/Berlin')
        ts_past = ts_current - pd.Timedelta(days=days)
        date_past = ts_past.strftime("%Y-%m-%d")
        df.iloc[0]['date'] = date_past
        self.__CSV_HANDLER.save_df_to_csv(df, self.__DEFAULT_CSV_PATH)

    @staticmethod
    def __rename_csv_to_last_years_csv(path_csv: str):
        last_year = str(pd.Timestamp.now().year - 1)
        path_csv_new = path_csv[:-8] + last_year + path_csv[-4:]
        os.rename(path_csv, path_csv_new)

    def test_init_working_csv(self):
        self.__init_new_dummy_and_put_default_stats_on_node('xxxApiKey123')
        self.__init_new_dummy_and_put_default_stats_on_node('xxxApiKey567')
        self.__init_new_fetcher_and_check_csv_count('0', 1)
        self.__init_new_fetcher_and_check_csv_count('0', 1)
        self.__init_new_fetcher_and_check_csv_count('1', 2)

    def __init_new_dummy_and_put_default_stats_on_node(self, api_key: str):
        stats = self.__create_stats1()
        dummy = BrokerNodeDummy(api_key)
        dummy.put_import_info_on_broker(stats)

    def __init_new_fetcher_and_check_csv_count(self, id_node: str, count: int):
        fetcher = NodeInfoFetcher(id_node, self.__DIR_WORKING)
        fetcher.fetch_to_csv()
        count_csv = len(self.__list_csv_in_working_directory())
        self.assertEqual(count, count_csv)

    def __list_csv_in_working_directory(self) -> list:
        return [name for name in os.listdir(self.__DIR_WORKING) if '.csv' in name]

    def test_csv_columns(self):
        df = self.__CSV_HANDLER.read_csv_as_df(self.__DEFAULT_CSV_PATH)
        header = list(df.columns)
        expected_columns = ['date', 'last_contact', 'last_start', 'last_write', 'last_reject', 'imported',
                            'updated', 'invalid', 'failed', 'error_rate', 'daily_imported',
                            'daily_updated', 'daily_invalid', 'daily_failed', 'daily_error_rate']
        self.assertTrue(len(expected_columns), len(header))
        self.assertCountEqual(expected_columns, header)

    def test_fetch_default_stats_to_csv(self):
        df = self.__CSV_HANDLER.read_csv_as_df(self.__DEFAULT_CSV_PATH)
        self.assertEqual(1, df.shape[0])
        self.__check_date_stats_in_csv_row(df.iloc[0], self.__CURRENT_YMD, '2020-01-01 00:00:00', '-', '-')
        self.__check_global_import_stats_in_csv_row(df.iloc[0], '0', '0', '0', '0', '-')
        self.__check_daily_import_stats_in_csv_row(df.iloc[0], '-', '-', '-', '-', '-')

    def test_update_default_stats_in_csv(self):
        stats2 = self.__create_stats2()
        df = self.__put_improt_stats_on_broker_and_get_fetched_csv_as_df(stats2)
        self.assertEqual(1, df.shape[0])
        self.__check_date_stats_in_csv_row(df.iloc[0], self.__CURRENT_YMD, '2020-01-01 00:00:00', '2020-03-03 00:00:00', '2020-03-03 00:00:00')
        self.__check_global_import_stats_in_csv_row(df.iloc[0], '2000', '400', '250', '350', '20.0')
        self.__check_daily_import_stats_in_csv_row(df.iloc[0], '-', '-', '-', '-', '-')

    def test_fetch_next_stats_in_csv(self):
        self.__change_date_of_current_csv_to_past_days(1)
        stats2 = self.__create_stats2()
        df = self.__put_improt_stats_on_broker_and_get_fetched_csv_as_df(stats2)
        self.assertEqual(2, df.shape[0])
        self.__check_date_stats_in_csv_row(df.iloc[1], self.__CURRENT_YMD, '2020-01-01 00:00:00', '2020-03-03 00:00:00', '2020-03-03 00:00:00')
        self.__check_global_import_stats_in_csv_row(df.iloc[1], '2000', '400', '250', '350', '20.0')
        self.__check_daily_import_stats_in_csv_row(df.iloc[1], '2000', '400', '250', '350', '20.0')

    def test_fetch_next_stats_in_csv_timegap(self):
        self.__change_date_of_current_csv_to_past_days(5)
        stats2 = self.__create_stats2()
        df = self.__put_improt_stats_on_broker_and_get_fetched_csv_as_df(stats2)
        self.assertEqual(2, df.shape[0])
        self.__check_date_stats_in_csv_row(df.iloc[1], self.__CURRENT_YMD, '2020-01-01 00:00:00', '2020-03-03 00:00:00', '2020-03-03 00:00:00')
        self.__check_global_import_stats_in_csv_row(df.iloc[1], '2000', '400', '250', '350', '20.0')
        self.__check_daily_import_stats_in_csv_row(df.iloc[1], '-', '-', '-', '-', '-')

    def test_fetch_next_stats_in_csv_with_dwh_restart(self):
        self.__change_date_of_current_csv_to_past_days(1)
        stats2 = self.__create_stats2_dwh_restart()
        df = self.__put_improt_stats_on_broker_and_get_fetched_csv_as_df(stats2)
        self.assertEqual(2, df.shape[0])
        self.__check_date_stats_in_csv_row(df.iloc[1], self.__CURRENT_YMD, '2020-01-01 12:00:00', '2020-03-03 00:00:00', '2020-03-03 00:00:00')
        self.__check_global_import_stats_in_csv_row(df.iloc[1], '2000', '400', '250', '350', '20.0')
        self.__check_daily_import_stats_in_csv_row(df.iloc[1], '-', '-', '-', '-', '-')

    def test_fetch_new_stats_in_csv_after_year_change(self):
        self.__change_date_of_current_csv_to_past_days(1)
        self.__rename_csv_to_last_years_csv(self.__DEFAULT_CSV_PATH)
        stats2 = self.__create_stats2()
        df = self.__put_improt_stats_on_broker_and_get_fetched_csv_as_df(stats2)
        self.assertEqual(1, df.shape[0])
        self.__check_date_stats_in_csv_row(df.iloc[0], self.__CURRENT_YMD, '2020-01-01 00:00:00', '2020-03-03 00:00:00', '2020-03-03 00:00:00')
        self.__check_global_import_stats_in_csv_row(df.iloc[0], '2000', '400', '250', '350', '20.0')
        self.__check_daily_import_stats_in_csv_row(df.iloc[0], '2000', '400', '250', '350', '20.0')

    def test_fetch_new_stats_in_csv_after_year_change_timegap(self):
        self.__change_date_of_current_csv_to_past_days(5)
        self.__rename_csv_to_last_years_csv(self.__DEFAULT_CSV_PATH)
        stats2 = self.__create_stats2()
        df = self.__put_improt_stats_on_broker_and_get_fetched_csv_as_df(stats2)
        self.assertEqual(1, df.shape[0])
        self.__check_date_stats_in_csv_row(df.iloc[0], self.__CURRENT_YMD, '2020-01-01 00:00:00', '2020-03-03 00:00:00', '2020-03-03 00:00:00')
        self.__check_global_import_stats_in_csv_row(df.iloc[0], '2000', '400', '250', '350', '20.0')
        self.__check_daily_import_stats_in_csv_row(df.iloc[0], '-', '-', '-', '-', '-')


if __name__ == '__main__':
    unittest.main()
