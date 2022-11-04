import os
import sys
import unittest
from pathlib import Path
from shutil import rmtree

this_path = Path(os.path.realpath(__file__))
path_src = os.path.join(this_path.parents[3], 'src')
sys.path.insert(0, path_src)

import bs4
import pandas as pd
from common import InfoCSVHandler, PropertiesReader
from csv_to_confluence import TemplatePageLoader, TemplatePageCSVInfoWriter


class TestTemplatePageCSVInfoWriter(unittest.TestCase):
    __DEFAULT_NODE_ID: str = '0'
    __DIR_ROOT: str = None
    __TEMPLATE: str = None

    @classmethod
    def setUpClass(cls):
        path_settings = os.path.join(this_path.parents[1], 'settings.json')
        PropertiesReader().load_properties_as_env_vars(path_settings)
        cls.__DIR_ROOT = os.environ['ROOT_DIR'] if os.environ['ROOT_DIR'] else os.getcwd()
        cls.__CSV_HANDLER = InfoCSVHandler()
        cls.__CSV_INFO_WRITER = TemplatePageCSVInfoWriter()
        name_csv = InfoCSVHandler().generate_csv_name(cls.__DEFAULT_NODE_ID)
        cls.__DEFAULT_CSV_PATH = os.path.join(cls.__DIR_ROOT, cls.__DEFAULT_NODE_ID, name_csv)

    def setUp(self):
        loader = TemplatePageLoader()
        self.__TEMPLATE = loader.get_template_page()
        dir_working = os.path.join(self.__DIR_ROOT, self.__DEFAULT_NODE_ID)
        if not os.path.exists(dir_working):
            os.makedirs(dir_working)

    def tearDown(self):
        rmtree(self.__DIR_ROOT)

    def test_write_template_from_one_row(self):
        self.__create_csv1()
        page = self.__CSV_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, self.__DEFAULT_NODE_ID)
        self.__check_template_page_dates(page, '2022-01-01 12:00:00', '2022-01-01 11:00:00', '2022-01-01 00:00:00', '-', '-')
        self.__check_template_page_total_imports(page, '0', '0', '0', '0', '0.0')
        self.__check_template_page_daily_imports(page, '-', '-', '-', '-', '-')

    def __create_csv1(self):
        df = pd.DataFrame(columns=self.__CSV_HANDLER.get_csv_columns())
        df.loc[len(df)] = ['2022-01-01 12:00:00', '2022-01-01 11:00:00', '2022-01-01 00:00:00', '-', '-', '0', '0', '0', '0', '0.0', '-', '-', '-', '-', '-']
        self.__CSV_HANDLER.save_df_to_csv(df, self.__DEFAULT_CSV_PATH)

    def test_write_template_from_multiple_rows(self):
        self.__create_csv2()
        page = self.__CSV_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, self.__DEFAULT_NODE_ID)
        self.__check_template_page_dates(page, '2022-02-02 12:00:00', '2022-02-02 11:00:00', '2022-02-02 00:00:00', '2022-02-02 10:00:00', '2022-02-02 10:00:00')
        self.__check_template_page_total_imports(page, '2000', '2000', '2000', '2000', '50.0')
        self.__check_template_page_daily_imports(page, '1000', '1000', '1000', '1000', '50.0')

    def __create_csv2(self):
        df = pd.DataFrame(columns=self.__CSV_HANDLER.get_csv_columns())
        df.loc[len(df)] = ['2022-01-01 12:00:00', '2022-01-01 11:00:00', '2022-01-01 00:00:00', '-', '-', '0', '0', '0', '0', '0.0', '-', '-', '-', '-', '-']
        df.loc[len(df)] = ['2022-02-02 12:00:00', '2022-02-02 11:00:00', '2022-02-02 00:00:00', '2022-02-02 10:00:00', '2022-02-02 10:00:00',
                           '2000', '2000', '2000', '2000', '50.0', '1000', '1000', '1000', '1000', '50.0']
        self.__CSV_HANDLER.save_df_to_csv(df, self.__DEFAULT_CSV_PATH)

    def test_write_template_from_row_with_null_values(self):
        self.__create_csv3()
        with self.assertRaises(AttributeError):
            _ = self.__CSV_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, self.__DEFAULT_NODE_ID)

    def __create_csv3(self):
        df = pd.DataFrame(columns=self.__CSV_HANDLER.get_csv_columns())
        df.loc[len(df)] = ['2022-01-01 12:00:00', '2022-01-01 11:00:00', '2022-01-01 00:00:00', None, '-', None, '0', '0', '0', '0.0', '-', '-', '-', '-', '-']
        self.__CSV_HANDLER.save_df_to_csv(df, self.__DEFAULT_CSV_PATH)

    def test_write_template_from_empty_csv(self):
        self.__create_empty_csv()
        with self.assertRaises(IndexError):
            _ = self.__CSV_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, self.__DEFAULT_NODE_ID)

    def __create_empty_csv(self):
        df = pd.DataFrame(columns=self.__CSV_HANDLER.get_csv_columns())
        self.__CSV_HANDLER.save_df_to_csv(df, self.__DEFAULT_CSV_PATH)

    def test_write_template_from_missing_csv(self):
        with self.assertRaises(FileNotFoundError):
            _ = self.__CSV_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, '99')

    def test_write_from_missing_csv_rows(self):
        self.__create_csv1_with_missing_rows()
        with self.assertRaises(ValueError):
            _ = self.__CSV_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, self.__DEFAULT_NODE_ID)

    def __create_csv1_with_missing_rows(self):
        df = pd.DataFrame(columns=['date', 'last_contact', 'last_start'])
        df.loc[len(df)] = ['2022-01-01 12:00:00', '2022-01-01 11:00:00', '2022-01-01 00:00:00']
        self.__CSV_HANDLER.save_df_to_csv(df, self.__DEFAULT_CSV_PATH)

    def test_write_into_missing_template_keys(self):
        template = bs4.BeautifulSoup(self.__TEMPLATE, 'html.parser')
        with self.assertRaises(AttributeError):
            template.find(class_='nonexisting').string.replace_with('value')

    def __check_template_page_dates(self, page_template: str, last_check: str, last_contact: str, last_start: str, last_write: str, last_reject: str):
        page = bs4.BeautifulSoup(page_template, 'html.parser')
        self.assertEqual(last_check, page.find(class_='last_check').string)
        self.assertEqual(last_contact, page.find(class_='last_contact').string)
        self.assertEqual(last_start, page.find(class_='last_start').string)
        self.assertEqual(last_write, page.find(class_='last_write').string)
        self.assertEqual(last_reject, page.find(class_='last_reject').string)

    def __check_template_page_total_imports(self, page_template: str, imported: str, updated: str, invalid: str, failed: str, error_rate: str):
        page = bs4.BeautifulSoup(page_template, 'html.parser')
        self.assertEqual(imported, page.find(class_='imported').string)
        self.assertEqual(updated, page.find(class_='updated').string)
        self.assertEqual(invalid, page.find(class_='invalid').string)
        self.assertEqual(failed, page.find(class_='failed').string)
        self.assertEqual(error_rate, page.find(class_='error_rate').string)

    def __check_template_page_daily_imports(self, page_template: str, imported: str, updated: str, invalid: str, failed: str, error_rate: str):
        page = bs4.BeautifulSoup(page_template, 'html.parser')
        self.assertEqual(imported, page.find(class_='daily_imported').string)
        self.assertEqual(updated, page.find(class_='daily_updated').string)
        self.assertEqual(invalid, page.find(class_='daily_invalid').string)
        self.assertEqual(failed, page.find(class_='daily_failed').string)
        self.assertEqual(error_rate, page.find(class_='daily_error_rate').string)


if __name__ == '__main__':
    unittest.main()
