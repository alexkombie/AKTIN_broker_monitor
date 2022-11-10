import os
import sys
import unittest
from pathlib import Path
from shutil import rmtree

import bs4
import pandas as pd

this_path = Path(os.path.realpath(__file__))
path_src = os.path.join(this_path.parents[2], 'src')
sys.path.insert(0, path_src)

from common import ErrorCSVHandler, PropertiesReader
from csv_to_confluence import TemplatePageLoader, TemplatePageCSVErrorWriter


class TestTemplatePageCSVErrorWriter(unittest.TestCase):
    __DEFAULT_NODE_ID: str = '0'
    __DIR_ROOT: str = None
    __TEMPLATE: str = None

    @classmethod
    def setUpClass(cls):
        path_settings = os.path.join(this_path.parents[1], 'resources', 'settings.json')
        PropertiesReader().load_properties_as_env_vars(path_settings)
        cls.__DIR_ROOT = os.environ['ROOT_DIR'] if os.environ['ROOT_DIR'] else os.getcwd()
        cls.__CSV_HANDLER = ErrorCSVHandler()
        cls.__CSV_ERROR_WRITER = TemplatePageCSVErrorWriter()
        name_csv = ErrorCSVHandler().generate_csv_name(cls.__DEFAULT_NODE_ID)
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
        self.__create_error_csv1()
        page = self.__CSV_ERROR_WRITER.add_content_to_template_page(self.__TEMPLATE, self.__DEFAULT_NODE_ID)
        self.__check_error_table_row_count(page, 1)

    def __create_error_csv1(self):
        df = pd.DataFrame(columns=self.__CSV_HANDLER.get_csv_columns())
        df.loc[len(df)] = ['2022-01-01 12:00:00', '1', 'Error']
        self.__CSV_HANDLER.save_df_to_csv(df, self.__DEFAULT_CSV_PATH)

    def test_write_template_from_multiple_rows(self):
        self.__create_error_csv2()
        page = self.__CSV_ERROR_WRITER.add_content_to_template_page(self.__TEMPLATE, self.__DEFAULT_NODE_ID)
        self.__check_error_table_row_count(page, 20)

    def __create_error_csv2(self):
        df = pd.DataFrame(columns=self.__CSV_HANDLER.get_csv_columns())
        for i in range(25):
            day = str(i).rjust(2, '0')
            timestamp = '2022-01-{} 12:00:00'.format(day)
            content = 'TestError{}'.format(i)
            df.loc[len(df)] = [timestamp, str(i), content]
        self.__CSV_HANDLER.save_df_to_csv(df, self.__DEFAULT_CSV_PATH)

    def test_write_template_from_row_with_null_values(self):
        self.__create_error_csv3()
        with self.assertRaises(AttributeError):
            _ = self.__CSV_ERROR_WRITER.add_content_to_template_page(self.__TEMPLATE, self.__DEFAULT_NODE_ID)

    def __create_error_csv3(self):
        df = pd.DataFrame(columns=self.__CSV_HANDLER.get_csv_columns())
        df.loc[len(df)] = ['2022-01-01 12:00:00', None, 'Error']
        self.__CSV_HANDLER.save_df_to_csv(df, self.__DEFAULT_CSV_PATH)

    def test_write_template_from_empty_csv(self):
        self.__create_empty_error_csv()
        page = self.__CSV_ERROR_WRITER.add_content_to_template_page(self.__TEMPLATE, self.__DEFAULT_NODE_ID)
        self.__check_error_table_row_count(page, 0)

    def __create_empty_error_csv(self):
        df = pd.DataFrame(columns=self.__CSV_HANDLER.get_csv_columns())
        self.__CSV_HANDLER.save_df_to_csv(df, self.__DEFAULT_CSV_PATH)

    def test_write_template_from_missing_csv(self):
        with self.assertRaises(FileNotFoundError):
            _ = self.__CSV_ERROR_WRITER.add_content_to_template_page(self.__TEMPLATE, '99')

    def test_write_from_missing_csv_rows(self):
        self.__create_error_csv1_with_missing_rows()
        with self.assertRaises(KeyError):
            _ = self.__CSV_ERROR_WRITER.add_content_to_template_page(self.__TEMPLATE, self.__DEFAULT_NODE_ID)

    def __create_error_csv1_with_missing_rows(self):
        df = pd.DataFrame(columns=['timestamp', 'content'])
        df.loc[len(df)] = ['2022-01-01 12:00:00', 'Error']
        self.__CSV_HANDLER.save_df_to_csv(df, self.__DEFAULT_CSV_PATH)

    def test_write_into_missing_template_keys(self):
        template = bs4.BeautifulSoup(self.__TEMPLATE, 'html.parser')
        with self.assertRaises(AttributeError):
            template.find(class_='nonexisting').string.replace_with('value')

    def __check_error_table_row_count(self, page_template: str, expected_rows: int):
        page = bs4.BeautifulSoup(page_template, 'html.parser')
        table_errors = page.find(class_='table_errors_body')
        count_rows = len(table_errors.find_all('tr'))
        self.assertEqual(expected_rows, count_rows - 1)
        count_total_column_items = len(table_errors.find_all('td'))
        self.assertEqual(expected_rows * 3, count_total_column_items)


if __name__ == '__main__':
    unittest.main()
