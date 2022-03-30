import os
import unittest

import bs4
import pandas as pd
from csv_to_confluence import TemplatePageCSVErrorWriter
from csv_to_confluence import TemplateResourceLoader
from common import ErrorCSVHandler
from common import load_properties_file_as_environment


class TestTemplatePageCSVInfoWriter(unittest.TestCase):
    __DEFAULT_NODE_ID: str = '0'
    __DIR_WORKING: str = None
    __HANDLER = ErrorCSVHandler()
    __TEMPLATE: str = None

    @classmethod
    def setUpClass(cls):
        load_properties_file_as_environment('settings.json')
        cls.__DIR_WORKING = os.environ['ROOT_DIR'] if os.environ['ROOT_DIR'] else os.getcwd()
        name_csv = cls.__HANDLER.generate_csv_name(cls.__DEFAULT_NODE_ID)
        cls.__PATH_CSV = os.path.join(cls.__DIR_WORKING, name_csv)

    def setUp(self):
        loader = TemplateResourceLoader()
        self.__TEMPLATE = loader.get_resource_as_string('template_page.html')

    def tearDown(self):
        [os.remove(name) for name in os.listdir(self.__DIR_WORKING) if '.csv' in name]

    def test_write_template_from_one_row(self):
        self.__create_error_csv1()
        page = self.__write_current_csv_into_template_page()
        self.__check_error_table_row_count(page, 1)

    def test_write_template_from_multiple_rows(self):
        self.__create_error_csv2()
        page = self.__write_current_csv_into_template_page()
        self.__check_error_table_row_count(page, 20)

    def test_write_template_from_row_with_null_values(self):
        self.__create_error_csv3()
        with self.assertRaises(AttributeError):
            _ = self.__write_current_csv_into_template_page()

    def test_write_template_from_empty_csv(self):
        self.__create_empty_error_csv()
        page = self.__write_current_csv_into_template_page()
        self.__check_error_table_row_count(page, 0)

    def test_write_template_from_missing_csv(self):
        with self.assertRaises(FileNotFoundError):
            _ = TemplatePageCSVErrorWriter('99', self.__DIR_WORKING)

    def test_write_from_missing_csv_rows(self):
        self.__create_error_csv1_with_missing_rows()
        with self.assertRaises(KeyError):
            _ = self.__write_current_csv_into_template_page()

    def test_write_into_missing_template_keys(self):
        template = bs4.BeautifulSoup(self.__TEMPLATE, 'html.parser')
        with self.assertRaises(AttributeError):
            template.find(class_='nonexisting').string.replace_with('value')

    def __create_error_csv1(self):
        df = pd.DataFrame(columns=self.__HANDLER.get_csv_columns())
        df.loc[len(df)] = ['2022-01-01 12:00:00', '1', 'Error']
        self.__HANDLER.save_df_to_csv(df, self.__PATH_CSV)

    def __create_error_csv2(self):
        df = pd.DataFrame(columns=self.__HANDLER.get_csv_columns())
        for i in range(25):
            day = str(i).rjust(2, '0')
            timestamp = '2022-01-{} 12:00:00'.format(day)
            content = 'TestError{}'.format(i)
            df.loc[len(df)] = [timestamp, str(i), content]
        self.__HANDLER.save_df_to_csv(df, self.__PATH_CSV)

    def __create_error_csv3(self):
        df = pd.DataFrame(columns=self.__HANDLER.get_csv_columns())
        df.loc[len(df)] = ['2022-01-01 12:00:00', None, 'Error']
        self.__HANDLER.save_df_to_csv(df, self.__PATH_CSV)

    def __create_empty_error_csv(self):
        df = pd.DataFrame(columns=self.__HANDLER.get_csv_columns())
        self.__HANDLER.save_df_to_csv(df, self.__PATH_CSV)

    def __create_error_csv1_with_missing_rows(self):
        df = pd.DataFrame(columns=['timestamp', 'content'])
        df.loc[len(df)] = ['2022-01-01 12:00:00', 'Error']
        self.__HANDLER.save_df_to_csv(df, self.__PATH_CSV)

    def __write_current_csv_into_template_page(self) -> str:
        writer = TemplatePageCSVErrorWriter(self.__DEFAULT_NODE_ID, self.__DIR_WORKING)
        page = writer.add_content_to_template_page(self.__TEMPLATE)
        return page

    def __check_error_table_row_count(self, page: str, expected_rows: int):
        template = bs4.BeautifulSoup(page, 'html.parser')
        table_errors = template.find(class_='table_errors_body')
        count_rows = len(table_errors.find_all('tr'))
        self.assertEqual(expected_rows, count_rows)
        count_total_column_items = len(table_errors.find_all('td'))
        self.assertEqual(expected_rows * 3, count_total_column_items)


if __name__ == '__main__':
    unittest.main()
