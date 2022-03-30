import os
import unittest

import bs4
import pandas as pd
from csv_to_confluence import TemplatePageCSVInfoWriter
from csv_to_confluence import TemplateResourceLoader
from common import InfoCSVHandler
from common import load_properties_file_as_environment


class TestTemplatePageCSVInfoWriter(unittest.TestCase):
    __DEFAULT_NODE_ID: str = '0'
    __DIR_WORKING: str = None
    __HANDLER = InfoCSVHandler()
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
        self.__create_csv1()
        page = self.__write_current_csv_into_template_page()
        self.__check_template_page_dates(page, '2022-01-01 12:00:00', '2022-01-01 11:00:00', '2022-01-01 00:00:00', '-', '-')
        self.__check_template_page_total_imports(page, '0', '0', '0', '0', '0.0')
        self.__check_template_page_daily_imports(page, '-', '-', '-', '-', '-')

    def test_write_template_from_multiple_rows(self):
        self.__create_csv2()
        page = self.__write_current_csv_into_template_page()
        self.__check_template_page_dates(page, '2022-02-02 12:00:00', '2022-02-02 11:00:00', '2022-02-02 00:00:00', '2022-02-02 10:00:00', '2022-02-02 10:00:00')
        self.__check_template_page_total_imports(page, '2000', '2000', '2000', '2000', '50.0')
        self.__check_template_page_daily_imports(page, '1000', '1000', '1000', '1000', '50.0')

    def test_write_template_from_row_with_null_values(self):
        self.__create_csv3()
        with self.assertRaises(AttributeError):
            _ = self.__write_current_csv_into_template_page()

    def test_write_template_from_empty_csv(self):
        self.__create_empty_csv()
        with self.assertRaises(IndexError):
            _ = self.__write_current_csv_into_template_page()

    def test_write_template_from_missing_csv(self):
        with self.assertRaises(FileNotFoundError):
            _ = TemplatePageCSVInfoWriter('99', self.__DIR_WORKING)

    def test_write_from_missing_csv_rows(self):
        self.__create_csv1_with_missing_rows()
        with self.assertRaises(ValueError):
            _ = self.__write_current_csv_into_template_page()

    def test_write_into_missing_template_keys(self):
        template = bs4.BeautifulSoup(self.__TEMPLATE, 'html.parser')
        with self.assertRaises(AttributeError):
            template.find(class_='nonexisting').string.replace_with('value')

    def __create_csv1(self):
        df = pd.DataFrame(columns=self.__HANDLER.get_csv_columns())
        df.loc[len(df)] = ['2022-01-01 12:00:00', '2022-01-01 11:00:00', '2022-01-01 00:00:00', '-', '-', '0', '0', '0', '0', '0.0', '-', '-', '-', '-', '-']
        self.__HANDLER.save_df_to_csv(df, self.__PATH_CSV)

    def __create_csv2(self):
        df = pd.DataFrame(columns=self.__HANDLER.get_csv_columns())
        df.loc[len(df)] = ['2022-01-01 12:00:00', '2022-01-01 11:00:00', '2022-01-01 00:00:00', '-', '-', '0', '0', '0', '0', '0.0', '-', '-', '-', '-', '-']
        df.loc[len(df)] = ['2022-02-02 12:00:00', '2022-02-02 11:00:00', '2022-02-02 00:00:00', '2022-02-02 10:00:00', '2022-02-02 10:00:00',
                           '2000', '2000', '2000', '2000', '50.0', '1000', '1000', '1000', '1000', '50.0']
        self.__HANDLER.save_df_to_csv(df, self.__PATH_CSV)

    def __create_csv3(self):
        df = pd.DataFrame(columns=self.__HANDLER.get_csv_columns())
        df.loc[len(df)] = ['2022-01-01 12:00:00', '2022-01-01 11:00:00', '2022-01-01 00:00:00', None, '-', None, '0', '0', '0', '0.0', '-', '-', '-', '-', '-']
        self.__HANDLER.save_df_to_csv(df, self.__PATH_CSV)

    def __create_empty_csv(self):
        df = pd.DataFrame(columns=self.__HANDLER.get_csv_columns())
        self.__HANDLER.save_df_to_csv(df, self.__PATH_CSV)

    def __create_csv1_with_missing_rows(self):
        df = pd.DataFrame(columns=['date', 'last_contact', 'last_start'])
        df.loc[len(df)] = ['2022-01-01 12:00:00', '2022-01-01 11:00:00', '2022-01-01 00:00:00']
        self.__HANDLER.save_df_to_csv(df, self.__PATH_CSV)

    def __write_current_csv_into_template_page(self) -> str:
        writer = TemplatePageCSVInfoWriter(self.__DEFAULT_NODE_ID, self.__DIR_WORKING)
        page = writer.add_content_to_template_page(self.__TEMPLATE)
        return page

    def __check_template_page_dates(self, page: str, last_check: str, last_contact: str, last_start: str, last_write: str, last_reject: str):
        page = bs4.BeautifulSoup(page, 'html.parser')
        self.assertEqual(last_check, page.find(class_='last_check').string)
        self.assertEqual(last_contact, page.find(class_='last_contact').string)
        self.assertEqual(last_start, page.find(class_='last_start').string)
        self.assertEqual(last_write, page.find(class_='last_write').string)
        self.assertEqual(last_reject, page.find(class_='last_reject').string)

    def __check_template_page_total_imports(self, page: str, imported: str, updated: str, invalid: str, failed: str, error_rate: str):
        page = bs4.BeautifulSoup(page, 'html.parser')
        self.assertEqual(imported, page.find(class_='imported').string)
        self.assertEqual(updated, page.find(class_='updated').string)
        self.assertEqual(invalid, page.find(class_='invalid').string)
        self.assertEqual(failed, page.find(class_='failed').string)
        self.assertEqual(error_rate, page.find(class_='error_rate').string)

    def __check_template_page_daily_imports(self, page: str, imported: str, updated: str, invalid: str, failed: str, error_rate: str):
        page = bs4.BeautifulSoup(page, 'html.parser')
        self.assertEqual(imported, page.find(class_='daily_imported').string)
        self.assertEqual(updated, page.find(class_='daily_updated').string)
        self.assertEqual(invalid, page.find(class_='daily_invalid').string)
        self.assertEqual(failed, page.find(class_='daily_failed').string)
        self.assertEqual(error_rate, page.find(class_='daily_error_rate').string)


if __name__ == '__main__':
    unittest.main()
