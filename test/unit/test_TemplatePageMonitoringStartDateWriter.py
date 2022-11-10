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

from common import InfoCSVHandler, PropertiesReader
from csv_to_confluence import TemplatePageLoader, TemplatePageMonitoringStartDateWriter


class TestTemplatePageClinicInfoWriter(unittest.TestCase):
    __DIR_ROOT: str = None
    __TEMPLATE: str = None

    @classmethod
    def setUpClass(cls):
        path_settings = os.path.join(this_path.parents[1], 'resources', 'settings.json')
        PropertiesReader().load_properties_as_env_vars(path_settings)
        cls.__DIR_ROOT = os.environ['ROOT_DIR'] if os.environ['ROOT_DIR'] else os.getcwd()
        cls.__START_DATE_WRITER = TemplatePageMonitoringStartDateWriter()
        cls.__CSV_HANDLER = InfoCSVHandler()

    def setUp(self):
        loader = TemplatePageLoader()
        self.__TEMPLATE = loader.get_template_page()

    def tearDown(self):
        rmtree(self.__DIR_ROOT)

    def __init_working_dir_with_default_csv_for_node(self, id_node: str):
        name_csv = InfoCSVHandler().generate_csv_name(id_node)
        dir_working = os.path.join(self.__DIR_ROOT, id_node)
        if not os.path.exists(dir_working):
            os.makedirs(dir_working)
        df = pd.DataFrame(columns=self.__CSV_HANDLER.get_csv_columns())
        df.loc[len(df)] = ['2022-01-01 00:00:00', '2022-02-02 00:00:00', '2022-03-03 00:00:00', '-', '-', '0', '0', '0', '0', '0.0', '-', '-', '-', '-', '-']
        path_csv = os.path.join(dir_working, name_csv)
        self.__CSV_HANDLER.save_df_to_csv(df, path_csv)

    def test_monitoring_start_date(self):
        id_node = '1'
        self.__init_working_dir_with_default_csv_for_node(id_node)
        page = self.__START_DATE_WRITER.add_content_to_template_page(self.__TEMPLATE, id_node)
        self.__check_monitoring_start_date_of_page(page, '2022-01-01')

    def __check_monitoring_start_date_of_page(self, page: str, expected_start_date: str):
        html = bs4.BeautifulSoup(page, 'html.parser')
        date = html.find_all('time')[0].get('datetime')
        self.assertEqual(expected_start_date, date)

    def test_monitoring_start_date_empty_csv(self):
        id_node = '1'
        self.__init_working_dir_with_empty_csv_for_node(id_node)
        with self.assertRaises(IndexError):
            _ = self.__START_DATE_WRITER.add_content_to_template_page(self.__TEMPLATE, id_node)

    def __init_working_dir_with_empty_csv_for_node(self, id_node: str):
        name_csv = InfoCSVHandler().generate_csv_name(id_node)
        dir_working = os.path.join(self.__DIR_ROOT, id_node)
        if not os.path.exists(dir_working):
            os.makedirs(dir_working)
        df = pd.DataFrame(columns=self.__CSV_HANDLER.get_csv_columns())
        path_csv = os.path.join(dir_working, name_csv)
        self.__CSV_HANDLER.save_df_to_csv(df, path_csv)

    def test_monitoring_start_date_no_csv(self):
        id_node = '1'
        self.__init_working_dir(id_node)
        with self.assertRaises(FileNotFoundError):
            _ = self.__START_DATE_WRITER.add_content_to_template_page(self.__TEMPLATE, id_node)

    def __init_working_dir(self, id_node: str):
        dir_working = os.path.join(self.__DIR_ROOT, id_node)
        if not os.path.exists(dir_working):
            os.makedirs(dir_working)


if __name__ == '__main__':
    unittest.main()
