import os
import unittest
from shutil import rmtree

import bs4
import pandas as pd
from common import InfoCSVHandler, PropertiesReader
from csv_to_confluence import TemplatePageClinicInfoWriter, TemplatePageLoader


class TestTemplatePageClinicInfoWriter(unittest.TestCase):
    __DIR_ROOT: str = None
    __TEMPLATE: str = None

    @classmethod
    def setUpClass(cls):
        PropertiesReader().load_properties_as_env_vars('settings.json')
        cls.__DIR_ROOT = os.environ['ROOT_DIR'] if os.environ['ROOT_DIR'] else os.getcwd()
        cls.__CLINIC_INFO_WRITER = TemplatePageClinicInfoWriter()
        cls.__CSV_HANDLER = InfoCSVHandler()

    def setUp(self):
        loader = TemplatePageLoader()
        self.__TEMPLATE = loader.get_template_page()

    def tearDown(self):
        [rmtree(name) for name in os.listdir(self.__DIR_ROOT) if os.path.isdir(name) and len(name) <= 2]

    def test_clinic_name_one(self):
        id_node = '1'
        self.__init_working_dir_with_default_csv_for_node(id_node)
        page = self.__CLINIC_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, id_node)
        self.__check_key_value_of_page(page, 'clinic_name', 'Institute of One')

    def __init_working_dir_with_default_csv_for_node(self, id_node: str):
        name_csv = InfoCSVHandler().generate_csv_name(id_node)
        dir_working = os.path.join(self.__DIR_ROOT, id_node)
        if not os.path.exists(dir_working):
            os.makedirs(dir_working)
        df = pd.DataFrame(columns=self.__CSV_HANDLER.get_csv_columns())
        df.loc[len(df)] = ['2022-01-01 00:00:00', '2022-02-02 00:00:00', '2022-03-03 00:00:00', '-', '-', '0', '0', '0', '0', '0.0', '-', '-', '-', '-', '-']
        path_csv = os.path.join(dir_working, name_csv)
        self.__CSV_HANDLER.save_df_to_csv(df, path_csv)

    def __check_key_value_of_page(self, page: str, key_page: str, expected_value: str):
        html = bs4.BeautifulSoup(page, 'html.parser')
        name = html.find(class_=key_page).string
        self.assertEqual(expected_value, name)

    def test_information_system_one(self):
        id_node = '1'
        self.__init_working_dir_with_default_csv_for_node(id_node)
        page = self.__CLINIC_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, id_node)
        self.__check_key_value_of_page(page, 'information_system', 'ABC')

    def test_interface_import_one(self):
        id_node = '1'
        self.__init_working_dir_with_default_csv_for_node(id_node)
        page = self.__CLINIC_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, id_node)
        self.__check_key_value_of_page(page, 'interface_import', 'DEF')

    def test_clinic_name_two(self):
        id_node = '2'
        self.__init_working_dir_with_default_csv_for_node(id_node)
        page = self.__CLINIC_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, id_node)
        self.__check_key_value_of_page(page, 'clinic_name', 'Institute of Two')

    def test_information_system_no_name_in_mapping(self):
        id_node = '2'
        self.__init_working_dir_with_default_csv_for_node(id_node)
        page = self.__CLINIC_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, id_node)
        self.__check_key_value_of_page(page, 'information_system', 'changeme')

    def test_interface_import_no_name_in_mapping(self):
        id_node = '2'
        self.__init_working_dir_with_default_csv_for_node(id_node)
        page = self.__CLINIC_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, id_node)
        self.__check_key_value_of_page(page, 'interface_import', 'changeme')

    def test_clinic_name_no_name_in_mapping(self):
        id_node = '3'
        self.__init_working_dir_with_default_csv_for_node(id_node)
        page = self.__CLINIC_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, id_node)
        self.__check_key_value_of_page(page, 'clinic_name', 'changeme')

    def test_clinic_name_no_node_in_mapping(self):
        id_node = '99'
        self.__init_working_dir_with_default_csv_for_node(id_node)
        with self.assertRaises(KeyError):
            _ = self.__CLINIC_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, id_node)

    def test_monitoring_start_date(self):
        id_node = '1'
        self.__init_working_dir_with_default_csv_for_node(id_node)
        page = self.__CLINIC_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, id_node)
        self.__check_monitoring_start_date_of_page(page, '2022-01-01')

    def __check_monitoring_start_date_of_page(self, page: str, expected_start_date: str):
        html = bs4.BeautifulSoup(page, 'html.parser')
        date = html.find_all('time')[0].get('datetime')
        self.assertEqual(expected_start_date, date)

    def test_monitoring_start_date_empty_csv(self):
        id_node = '1'
        self.__init_working_dir_with_empty_csv_for_node(id_node)
        with self.assertRaises(IndexError):
            _ = self.__CLINIC_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, id_node)

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
            _ = self.__CLINIC_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, id_node)

    def __init_working_dir(self, id_node: str):
        dir_working = os.path.join(self.__DIR_ROOT, id_node)
        if not os.path.exists(dir_working):
            os.makedirs(dir_working)

    def test_full_clinic_ids(self):
        id_node = '3'
        self.__init_working_dir_with_default_csv_for_node(id_node)
        page = self.__CLINIC_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, id_node)
        self.__check_key_value_of_page(page, 'root_patient', '1.2.2')
        self.__check_key_value_of_page(page, 'root_encounter', '1.2.45')
        self.__check_key_value_of_page(page, 'root_billing', '1.2.47')
        self.__check_key_value_of_page(page, 'format_patient', '1111')
        self.__check_key_value_of_page(page, 'format_encounter', '2222')
        self.__check_key_value_of_page(page, 'format_billing', '3333')

    def test_some_clinic_ids(self):
        id_node = '10'
        self.__init_working_dir_with_default_csv_for_node(id_node)
        page = self.__CLINIC_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, id_node)
        self.__check_key_value_of_page(page, 'root_patient', '1.2.2')
        self.__check_key_value_of_page(page, 'root_encounter', 'changeme')
        self.__check_key_value_of_page(page, 'root_billing', 'changeme')
        self.__check_key_value_of_page(page, 'format_patient', '1111')
        self.__check_key_value_of_page(page, 'format_encounter', 'changeme')
        self.__check_key_value_of_page(page, 'format_billing', 'changeme')

    def test_no_clinic_ids(self):
        id_node = '2'
        self.__init_working_dir_with_default_csv_for_node(id_node)
        page = self.__CLINIC_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, id_node)
        self.__check_key_value_of_page(page, 'root_patient', 'changeme')
        self.__check_key_value_of_page(page, 'root_encounter', 'changeme')
        self.__check_key_value_of_page(page, 'root_billing', 'changeme')
        self.__check_key_value_of_page(page, 'format_patient', 'changeme')
        self.__check_key_value_of_page(page, 'format_encounter', 'changeme')
        self.__check_key_value_of_page(page, 'format_billing', 'changeme')


if __name__ == '__main__':
    unittest.main()
