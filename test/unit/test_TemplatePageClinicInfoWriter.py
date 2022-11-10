import os
import sys
import unittest
from pathlib import Path

this_path = Path(os.path.realpath(__file__))
path_src = os.path.join(this_path.parents[2], 'src')
sys.path.insert(0, path_src)

import bs4
from common import PropertiesReader
from csv_to_confluence import TemplatePageClinicInfoWriter, TemplatePageLoader


class TestTemplatePageClinicInfoWriter(unittest.TestCase):
    __DIR_ROOT: str = None
    __TEMPLATE: str = None

    @classmethod
    def setUpClass(cls):
        path_settings = os.path.join(this_path.parents[1], 'resources', 'settings.json')
        PropertiesReader().load_properties_as_env_vars(path_settings)
        cls.__DIR_ROOT = os.environ['ROOT_DIR'] if os.environ['ROOT_DIR'] else os.getcwd()
        cls.__CLINIC_INFO_WRITER = TemplatePageClinicInfoWriter()

    def setUp(self):
        loader = TemplatePageLoader()
        self.__TEMPLATE = loader.get_template_page()

    def test_clinic_name_one(self):
        page = self.__CLINIC_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, '1')
        self.__check_key_value_of_page(page, 'clinic_name', 'Institute of One')

    def __check_key_value_of_page(self, page: str, key_page: str, expected_value: str):
        html = bs4.BeautifulSoup(page, 'html.parser')
        name = html.find(class_=key_page).string
        self.assertEqual(expected_value, name)

    def test_information_system_one(self):
        page = self.__CLINIC_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, '1')
        self.__check_key_value_of_page(page, 'information_system', 'ABC')

    def test_interface_import_one(self):
        page = self.__CLINIC_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, '1')
        self.__check_key_value_of_page(page, 'interface_import', 'DEF')

    def test_clinic_name_two(self):
        page = self.__CLINIC_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, '2')
        self.__check_key_value_of_page(page, 'clinic_name', 'Institute of Two')

    def test_information_system_no_name_in_mapping(self):
        page = self.__CLINIC_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, '2')
        self.__check_key_value_of_page(page, 'information_system', 'changeme')

    def test_interface_import_no_name_in_mapping(self):
        page = self.__CLINIC_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, '2')
        self.__check_key_value_of_page(page, 'interface_import', 'changeme')

    def test_clinic_name_no_name_in_mapping(self):
        page = self.__CLINIC_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, '3')
        self.__check_key_value_of_page(page, 'clinic_name', 'changeme')

    def test_clinic_name_no_node_in_mapping(self):
        with self.assertRaises(KeyError):
            _ = self.__CLINIC_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, '99')

    def test_full_clinic_ids(self):
        page = self.__CLINIC_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, '3')
        self.__check_key_value_of_page(page, 'root_patient', '1.2.2')
        self.__check_key_value_of_page(page, 'root_encounter', '1.2.45')
        self.__check_key_value_of_page(page, 'root_billing', '1.2.47')
        self.__check_key_value_of_page(page, 'format_patient', '1111')
        self.__check_key_value_of_page(page, 'format_encounter', '2222')
        self.__check_key_value_of_page(page, 'format_billing', '3333')

    def test_some_clinic_ids(self):
        page = self.__CLINIC_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, '10')
        self.__check_key_value_of_page(page, 'root_patient', '1.2.2')
        self.__check_key_value_of_page(page, 'root_encounter', 'changeme')
        self.__check_key_value_of_page(page, 'root_billing', 'changeme')
        self.__check_key_value_of_page(page, 'format_patient', '1111')
        self.__check_key_value_of_page(page, 'format_encounter', 'changeme')
        self.__check_key_value_of_page(page, 'format_billing', 'changeme')

    def test_no_clinic_ids(self):
        page = self.__CLINIC_INFO_WRITER.add_content_to_template_page(self.__TEMPLATE, '2')
        self.__check_key_value_of_page(page, 'root_patient', 'changeme')
        self.__check_key_value_of_page(page, 'root_encounter', 'changeme')
        self.__check_key_value_of_page(page, 'root_billing', 'changeme')
        self.__check_key_value_of_page(page, 'format_patient', 'changeme')
        self.__check_key_value_of_page(page, 'format_encounter', 'changeme')
        self.__check_key_value_of_page(page, 'format_billing', 'changeme')


if __name__ == '__main__':
    unittest.main()
