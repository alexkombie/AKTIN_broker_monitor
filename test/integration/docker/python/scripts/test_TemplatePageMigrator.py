import unittest

import bs4
from bs4.element import Tag
from common import PropertiesReader
from csv_to_confluence import ResourceLoader, TemplatePageMigrator


class TestTemplatePageMigrator(unittest.TestCase):
    __TEMPLATE: str = None

    @classmethod
    def setUpClass(cls):
        PropertiesReader().load_properties_as_env_vars('settings.json')
        cls.__MIGRATOR = TemplatePageMigrator()

    def setUp(self):
        loader = ResourceLoader()
        self.__TEMPLATE = loader.get_resource_as_string('template_page.html')

    def test_not_outdated_version(self):
        is_outdated = self.__MIGRATOR.is_template_page_outdated(self.__TEMPLATE)
        self.assertFalse(is_outdated)

    def test_outdated_version(self):
        self.__set_template_version_to_outdated()
        is_outdated = self.__MIGRATOR.is_template_page_outdated(self.__TEMPLATE)
        self.assertTrue(is_outdated)

    def test_tempalte_migration(self):
        self.__set_default_clinic_information()
        self.__set_default_id_information()
        self.__set_default_import_information()
        self.__set_template_version_to_outdated()
        page = self.__MIGRATOR.migrate_page_template_to_newer_version(self.__TEMPLATE)
        self.__check_default_clinic_information(page)
        self.__check_default_id_information(page)
        self.__check_default_import_information_after_migration(page)

    def __set_template_version_to_outdated(self):
        html = bs4.BeautifulSoup(self.__TEMPLATE, 'html.parser')
        html.find(class_='version_template').string.replace_with('Version 0.9')
        self.__TEMPLATE = str(html)

    def __set_default_clinic_information(self):
        html = bs4.BeautifulSoup(self.__TEMPLATE, 'html.parser')
        html.find(class_='clinic_name').string.replace_with('clinic_name')
        html.find(class_='online_since').replace_with(self.__create_datetime_tag())
        html.find(class_='information_system').string.replace_with('information_system')
        html.find(class_='interface_import').string.replace_with('interface_import')
        html.find(class_='contact_ed').string.replace_with('contact_ed')
        html.find(class_='contact_it').string.replace_with('contact_it')
        self.__TEMPLATE = str(html)

    @staticmethod
    def __create_datetime_tag() -> Tag:
        frame = bs4.BeautifulSoup().new_tag('td', attrs={'class': 'online_since'})
        time = bs4.BeautifulSoup().new_tag('time', attrs={'datetime': '2020-01-01'})
        frame.append(time)
        return frame

    def __check_default_clinic_information(self, page: str):
        html = bs4.BeautifulSoup(page, 'html.parser')
        self.assertEqual('clinic_name', html.find(class_='clinic_name').string)
        self.assertEqual('<td class="online_since"><time datetime="2020-01-01"></time></td>', str(html.find(class_='online_since')))
        self.assertEqual('information_system', html.find(class_='information_system').string)
        self.assertEqual('interface_import', html.find(class_='interface_import').string)
        self.assertEqual('contact_ed', html.find(class_='contact_ed').string)
        self.assertEqual('contact_it', html.find(class_='contact_it').string)

    def __set_default_id_information(self):
        html = bs4.BeautifulSoup(self.__TEMPLATE, 'html.parser')
        html.find(class_='root_patient').string.replace_with('1111.1111')
        html.find(class_='format_patient').string.replace_with('1111')
        html.find(class_='root_encounter').string.replace_with('2222.2222')
        html.find(class_='format_encounter').string.replace_with('2222')
        html.find(class_='root_billing').string.replace_with('3333.3333')
        html.find(class_='format_billing').string.replace_with('3333')
        self.__TEMPLATE = str(html)

    def __check_default_id_information(self, page: str):
        html = bs4.BeautifulSoup(page, 'html.parser')
        self.assertEqual('1111.1111', html.find(class_='root_patient').string)
        self.assertEqual('1111', html.find(class_='format_patient').string)
        self.assertEqual('2222.2222', html.find(class_='root_encounter').string)
        self.assertEqual('2222', html.find(class_='format_encounter').string)
        self.assertEqual('3333.3333', html.find(class_='root_billing').string)
        self.assertEqual('3333', html.find(class_='format_billing').string)

    def __set_default_import_information(self):
        html = bs4.BeautifulSoup(self.__TEMPLATE, 'html.parser')
        html.find(class_='imported').string.replace_with('1000')
        html.find(class_='updated').string.replace_with('1000')
        html.find(class_='invalid').string.replace_with('1000')
        html.find(class_='failed').string.replace_with('1000')
        html.find(class_='error_rate').string.replace_with('50.0')
        self.__TEMPLATE = str(html)

    def __check_default_import_information_after_migration(self, page: str):
        html = bs4.BeautifulSoup(page, 'html.parser')
        self.assertEqual('0', html.find(class_='imported').string)
        self.assertEqual('0', html.find(class_='updated').string)
        self.assertEqual('0', html.find(class_='invalid').string)
        self.assertEqual('0', html.find(class_='failed').string)
        self.assertEqual('0.0', html.find(class_='error_rate').string)


if __name__ == '__main__':
    unittest.main()
