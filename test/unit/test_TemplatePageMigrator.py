import os
import sys
import unittest
from pathlib import Path

import bs4
from bs4.element import Tag

this_path = Path(os.path.realpath(__file__))
path_src = os.path.join(this_path.parents[2], 'src')
sys.path.insert(0, path_src)

from common import ConfigReader
from csv_to_confluence import TemplatePageLoader, TemplatePageMigrator


class TestTemplatePageMigrator(unittest.TestCase):
    __TEMPLATE: str = None

    @classmethod
    def setUpClass(cls):
        path_settings = os.path.join(this_path.parents[1], 'resources', 'settings.toml')
        ConfigReader().load_config_as_env_vars(path_settings)
        cls.__MIGRATOR = TemplatePageMigrator()

    def setUp(self):
        loader = TemplatePageLoader()
        self.__TEMPLATE = loader.get_template_page()

    def test_not_outdated_version(self):
        is_outdated = self.__MIGRATOR.is_template_page_outdated(self.__TEMPLATE)
        self.assertFalse(is_outdated)

    def test_outdated_version(self):
        self.__set_template_version_to_outdated()
        is_outdated = self.__MIGRATOR.is_template_page_outdated(self.__TEMPLATE)
        self.assertTrue(is_outdated)

    def test_tempalte_migration(self):
        self.__set_default_date_information()
        self.__set_template_version_to_outdated()
        page = self.__MIGRATOR.migrate_page_template_to_newer_version(self.__TEMPLATE)
        self.__check_default_date_information(page)

    def __set_template_version_to_outdated(self):
        html = bs4.BeautifulSoup(self.__TEMPLATE, 'html.parser')
        html.find(class_='version_template').string.replace_with('0.9')
        self.__TEMPLATE = str(html)

    def __set_default_date_information(self):
        html = bs4.BeautifulSoup(self.__TEMPLATE, 'html.parser')
        html.find(class_='online_since').replace_with(self.__create_datetime_tag())
        self.__TEMPLATE = str(html)

    @staticmethod
    def __create_datetime_tag() -> Tag:
        frame = bs4.BeautifulSoup().new_tag('td', attrs={'class': 'online_since'})
        time = bs4.BeautifulSoup().new_tag('time', attrs={'datetime': '2020-01-01'})
        frame.append(time)
        return frame

    def __check_default_date_information(self, page: str):
        html = bs4.BeautifulSoup(page, 'html.parser')
        self.assertEqual('<td class="online_since"><time datetime="2020-01-01"></time></td>', str(html.find(class_='online_since')))


if __name__ == '__main__':
    unittest.main()
