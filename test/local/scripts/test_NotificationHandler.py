import os
import sys
import unittest
from pathlib import Path
from shutil import rmtree

this_path = Path(os.path.realpath(__file__))
path_src = os.path.join(this_path.parents[3], 'src')
sys.path.insert(0, path_src)

import bs4
from common import PropertiesReader
from csv_to_confluence import TemplatePageElementCreator, TemplatePageLoader
from email_service import NoImportsNotificationHandler, OfflineNotificationHandler, OutdatedVersionNotificationHandler


class TestNotificationHandler(unittest.TestCase):
    __DIR_ROOT: str = None

    @classmethod
    def setUpClass(cls):
        path_settings = os.path.join(this_path.parents[1], 'settings.json')
        PropertiesReader().load_properties_as_env_vars(path_settings)
        cls.__DIR_ROOT = cls.__init_root_dir()
        cls.__LOADER = TemplatePageLoader()
        cls.__ELEMENT_CREATOR = TemplatePageElementCreator()
        cls.__OFFLINE_NOTIFIER = OfflineNotificationHandler()
        cls.__NO_IMPORTS_NOTIFER = NoImportsNotificationHandler()
        cls.__OUTDATED_VERSION_NOTIFIER = OutdatedVersionNotificationHandler()

    @staticmethod
    def __init_root_dir():
        root_dir = os.environ['ROOT_DIR'] if os.environ['ROOT_DIR'] else os.getcwd()
        if not os.path.exists(root_dir):
            os.makedirs(root_dir)
        return root_dir

    @classmethod
    def tearDownClass(cls):
        rmtree(cls.__DIR_ROOT)

    def test_offline_status(self):
        template = self.__set_status_of_template_page('OFFLINE')
        self.assertTrue(self.__OFFLINE_NOTIFIER.did_my_status_occur(template))
        self.assertFalse(self.__NO_IMPORTS_NOTIFER.did_my_status_occur(template))

    def test_no_imports_status(self):
        template = self.__set_status_of_template_page('NO IMPORTS')
        self.assertFalse(self.__OFFLINE_NOTIFIER.did_my_status_occur(template))
        self.assertTrue(self.__NO_IMPORTS_NOTIFER.did_my_status_occur(template))

    def test_outdated_version_status(self):
        template = self.__set_version_of_template_page('1.2.3')
        self.assertTrue(self.__OUTDATED_VERSION_NOTIFIER.did_my_status_occur(template))

    def test_no_version(self):
        template = self.__set_version_of_template_page('-')
        self.assertFalse(self.__OUTDATED_VERSION_NOTIFIER.did_my_status_occur(template))

    def test_empty_status(self):
        template = self.__set_status_of_template_page('')
        self.assertFalse(self.__OFFLINE_NOTIFIER.did_my_status_occur(template))
        self.assertFalse(self.__NO_IMPORTS_NOTIFER.did_my_status_occur(template))
        self.assertFalse(self.__OUTDATED_VERSION_NOTIFIER.did_my_status_occur(template))

    def test_empty_version(self):
        template = self.__set_version_of_template_page('')
        self.assertTrue(self.__OFFLINE_NOTIFIER.did_my_status_occur(template))
        self.assertFalse(self.__NO_IMPORTS_NOTIFER.did_my_status_occur(template))
        self.assertFalse(self.__OUTDATED_VERSION_NOTIFIER.did_my_status_occur(template))

    def test_default_status(self):
        template = self.__set_status_of_template_page('ONLINE')
        self.assertFalse(self.__OFFLINE_NOTIFIER.did_my_status_occur(template))
        self.assertFalse(self.__NO_IMPORTS_NOTIFER.did_my_status_occur(template))
        self.assertFalse(self.__OUTDATED_VERSION_NOTIFIER.did_my_status_occur(template))

    def test_default_version(self):
        template = self.__set_version_of_template_page('dwh-j2ee-1.5.1rc1')
        self.assertTrue(self.__OFFLINE_NOTIFIER.did_my_status_occur(template))
        self.assertFalse(self.__NO_IMPORTS_NOTIFER.did_my_status_occur(template))
        self.assertFalse(self.__OUTDATED_VERSION_NOTIFIER.did_my_status_occur(template))

    def __set_status_of_template_page(self, title_status: str) -> str:
        template = self.__LOADER.get_template_page()
        soup = bs4.BeautifulSoup(template, 'html.parser')
        param_title = self.__ELEMENT_CREATOR.create_ac_parameter_element('title', title_status)
        frame = self.__ELEMENT_CREATOR.create_ac_macro_element('status')
        frame.append(param_title)
        status = self.__ELEMENT_CREATOR.create_html_element('td', {'style': 'text-align:center;', 'class': 'status'})
        status.append(frame)
        soup.find(class_='status').replace_with(status)
        return str(soup)

    def __set_version_of_template_page(self, version: str) -> str:
        template = self.__LOADER.get_template_page()
        soup = bs4.BeautifulSoup(template, 'html.parser')
        soup.find(class_='dwh-j2ee').string.replace_with(version)
        return str(soup)


if __name__ == '__main__':
    unittest.main()
