import unittest

import bs4
from common import PropertiesReader
from csv_to_confluence import TemplatePageElementCreator, TemplatePageLoader
from email_service import NoImportsNotificationHandler, OfflineNotificationHandler, OutdatedVersionNotificationHandler


class TestNotificationHandler(unittest.TestCase):
    __TEMPLATE: str = None

    @classmethod
    def setUpClass(cls):
        PropertiesReader().load_properties_as_env_vars('settings.json')
        cls.__LOADER = TemplatePageLoader()
        cls.__ELEMENT_CREATOR = TemplatePageElementCreator()
        cls.__OFFLINE_NOTIFIER = OfflineNotificationHandler()
        cls.__NO_IMPORTS_NOTIFER = NoImportsNotificationHandler()
        cls.__OUTDATED_VERSION_NOTIFIER = OutdatedVersionNotificationHandler()

    def test_offline_status(self):
        self.__set_status_of_template_page('OFFLINE')
        self.assertTrue(self.__OFFLINE_NOTIFIER.did_my_status_occur(self.__TEMPLATE))
        self.assertFalse(self.__NO_IMPORTS_NOTIFER.did_my_status_occur(self.__TEMPLATE))

    def test_no_imports_status(self):
        self.__set_status_of_template_page('NO IMPORTS')
        self.assertFalse(self.__OFFLINE_NOTIFIER.did_my_status_occur(self.__TEMPLATE))
        self.assertTrue(self.__NO_IMPORTS_NOTIFER.did_my_status_occur(self.__TEMPLATE))

    def test_outdate_version_status(self):
        self.__set_version_of_template_page('1.2.3')
        self.assertTrue(self.__OUTDATED_VERSION_NOTIFIER.did_my_status_occur(self.__TEMPLATE))

    def test_empty_status_and_version(self):
        self.__set_status_of_template_page('')
        self.__set_version_of_template_page('')
        self.assertFalse(self.__OFFLINE_NOTIFIER.did_my_status_occur(self.__TEMPLATE))
        self.assertFalse(self.__NO_IMPORTS_NOTIFER.did_my_status_occur(self.__TEMPLATE))
        self.assertTrue(self.__OUTDATED_VERSION_NOTIFIER.did_my_status_occur(self.__TEMPLATE))

    def test_default_status_and_version(self):
        self.__set_status_of_template_page('ONLINE')
        self.__set_version_of_template_page('1.5.2')
        self.assertFalse(self.__OFFLINE_NOTIFIER.did_my_status_occur(self.__TEMPLATE))
        self.assertFalse(self.__NO_IMPORTS_NOTIFER.did_my_status_occur(self.__TEMPLATE))
        self.assertFalse(self.__OUTDATED_VERSION_NOTIFIER.did_my_status_occur(self.__TEMPLATE))

    def __set_status_of_template_page(self, title_status: str):
        self.__TEMPLATE = self.__LOADER.get_template_page()
        soup = bs4.BeautifulSoup(self.__TEMPLATE, 'html.parser')
        param_title = self.__ELEMENT_CREATOR.create_ac_parameter_element('title', title_status)
        frame = self.__ELEMENT_CREATOR.create_ac_macro_element('status')
        frame.append(param_title)
        status = self.__ELEMENT_CREATOR.create_html_element('td', {'style': 'text-align:center;', 'class': 'status'})
        status.append(frame)
        soup.find(class_='status').replace_with(status)
        self.__TEMPLATE = str(soup)

    def __set_version_of_template_page(self, version: str):
        self.__TEMPLATE = self.__LOADER.get_template_page()
        soup = bs4.BeautifulSoup(self.__TEMPLATE, 'html.parser')
        soup.find(class_='dwh-j2ee').string.replace_with(version)
        self.__TEMPLATE = str(soup)


if __name__ == '__main__':
    unittest.main()
