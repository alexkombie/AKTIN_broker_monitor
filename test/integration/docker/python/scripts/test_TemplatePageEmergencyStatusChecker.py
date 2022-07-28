import unittest

import bs4
from common import PropertiesReader
from csv_to_confluence import TemplatePageElementCreator, TemplatePageLoader
from email_service import TemplatePageEmergencyStatusChecker


class TestTemplatePageEmergencyStatusChecker(unittest.TestCase):
    __TEMPLATE: str = None

    @classmethod
    def setUpClass(cls):
        PropertiesReader().load_properties_as_env_vars('settings.json')
        cls.__ELEMENT_CREATOR = TemplatePageElementCreator()
        cls.__PAGE_EMERGENCY_STATUS_CHECKER = TemplatePageEmergencyStatusChecker()

    def test_offline_status(self):
        self.__set_status_of_template_page('OFFLINE')
        code = self.__PAGE_EMERGENCY_STATUS_CHECKER.check_for_emergency_status(self.__TEMPLATE)
        self.assertEqual(1, code)

    def test_no_imports_status(self):
        self.__set_status_of_template_page('NO IMPORTS')
        code = self.__PAGE_EMERGENCY_STATUS_CHECKER.check_for_emergency_status(self.__TEMPLATE)
        self.assertEqual(2, code)

    def test_default_status(self):
        self.__set_status_of_template_page('ONLINE')
        code = self.__PAGE_EMERGENCY_STATUS_CHECKER.check_for_emergency_status(self.__TEMPLATE)
        self.assertEqual(0, code)

    def test_other_status(self):
        self.__set_status_of_template_page('TESTING')
        code = self.__PAGE_EMERGENCY_STATUS_CHECKER.check_for_emergency_status(self.__TEMPLATE)
        self.assertEqual(0, code)

    def __set_status_of_template_page(self, title_status: str):
        self.__TEMPLATE = TemplatePageLoader().get_template_page()
        soup = bs4.BeautifulSoup(self.__TEMPLATE, 'html.parser')
        param_title = self.__ELEMENT_CREATOR.create_ac_parameter_element('title', title_status)
        frame = self.__ELEMENT_CREATOR.create_ac_macro_element('status')
        frame.append(param_title)
        status = self.__ELEMENT_CREATOR.create_html_element('td', {'style': 'text-align:center;', 'class': 'status'})
        status.append(frame)
        soup.find(class_='status').replace_with(status)
        self.__TEMPLATE = str(soup)


if __name__ == '__main__':
    unittest.main()
