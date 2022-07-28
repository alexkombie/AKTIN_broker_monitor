import unittest
from email.mime.text import MIMEText

import bs4
from common import PropertiesReader
from csv_to_confluence import TemplatePageLoader
from email_service import NoImportsMailTemplateHandler, OfflineMailTemplateHandler


class TestMailTemplateHandler(unittest.TestCase):
    __TEMPLATE: str = None

    @classmethod
    def setUpClass(cls):
        PropertiesReader().load_properties_as_env_vars('settings.json')
        cls.__OFFLINE_MAIL_TEMPLATE_HANDLER = OfflineMailTemplateHandler()
        cls.__NO_IMPORTS_MAIL_TEMPLATE_HANDLER = NoImportsMailTemplateHandler()

    def setUp(self):
        self.__TEMPLATE = TemplatePageLoader().get_template_page()
        soup = bs4.BeautifulSoup(self.__TEMPLATE, 'html.parser')
        soup.find(class_='last_contact').string.replace_with('2022-01-01 12:00:45')
        soup.find(class_='last_write').string.replace_with('2025-11-11')
        soup.find(class_='clinic_name').string.replace_with('important clinic')
        self.__TEMPLATE = str(soup)

    def test_offline_mail_template(self):
        mail = self.__OFFLINE_MAIL_TEMPLATE_HANDLER.get_mail_template_filled_with_information_from_template_page(self.__TEMPLATE)
        self.__check_common_config(mail)
        self.assertEqual('Automatische Information: AKTIN DWH Offline', mail['Subject'])
        print(mail.as_string())
        self.assertTrue('<b>01.01.2022</b>' in mail.as_string())
        self.assertFalse('<b>${last_contact}</b>' in mail.as_string())

    def test_no_imports_mail_template(self):
        mail = self.__NO_IMPORTS_MAIL_TEMPLATE_HANDLER.get_mail_template_filled_with_information_from_template_page(self.__TEMPLATE)
        self.__check_common_config(mail)
        self.assertEqual('Automatische Information: AKTIN DWH Keine Imports', mail['Subject'])
        print(mail.as_string())
        self.assertTrue('<b>11.11.2025</b>' in mail.as_string())
        self.assertFalse('<b>${last_write}</b>' in mail.as_string())

    def __check_common_config(self, mail: MIMEText):
        self.assertEqual(MIMEText, type(mail))
        self.assertEqual('text/html; charset="iso-8859-1"', mail['Content-Type'])
        self.assertTrue('<b>important clinic</b>' in mail.as_string())
        self.assertFalse('<b>${clinic_name}</b>' in mail.as_string())


if __name__ == '__main__':
    unittest.main()

#
