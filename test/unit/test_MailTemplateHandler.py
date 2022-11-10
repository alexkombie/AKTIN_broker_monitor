import os
import sys
import unittest
from email.mime.text import MIMEText
from pathlib import Path
from shutil import rmtree

import bs4
import pandas as pd

this_path = Path(os.path.realpath(__file__))
path_src = os.path.join(this_path.parents[2], 'src')
sys.path.insert(0, path_src)

from csv_to_confluence import TemplatePageLoader
from common import InfoCSVHandler, PropertiesReader
from email_service import NoImportsMailTemplateHandler, OfflineMailTemplateHandler, OutdatedVersionMailTemplateHandler


class TestMailTemplateHandler(unittest.TestCase):
    __TEMPLATE: str = None
    __DEFAULT_NODE_ID: str = '1'
    __DIR_ROOT: str = None

    @classmethod
    def setUpClass(cls):
        path_settings = os.path.join(this_path.parents[1], 'resources', 'settings.json')
        PropertiesReader().load_properties_as_env_vars(path_settings)
        cls.__DIR_ROOT = os.environ['ROOT_DIR'] if os.environ['ROOT_DIR'] else os.getcwd()
        cls.__CSV_HANDLER = InfoCSVHandler()
        cls.__OFFLINE_MAIL_TEMPLATE_HANDLER = OfflineMailTemplateHandler()
        cls.__NO_IMPORTS_MAIL_TEMPLATE_HANDLER = NoImportsMailTemplateHandler(cls.__DEFAULT_NODE_ID)
        cls.__OUTDATED_VERSION_MAIL_TEMPLATE_HANDLER = OutdatedVersionMailTemplateHandler()

    def setUp(self):
        self.__TEMPLATE = TemplatePageLoader().get_template_page()
        soup = bs4.BeautifulSoup(self.__TEMPLATE, 'html.parser')
        soup.find(class_='last_contact').string.replace_with('2022-01-01 12:00:45')
        soup.find(class_='last_write').string.replace_with('2025-11-11')
        soup.find(class_='clinic_name').string.replace_with('important clinic')
        soup.find(class_='dwh-j2ee').string.replace_with('1.2.3')
        self.__TEMPLATE = str(soup)

    def tearDown(self):
        if Path(self.__DIR_ROOT).exists() and Path(self.__DIR_ROOT).is_dir():
            rmtree(self.__DIR_ROOT)

    def test_offline_mail_template(self):
        mail = self.__OFFLINE_MAIL_TEMPLATE_HANDLER.get_mail_template_filled_with_information_from_template_page(self.__TEMPLATE)
        self.__check_common_config(mail)
        self.assertEqual('Automatische Information: AKTIN DWH Offline', mail['Subject'])
        self.assertTrue('<b>01.01.2022</b>' in mail.as_string())
        self.assertFalse('<b>${last_contact}</b>' in mail.as_string())

    def test_no_imports_mail_template(self):
        mail = self.__NO_IMPORTS_MAIL_TEMPLATE_HANDLER.get_mail_template_filled_with_information_from_template_page(self.__TEMPLATE)
        self.__check_common_config(mail)
        self.assertEqual('Automatische Information: AKTIN DWH Keine Imports', mail['Subject'])
        self.assertTrue('<b>11.11.2025</b>' in mail.as_string())
        self.assertFalse('<b>${last_write}</b>' in mail.as_string())

    def test_outdated_version_mail_template(self):
        mail = self.__OUTDATED_VERSION_MAIL_TEMPLATE_HANDLER.get_mail_template_filled_with_information_from_template_page(self.__TEMPLATE)
        self.__check_common_config(mail)
        self.assertEqual('Automatische Information: AKTIN DWH Version veraltet', mail['Subject'])
        self.assertTrue('<b>1.2.3</b>' in mail.as_string())
        self.assertFalse('<b>${version_dwh}</b>' in mail.as_string())
        # self.assertTrue('<b>1.5.1rc1</b>' in mail.as_string())
        self.assertFalse('<b>${current_version_dwh}</b>' in mail.as_string())
        self.assertTrue('<b>1.5.3</b>' in mail.as_string())
        self.assertFalse('<b>${current_version_i2b2}</b>' in mail.as_string())

    def __check_common_config(self, mail: MIMEText):
        self.assertEqual(MIMEText, type(mail))
        self.assertEqual('text/html; charset="iso-8859-1"', mail['Content-Type'])
        self.assertTrue('<b>important clinic</b>' in mail.as_string())
        self.assertFalse('<b>${clinic_name}</b>' in mail.as_string())

    def test_write_last_import_date_from_csv(self):
        self.__create_csv()
        self.__set_empty_last_write_in_template()
        mail = self.__NO_IMPORTS_MAIL_TEMPLATE_HANDLER.get_mail_template_filled_with_information_from_template_page(self.__TEMPLATE)
        self.assertTrue('<b>11.11.2022</b>' in mail.as_string())
        self.assertFalse('<b>${last_write}</b>' in mail.as_string())

    def __set_empty_last_write_in_template(self):
        self.__TEMPLATE = TemplatePageLoader().get_template_page()
        soup = bs4.BeautifulSoup(self.__TEMPLATE, 'html.parser')
        soup.find(class_='last_write').string.replace_with('-')
        self.__TEMPLATE = str(soup)

    def __create_csv(self):
        name_csv = self.__CSV_HANDLER.generate_csv_name(self.__DEFAULT_NODE_ID)
        dir_working = os.path.join(self.__DIR_ROOT, self.__DEFAULT_NODE_ID)
        os.makedirs(dir_working)
        path_csv = os.path.join(dir_working, name_csv)
        df = pd.DataFrame(columns=self.__CSV_HANDLER.get_csv_columns())
        df.loc[len(df)] = ['-', '-', '-', '-', '-', '0', '0', '0', '0', '0.0', '-', '-', '-', '-', '-']
        df.loc[len(df)] = ['-', '-', '-', '2022-11-11 00:00:00', '-', '0', '0', '0', '0', '0.0', '-', '-', '-', '-', '-']
        df.loc[len(df)] = ['-', '-', '-', '-', '-', '0', '0', '0', '0', '0.0', '-', '-', '-', '-', '-']
        self.__CSV_HANDLER.save_df_to_csv(df, path_csv)


if __name__ == '__main__':
    unittest.main()
