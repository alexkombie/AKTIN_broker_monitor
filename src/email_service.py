# -*- coding: utf-8 -*
# Created on Tue Jul 26 12:00 2022
# @version: 1.0

#
#      Copyright (c) 2022  Alexander Kombeiz
#
#      This program is free software: you can redistribute it and/or modify
#      it under the terms of the GNU Affero General Public License as
#      published by the Free Software Foundation, either version 3 of the
#      License, or (at your option) any later version.
#
#      This program is distributed in the hope that it will be useful,
#      but WITHOUT ANY WARRANTY; without even the implied warranty of
#      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#      GNU Affero General Public License for more details.
#
#      You should have received a copy of the GNU Affero General Public License
#      along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
#

import json
import logging
import os
import smtplib
import sys
from abc import ABC, abstractmethod
from email.mime.text import MIMEText
from smtplib import SMTP_SSL as SMTP

import bs4
import pandas as pd
from dateutil import parser

from common import ConfluenceConnection, ConfluenceNodeMapper, MyLogger, PropertiesReader, ResourceLoader, SingletonABCMeta, SingletonMeta, TimestampHandler


class MailServerConnection(metaclass=SingletonABCMeta):
    _CONNECTION: smtplib.SMTP_SSL = None

    __SET_REQUIRED_KEYS = {'EMAIL_HOST',
                           'EMAIL_USER',
                           'EMAIL_PASSWORD'
                           }

    def __init__(self):
        dict_email = self.__load_json_file_as_dict(os.environ['EMAIL_CONFIG_JSON'])
        self.__validate_properties(dict_email)
        self.__DICT_EMAIL = dict_email
        self._SENDER = self.__DICT_EMAIL['EMAIL_USER']

    @staticmethod
    def __load_json_file_as_dict(path_file: str) -> dict:
        with open(path_file, encoding='utf-8') as json_file:
            dict_email = json.load(json_file)
        return dict_email

    def __validate_properties(self, properties: dict):
        set_found_keys = set(properties.keys())
        set_matched_keys = self.__SET_REQUIRED_KEYS.intersection(set_found_keys)
        if set_matched_keys != self.__SET_REQUIRED_KEYS:
            raise SystemExit('following keys are missing in email config file: {0}'.format(self.__SET_REQUIRED_KEYS.difference(set_matched_keys)))

    def _connect(self):
        host = self.__DICT_EMAIL['EMAIL_HOST']
        self._CONNECTION = SMTP(host)
        user = self.__DICT_EMAIL['EMAIL_USER']
        password = self.__DICT_EMAIL['EMAIL_PASSWORD']
        self._CONNECTION.login(user, password)

    def _close(self):
        if self._CONNECTION:
            self._CONNECTION.close()


class MailSender(MailServerConnection):

    def __init__(self):
        super().__init__()
        self._connect()

    def __del__(self):
        self._close()

    def send_mail(self, list_receiver: list, mail: MIMEText):
        receivers = ','.join(list_receiver)
        mail['From'] = self._SENDER
        mail['To'] = receivers
        self._CONNECTION.sendmail(self._SENDER, receivers, mail.as_string())


class MailTemplateHandler(ResourceLoader, ABC):
    _FILENAME_TEMPLATE: str = None
    _TEXT_SUBTYPE: str = 'html'
    _PARSER: str = 'html.parser'
    _ENCODING: str = 'iso-8859-1'

    @abstractmethod
    def get_mail_template_filled_with_information_from_template_page(self, page_template: str) -> MIMEText:
        pass

    @staticmethod
    def _format_date_string_to_german_format(date: str) -> str:
        d = parser.parse(date)
        return d.strftime('%d.%m.%Y')


class OfflineMailTemplateHandler(MailTemplateHandler):
    _FILENAME_TEMPLATE: str = 'template_mail_offline.html'

    def get_mail_template_filled_with_information_from_template_page(self, page_template: str) -> MIMEText:
        soup = bs4.BeautifulSoup(page_template, self._PARSER)
        clinic_name = soup.find(class_='clinic_name').text
        last_contact = soup.find(class_='last_contact').text
        formatted_last_contact = self._format_date_string_to_german_format(last_contact)
        content = self._get_resource_as_string(self._FILENAME_TEMPLATE, self._ENCODING)
        content = content.replace('${clinic_name}', clinic_name)
        content = content.replace('${last_contact}', formatted_last_contact)
        mail = MIMEText(content, self._TEXT_SUBTYPE, self._ENCODING)
        mail['Subject'] = "Automatische Information: AKTIN DWH Offline"
        return mail


class NoImportsMailTemplateHandler(MailTemplateHandler):
    _FILENAME_TEMPLATE: str = 'template_mail_no_imports.html'

    def get_mail_template_filled_with_information_from_template_page(self, page_template: str) -> MIMEText:
        soup = bs4.BeautifulSoup(page_template, self._PARSER)
        clinic_name = soup.find(class_='clinic_name').text
        last_write = soup.find(class_='last_write').text
        formatted_last_write = self._format_date_string_to_german_format(last_write)
        content = self._get_resource_as_string(self._FILENAME_TEMPLATE, self._ENCODING)
        content = content.replace('${clinic_name}', clinic_name)
        content = content.replace('${last_write}', formatted_last_write)
        mail = MIMEText(content, self._TEXT_SUBTYPE, self._ENCODING)
        mail['Subject'] = "Automatische Information: AKTIN DWH Keine Imports"
        return mail


class ConfluencePageRecipientsExtractor(metaclass=SingletonMeta):
    __CONFLUENCE_EMAIL_LIST: str = 'E-Mail-Verteiler'

    def __init__(self):
        confluence = ConfluenceConnection()
        page_confluence_email = confluence.get_page_content(self.__CONFLUENCE_EMAIL_LIST)
        tables_email = pd.read_html(page_confluence_email)
        df = tables_email[0]
        df['Node ID'] = pd.to_numeric(df['Node ID'])
        df = df.fillna('')
        self.__DF_EMAIL = df

    def extract_all_recipients_for_node_id(self, id_node: str) -> list:
        recipients_ed = self.__extract_ed_recipients_for_node_id(id_node)
        recipients_it = self.__extract_it_recipients_for_node_id(id_node)
        recipients_ed.extend(recipients_it)
        unique_recipients = list(dict.fromkeys(recipients_ed))
        return unique_recipients

    def __extract_ed_recipients_for_node_id(self, id_node: str) -> list:
        """
        For ED contacts, only the main contacts (Hauptansprechpartner) are used (only one in most cases)
        """
        list_contacts = []
        df_recipients = self.__get_recipients_df_for_node_id(id_node, 'Notaufnahme')
        df_main_contact = df_recipients[df_recipients['Hauptansprechpartner?'] != '']
        series_contacts = df_main_contact['Kontakt']
        for contact in series_contacts:
            list_contacts.append(contact)
        return list_contacts

    def __extract_it_recipients_for_node_id(self, id_node: str) -> list:
        """
        For IT contacts, all contacts are used
        """
        list_contacts = []
        df_recipients = self.__get_recipients_df_for_node_id(id_node, 'IT')
        series_contacts = df_recipients['Kontakt']
        for contact in series_contacts:
            list_contacts.append(contact)
        return list_contacts

    def __get_recipients_df_for_node_id(self, id_node: str, type_recipients: str) -> pd.DataFrame:
        list_idx = self.__DF_EMAIL.index[self.__DF_EMAIL['Node ID'] == int(id_node)].tolist()
        df_node = self.__DF_EMAIL.iloc[list_idx]
        df_recipients = df_node[df_node['Ansprechpartner für'] == type_recipients]
        return df_recipients


class TemplatePageEmergencyStatusChecker(metaclass=SingletonMeta):
    __PARSER: str = 'html.parser'

    def check_for_emergency_status(self, page_template: str) -> int:
        if self.__has_page_status_offline(page_template):
            return 1
        elif self.__has_page_status_no_imports(page_template):
            return 2
        return 0

    def __has_page_status_offline(self, page_template: str) -> bool:
        return self.__has_page_status(page_template, 'OFFLINE')

    def __has_page_status_no_imports(self, page_template: str) -> bool:
        return self.__has_page_status(page_template, 'NO IMPORTS')

    def __has_page_status(self, page_template: str, expected_status: str) -> bool:
        soup = bs4.BeautifulSoup(page_template, self.__PARSER)
        element_status = soup.find(class_='status')
        status = element_status.find('ac:parameter', attrs={'ac:name': 'title'})
        if status.text == expected_status:
            return True
        return False


class ConsecutiveSentEmailsCounter(metaclass=SingletonMeta):
    __ENCODING = 'utf-8'
    __DICT_TRACKING: dict = {}

    def __init__(self):
        self.__PATH_TRACKING_JSON = os.path.join(os.environ['ROOT_DIR'], 'tracking_sent_mails.json')
        self.__TIMESTAMP_HANDLER = TimestampHandler()
        self.__init_tracking_json_if_not_exists()
        self.__load_tracking_json()

    def __init_tracking_json_if_not_exists(self):
        if not os.path.isfile(self.__PATH_TRACKING_JSON):
            self.__save_tracking_json()

    def __load_tracking_json(self):
        with open(self.__PATH_TRACKING_JSON, 'r', encoding=self.__ENCODING) as infile:
            self.__DICT_TRACKING = json.load(infile)

    def __save_tracking_json(self):
        with open(self.__PATH_TRACKING_JSON, 'w', encoding=self.__ENCODING) as outfile:
            json.dump(self.__DICT_TRACKING, outfile)

    def does_entry_exist_for_node(self, id_node: str) -> bool:
        return id_node in self.__DICT_TRACKING.keys()

    def create_or_update_node_entry(self, id_node: str):
        self.__DICT_TRACKING[id_node] = self.__TIMESTAMP_HANDLER.get_current_date()
        self.__save_tracking_json()

    def delete_entry_for_node_if_exists(self, id_node: str):
        if id_node in self.__DICT_TRACKING.keys():
            del self.__DICT_TRACKING[id_node]
            self.__save_tracking_json()

    def was_last_email_sent_more_than_one_week_ago(self, id_node: str) -> bool:
        last_sent = self.__DICT_TRACKING.get(id_node)
        current_date = self.__TIMESTAMP_HANDLER.get_current_date()
        delta = self.__TIMESTAMP_HANDLER.get_timedelta_in_absolute_hours(last_sent, current_date)
        delta_in_weeks = delta // 168
        return delta_in_weeks >= 1


class NodeEventNotifierManager:

    def __init__(self):
        self.__CONFLUENCE = ConfluenceConnection()
        self.__MAPPER = ConfluenceNodeMapper()
        self.__LIST_NODE_IDS = self.__MAPPER.get_all_keys()
        self.__PAGE_EMERGENCY_STATUS_CHECKER = TemplatePageEmergencyStatusChecker()
        self.__SENT_MAILS_COUNTER = ConsecutiveSentEmailsCounter()

    def notify_node_recipients_on_emergency_status(self):
        for id_node in self.__LIST_NODE_IDS:
            name_page = self.__MAPPER.get_node_value_from_mapping_dict(id_node, 'COMMON')
            if self.__CONFLUENCE.does_page_exists(name_page):
                page = self.__CONFLUENCE.get_page_content(name_page)
                status = self.__PAGE_EMERGENCY_STATUS_CHECKER.check_for_emergency_status(page)
                if status == 1:
                    handler = OfflineMailTemplateHandler()
                elif status == 2:
                    handler = NoImportsMailTemplateHandler()
                else:
                    self.__SENT_MAILS_COUNTER.delete_entry_for_node_if_exists(id_node)
                    continue
                mail = handler.get_mail_template_filled_with_information_from_template_page(page)
                recipients = ConfluencePageRecipientsExtractor().extract_all_recipients_for_node_id(id_node)
                if not recipients:
                    continue
                if self.__SENT_MAILS_COUNTER.does_entry_exist_for_node(id_node):
                    if not self.__SENT_MAILS_COUNTER.was_last_email_sent_more_than_one_week_ago(id_node):
                        continue
                MailSender().send_mail(recipients, mail)
                self.__SENT_MAILS_COUNTER.create_or_update_node_entry(id_node)


def main(path_config: str):
    logger = MyLogger()
    reader = PropertiesReader()
    try:
        logger.init_logger()
        reader.load_properties_as_env_vars(path_config)
        manager = NodeEventNotifierManager()
        manager.notify_node_recipients_on_emergency_status()
    except Exception as e:
        logging.exception(e)
    finally:
        logger.stop_logger()


if __name__ == '__main__':
    if len(sys.argv) == 1:
        raise SystemExit('path to config file is missing')
    if len(sys.argv) > 2:
        raise SystemExit('invalid number of input arguments')
    main(sys.argv[1])
