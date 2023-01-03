# -*- coding: utf-8 -*
# Created on Tue Jul 26 12:00 2022
# @version: 1.1

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
import sys
from abc import ABC, abstractmethod
from email.mime.text import MIMEText

import bs4
import pandas as pd
from dateutil import parser
from packaging import version

from common import ConfluenceConnection, ConfluenceNodeMapper, InfoCSVHandler, MailSender, MyLogger, PropertiesReader, \
    ResourceLoader, SingletonABCMeta, SingletonMeta, TimestampHandler
from my_error_notifier import MyErrorNotifier


# TODO: send mail on high error rate
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
    """
    Get mailing template for node status "offline" and fills it with content
    """
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
    """
    Get mailing template for node status "no imports" and fills it with content
    """
    _FILENAME_TEMPLATE: str = 'template_mail_no_imports.html'

    def __init__(self, id_node: str):
        super().__init__()
        self.__DIR_ROOT = os.environ['ROOT_DIR']
        self.__ID_NODE = id_node
        self.__HANDLER = InfoCSVHandler()

    def get_mail_template_filled_with_information_from_template_page(self, page_template: str) -> MIMEText:
        soup = bs4.BeautifulSoup(page_template, self._PARSER)
        clinic_name = soup.find(class_='clinic_name').text
        last_write = soup.find(class_='last_write').text
        last_write = self.__get_last_import_date_from_csv() if last_write == '-' else last_write
        formatted_last_write = self._format_date_string_to_german_format(last_write)
        content = self._get_resource_as_string(self._FILENAME_TEMPLATE, self._ENCODING)
        content = content.replace('${clinic_name}', clinic_name)
        content = content.replace('${last_write}', formatted_last_write)
        mail = MIMEText(content, self._TEXT_SUBTYPE, self._ENCODING)
        mail['Subject'] = "Automatische Information: AKTIN DWH Keine Imports"
        return mail

    def __get_last_import_date_from_csv(self) -> str:
        path_csv = self.__get_csv_file_path()
        df = self.__HANDLER.read_csv_as_df(path_csv)
        series = df['last_write']
        list_idx = series[series == '-'].index
        series = series.drop(index=list_idx)
        return series.values[-1]

    def __get_csv_file_path(self) -> str:
        dir_working = os.path.join(self.__DIR_ROOT, self.__ID_NODE)
        name_csv = self.__HANDLER.generate_csv_name(self.__ID_NODE)
        path_csv = os.path.join(dir_working, name_csv)
        return path_csv


class OutdatedVersionMailTemplateHandler(MailTemplateHandler):
    """
    Get mailing template for node status "outdated version" and fills it with content
    """
    _FILENAME_TEMPLATE: str = 'template_mail_outdated_version.html'

    def __init__(self):
        super().__init__()
        self.__CURRENT_VERSION_DWH = os.environ['VERSION_DWH']
        self.__CURRENT_VERSION_I2B2 = os.environ['VERSION_I2B2']

    def get_mail_template_filled_with_information_from_template_page(self, page_template: str) -> MIMEText:
        soup = bs4.BeautifulSoup(page_template, self._PARSER)
        clinic_name = soup.find(class_='clinic_name').text
        version_dwh = soup.find(class_='dwh-j2ee').text
        content = self._get_resource_as_string(self._FILENAME_TEMPLATE, self._ENCODING)
        content = content.replace('${clinic_name}', clinic_name)
        content = content.replace('${version_dwh}', version_dwh)
        content = content.replace('${current_version_dwh}', self.__CURRENT_VERSION_DWH)
        content = content.replace('${current_version_i2b2}', self.__CURRENT_VERSION_I2B2)
        mail = MIMEText(content, self._TEXT_SUBTYPE, self._ENCODING)
        mail['Subject'] = "Automatische Information: AKTIN DWH Version veraltet"
        return mail


class ConfluencePageRecipientsExtractor(metaclass=SingletonMeta):
    """
    Extracts correspondants for broker node from another confluence page
    """
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
        Contacts can be blacklisted by setting a value in the appropriate column in Confluence.
        """
        list_contacts = []
        df_recipients = self.__get_recipients_df_for_node_id(id_node, 'Notaufnahme')
        df_main_contact = df_recipients[(df_recipients['Hauptansprechpartner?'] != '') & (
                    df_recipients['Abgemeldet von Monitor-Benachrichtigungen?'] == '')]
        series_contacts = df_main_contact['Kontakt']
        for contact in series_contacts:
            list_contacts.append(contact)
        return list_contacts

    def __extract_it_recipients_for_node_id(self, id_node: str) -> list:
        """
        For IT contacts, all contacts are used.
        Contacts can be blacklisted by setting a value in the appropriate column in Confluence.
        """
        list_contacts = []
        df_recipients = self.__get_recipients_df_for_node_id(id_node, 'IT')
        df_main_contact = df_recipients[df_recipients['Abgemeldet von Monitor-Benachrichtigungen?'] == '']
        series_contacts = df_main_contact['Kontakt']
        for contact in series_contacts:
            list_contacts.append(contact)
        return list_contacts

    def __get_recipients_df_for_node_id(self, id_node: str, type_recipients: str) -> pd.DataFrame:
        list_idx = self.__DF_EMAIL.index[self.__DF_EMAIL['Node ID'] == int(id_node)].tolist()
        df_node = self.__DF_EMAIL.iloc[list_idx]
        df_recipients = df_node[df_node['Ansprechpartner für'] == type_recipients]
        return df_recipients


class ConsecutiveSentEmailsCounter:
    """
    Checks when the last email was sent to node correspondants (to avoid notification spamming)
    """
    __ENCODING = 'utf-8'
    __DICT_TRACKING: dict = {}

    def __init__(self, filename: str):
        filename = filename.replace(' ', '_')
        self.__PATH_TRACKING_JSON = os.path.join(os.environ['ROOT_DIR'], "{}.json".format(filename))
        self.__TIMESTAMP_HANDLER = TimestampHandler()
        self.__MAPPER = ConfluenceNodeMapper()
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

    def create_or_update_node_entry(self, id_node: str):
        self.__DICT_TRACKING[id_node] = self.__TIMESTAMP_HANDLER.get_current_date()
        self.__save_tracking_json()

    def delete_entry_for_node_if_exists(self, id_node: str):
        if id_node in self.__DICT_TRACKING.keys():
            del self.__DICT_TRACKING[id_node]
            self.__save_tracking_json()

    def is_waiting_threshold_reached_for_node(self, id_node: str) -> bool:
        if id_node in self.__DICT_TRACKING.keys():
            threshold_weeks = self.__MAPPER.get_node_value_from_mapping_dict(id_node, 'WEEKS_NOTIFICATION_INTERVAL')
            if not threshold_weeks or threshold_weeks is None:
                threshold_weeks = 1
            last_sent = self.__DICT_TRACKING.get(id_node)
            current_date = self.__TIMESTAMP_HANDLER.get_current_date()
            delta = self.__TIMESTAMP_HANDLER.get_timedelta_in_absolute_hours(last_sent, current_date)
            delta_in_weeks = delta / 168
            return delta_in_weeks > threshold_weeks
        return True


class SentMailsLogger(metaclass=SingletonMeta):
    """
    Logs all sent mail to corresponding nodes (for traceability)
    """

    def __init__(self):
        self.__DIR_ROOT = os.environ['ROOT_DIR']
        self.__TIMESTAMP_HANDLER = TimestampHandler()

    def log_sent_mail_for_node(self, id_node: str, status: str):
        dir_working = self.__init_working_directory_if_nonexisting(id_node)
        path_log = self.__generate_mails_log_path(id_node, dir_working)
        with open(path_log, 'a') as log:
            current = self.__TIMESTAMP_HANDLER.get_current_date()
            log.write("{0} : Sent mail for status {1} to node id {2}\n".format(current, status, id_node))

    def __init_working_directory_if_nonexisting(self, name_folder: str) -> str:
        dir_working = os.path.join(self.__DIR_ROOT, name_folder)
        if not os.path.isdir(dir_working):
            os.makedirs(dir_working)
        return dir_working

    @staticmethod
    def __generate_mails_log_path(id_node: str, dir_working: str) -> str:
        name_file = '_'.join([id_node, 'sent_mails.log'])
        return os.path.join(dir_working, name_file)


class NotificationHandler(metaclass=SingletonABCMeta):
    _PARSER: str = 'html.parser'
    _MY_STATUS: str
    _TEMPLATE_HANDLER: MailTemplateHandler

    def __init__(self):
        self._CONFLUENCE_RECIPIENTS_EXTRACTOR = ConfluencePageRecipientsExtractor()
        self.__SENT_MAILS_LOGGER = SentMailsLogger()
        filename_tracking = '_'.join(['tracking', self._MY_STATUS.replace(' ', '_')])
        self._SENT_MAILS_COUNTER = ConsecutiveSentEmailsCounter(filename_tracking)
        self._MAIL_SENDER = MailSender()

    @abstractmethod
    def did_my_status_occur(self, page_template: str) -> bool:
        pass

    @abstractmethod
    def sent_my_mail_to_node(self, id_node: str, page_template: str):
        pass

    def log_my_sent_mail_to_node(self, id_node: str):
        self.__SENT_MAILS_LOGGER.log_sent_mail_for_node(id_node, self._MY_STATUS)

    def clean_my_status_for_node(self, id_node: str):
        self._SENT_MAILS_COUNTER.delete_entry_for_node_if_exists(id_node)

    def create_or_update_my_status_for_node(self, id_node: str):
        self._SENT_MAILS_COUNTER.create_or_update_node_entry(id_node)


class OfflineNotificationHandler(NotificationHandler):
    _MY_STATUS: str = 'OFFLINE'
    _TEMPLATE_HANDLER = OfflineMailTemplateHandler

    def __init__(self):
        super().__init__()
        self._TEMPLATE_HANDLER = OfflineMailTemplateHandler()

    def did_my_status_occur(self, page_template: str) -> bool:
        soup = bs4.BeautifulSoup(page_template, self._PARSER)
        element_status = soup.find(class_='status')
        status = element_status.find('ac:parameter', attrs={'ac:name': 'title'})
        if status.text == self._MY_STATUS:
            return True
        return False

    def sent_my_mail_to_node(self, id_node: str, page_template: str):
        mail = self._TEMPLATE_HANDLER.get_mail_template_filled_with_information_from_template_page(page_template)
        recipients = self._CONFLUENCE_RECIPIENTS_EXTRACTOR.extract_all_recipients_for_node_id(id_node)
        if recipients:
            if self._SENT_MAILS_COUNTER.is_waiting_threshold_reached_for_node(id_node):
                self._MAIL_SENDER.send_mail(recipients, mail)


class NoImportsNotificationHandler(NotificationHandler):
    _MY_STATUS: str = 'NO IMPORTS'
    _TEMPLATE_HANDLER: NoImportsMailTemplateHandler

    def did_my_status_occur(self, page_template: str) -> bool:
        soup = bs4.BeautifulSoup(page_template, self._PARSER)
        element_status = soup.find(class_='status')
        status = element_status.find('ac:parameter', attrs={'ac:name': 'title'})
        if status.text == self._MY_STATUS:
            return True
        return False

    def sent_my_mail_to_node(self, id_node: str, page_template: str):
        self._TEMPLATE_HANDLER = NoImportsMailTemplateHandler(id_node)
        mail = self._TEMPLATE_HANDLER.get_mail_template_filled_with_information_from_template_page(page_template)
        recipients = self._CONFLUENCE_RECIPIENTS_EXTRACTOR.extract_all_recipients_for_node_id(id_node)
        if recipients:
            if self._SENT_MAILS_COUNTER.is_waiting_threshold_reached_for_node(id_node):
                self._MAIL_SENDER.send_mail(recipients, mail)


class OutdatedVersionNotificationHandler(NotificationHandler):
    _MY_STATUS: str = 'DWH OUTDATED'
    _TEMPLATE_HANDLER = OutdatedVersionMailTemplateHandler

    def __init__(self):
        super().__init__()
        self._TEMPLATE_HANDLER = OutdatedVersionMailTemplateHandler()
        self.__CURRENT_VERSION_DWH = os.environ['VERSION_DWH']

    def did_my_status_occur(self, page_template: str) -> bool:
        soup = bs4.BeautifulSoup(page_template, self._PARSER)
        version_dwh = soup.find(class_='dwh-j2ee').text
        formatted_version = version_dwh.replace('dwh-j2ee-', '')
        if formatted_version and formatted_version != '-':
            return version.parse(self.__CURRENT_VERSION_DWH) > version.parse(formatted_version)
        return False

    def sent_my_mail_to_node(self, id_node: str, page_template: str):
        mail = self._TEMPLATE_HANDLER.get_mail_template_filled_with_information_from_template_page(page_template)
        recipients = self._CONFLUENCE_RECIPIENTS_EXTRACTOR.extract_all_recipients_for_node_id(id_node)
        if recipients:
            if self._SENT_MAILS_COUNTER.is_waiting_threshold_reached_for_node(id_node):
                self._MAIL_SENDER.send_mail(recipients, mail)


class NodeEventNotifierManager:

    def __init__(self):
        self.__CONFLUENCE = ConfluenceConnection()
        self.__MAPPER = ConfluenceNodeMapper()
        self.__OFFLINE_NOTIFIER = OfflineNotificationHandler()
        self.__NO_IMPORTS_NOTIFER = NoImportsNotificationHandler()
        self.__OUTDATED_VERSION_NOTIFIER = OutdatedVersionNotificationHandler()

    def notify_node_recipients_on_emergency_status(self):
        for id_node in self.__MAPPER.get_all_keys():
            name_page = self.__MAPPER.get_node_value_from_mapping_dict(id_node, 'COMMON_NAME')
            if self.__CONFLUENCE.does_page_exists(name_page):
                page = self.__CONFLUENCE.get_page_content(name_page)
                for notifier in (self.__OFFLINE_NOTIFIER, self.__NO_IMPORTS_NOTIFER, self.__OUTDATED_VERSION_NOTIFIER):
                    if notifier.did_my_status_occur(page):
                        notifier.sent_my_mail_to_node(id_node, page)
                        notifier.log_my_sent_mail_to_node(id_node)
                        notifier.create_or_update_my_status_for_node(id_node)
                    else:
                        notifier.clean_my_status_for_node(id_node)


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
        notifier = MyErrorNotifier(os.path.basename(__file__))
        notifier.notify_me(str(e))
    finally:
        logger.stop_logger()


if __name__ == '__main__':
    if len(sys.argv) == 1:
        raise SystemExit('path to config file is missing')
    if len(sys.argv) > 2:
        raise SystemExit('invalid number of input arguments')
    main(sys.argv[1])
