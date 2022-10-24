# -*- coding: utf-8 -*
# Created on Tue Feb 15 12:00 2022
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
import smtplib
from abc import ABC, ABCMeta, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from smtplib import SMTP_SSL as SMTP

import lxml.etree as ET
import pandas as pd
import requests
from atlassian import Confluence
from dateutil import parser
from pytz import timezone


class SingletonMeta(type):
    """
    Meta class to make python classes a Singleton
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class SingletonABCMeta(ABCMeta):
    """
    Meta class to make abstract python classes a Singleton -> All implementing classes
    are automatically Singletons
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonABCMeta, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class CSVHandler(ABC, metaclass=SingletonABCMeta):
    """
    Operations for reading a CSV file as a dataframe or writing a dataframe to CSV
    """
    _CSV_CATEGORY: str
    __CSV_SEPARATOR: str = ';'
    __CSV_ENCODING: str = 'UTF-8'
    __TIMEZONE: str = 'Europe/Berlin'

    def save_df_to_csv(self, df: pd.DataFrame, path_csv: str):
        df.to_csv(path_csv, sep=self.__CSV_SEPARATOR, encoding=self.__CSV_ENCODING, index=False)

    def read_csv_as_df(self, path_csv: str) -> pd.DataFrame:
        return pd.read_csv(path_csv, sep=self.__CSV_SEPARATOR, encoding=self.__CSV_ENCODING, dtype=str)

    def generate_csv_name(self, id_node: str) -> str:
        current_year = str(pd.Timestamp.now().tz_localize(self.__TIMEZONE).year)
        name_csv = '_'.join([id_node, self._CSV_CATEGORY, current_year])
        return ''.join([name_csv, '.csv'])

    def generate_csv_name_with_custom_year(self, id_node: str, year: str) -> str:
        name_csv = '_'.join([id_node, self._CSV_CATEGORY, year])
        return ''.join([name_csv, '.csv'])

    def init_csv_file_if_not_exists(self, path_csv: str, name_csv: str = None) -> str:
        if name_csv:
            path_csv = os.path.join(path_csv, name_csv)
        if not os.path.isfile(path_csv):
            df = pd.DataFrame(columns=self.get_csv_columns())
            self.save_df_to_csv(df, path_csv)
        return path_csv

    @abstractmethod
    def get_csv_columns(self) -> list:
        pass


class InfoCSVHandler(CSVHandler):
    """
    Implementation for stats CSV. Writes CSV with import and connection information
    of a single connected node
    """
    _CSV_CATEGORY = 'stats'

    def get_csv_columns(self) -> list:
        return ['date', 'last_contact', 'last_start', 'last_write', 'last_reject',
                'imported', 'updated', 'invalid', 'failed', 'error_rate',
                'daily_imported', 'daily_updated', 'daily_invalid', 'daily_failed', 'daily_error_rate']


class ErrorCSVHandler(CSVHandler):
    """
    Implementation for error CSV. Writes CSV with import errors of a single connected
    node
    """
    _CSV_CATEGORY = 'errors'

    def get_csv_columns(self) -> list:
        return ['timestamp', 'repeats', 'content']


class TimestampHandler(metaclass=SingletonMeta):
    """
    Handles everything regarding dates and timestamps
    """

    def __init__(self):
        self.__TIMEZONE = timezone('Europe/Berlin')

    def get_current_date(self) -> str:
        return str(datetime.now(self.__TIMEZONE))

    def get_yesterdays_date(self) -> str:
        date = datetime.now(self.__TIMEZONE)
        date = date - timedelta(days=1)
        return str(date)

    def get_current_year(self) -> str:
        date = datetime.now(self.__TIMEZONE)
        return str(date.year)

    @staticmethod
    def get_year_from_date_string(date: str) -> str:
        d = parser.parse(date)
        return str(d.year)

    @staticmethod
    def get_YMD_from_date_string(date: str) -> str:
        d = parser.parse(date)
        return d.strftime('%Y-%m-%d')

    @staticmethod
    def get_YMD_HMS_from_date_string(date: str) -> str:
        d = parser.parse(date)
        return d.strftime('%Y-%m-%d %H:%M:%S')

    def get_timedelta_in_absolute_hours(self, date1: str, date2: str) -> int:
        """
        Timezone information is ignored beacuse of possible inconsistencies
        of inputs (for example: date1 has timezone information, date2 has none)
        """
        d1 = parser.parse(self.get_YMD_HMS_from_date_string(date1))
        d2 = parser.parse(self.get_YMD_HMS_from_date_string(date2))
        delta = d2 - d1
        return abs(delta.total_seconds() // 3600)

    def convert_utc_to_local_date_string(self, date: str) -> str:
        d = parser.parse(date).astimezone(self.__TIMEZONE)
        return str(d)


class BrokerNodeConnection(metaclass=SingletonMeta):
    """
    Uses REST endpoint of broker-server to get information about
    connected nodes
    """

    def __init__(self):
        self.__BROKER_URL = os.environ['BROKER_URL']
        self.__ADMIN_API_KEY = os.environ['ADMIN_API_KEY']
        self.__check_broker_server_availability()

    def __check_broker_server_availability(self):
        url = self.__append_to_broker_url('broker', 'status')
        response = requests.head(url)
        response.raise_for_status()

    def __append_to_broker_url(self, *items: str) -> str:
        url = self.__BROKER_URL
        for item in items:
            url = '{}/{}'.format(url, item)
        return url

    def get_broker_nodes(self) -> list:
        url = self.__append_to_broker_url('broker', 'node')
        tree = self.__get_processed_response(url)
        return [node.find('id').text for node in tree.getchildren()]

    # TODO: How to set inner class as return type?
    def get_broker_node(self, id_node: str):
        url = self.__append_to_broker_url('broker', 'node', id_node)
        tree = self.__get_processed_response(url)
        node = self.BrokerNode(
                id_node,
                tree.find('clientDN').text,
                tree.find('last-contact').text)
        return node

    # TODO: How to set inner class as return type?
    def get_broker_node_stats(self, id_node: str):
        url = self.__append_to_broker_url('broker', 'node', id_node, 'stats')
        tree = self.__get_processed_response(url)
        stats = self.BrokerNodeStats(
                tree.find('start').text,
                tree.find('last-write').text if tree.find('last-write') is not None else None,
                tree.find('last-reject').text if tree.find('last-reject') is not None else None,
                tree.find('imported').text,
                tree.find('updated').text,
                tree.find('invalid').text,
                tree.find('failed').text)
        return stats

    def get_broker_node_errors(self, id_node: str) -> list:
        url = self.__append_to_broker_url('broker', 'node', id_node, 'stats')
        tree = self.__get_processed_response(url)
        errors = []
        for elem in tree.find('last-errors').getchildren():
            error = self.BrokerNodeError(
                    elem.get('repeats'),
                    elem.get('timestamp'),
                    elem.text)
            errors.append(error)
        return errors

    def get_broker_node_resource(self, id_node: str, resource: str) -> dict:
        """
        Possible resources are 'versions', 'rscript', 'python', 'import-scripts'.
        URL to mentioned resources can also be non-existing.
        """
        url = self.__append_to_broker_url('broker', 'node', id_node, resource)
        try:
            tree = self.__get_processed_response(url)
        except requests.exceptions.HTTPError:
            return {}
        resources = {}
        for elem in tree.findall('entry'):
            resources[elem.get('key')] = elem.text
        return resources

    def __get_processed_response(self, url: str) -> ET.ElementTree:
        """
        Returns processed XML tree object without namespace from GET request
        """
        response = requests.get(url, headers=self.__create_basic_headers())
        response.raise_for_status()
        tree = ET.fromstring(response.content)
        return self.__remove_namespace_from_tree(tree)

    def __create_basic_headers(self) -> dict:
        """
        HTTP header for requests to AKTIN Broker
        """
        headers = requests.utils.default_headers()
        headers['Authorization'] = ' '.join(['Bearer', self.__ADMIN_API_KEY])
        headers['User-Agent'] = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'
        headers['Accept'] = 'application/xml'
        return headers

    @staticmethod
    def __remove_namespace_from_tree(tree: ET.ElementTree) -> ET.ElementTree:
        """
        To enable search/processing via xpath
        """
        for elem in tree.getiterator():
            if not hasattr(elem.tag, 'find'):
                continue
            i = elem.tag.find('}')
            if i >= 0:
                elem.tag = elem.tag[i + 1:]
        return tree

    @dataclass()
    class BrokerNode:

        __ID: str
        __DOMAIN_NAME: str
        __LAST_CONTACT: str

        @property
        def id(self) -> str:
            return self.__ID

        @property
        def domain_name(self) -> str:
            return self.__DOMAIN_NAME

        @property
        def last_contact(self) -> str:
            return self.__LAST_CONTACT

    @dataclass()
    class BrokerNodeStats:

        __LAST_START: str
        __LAST_WRITE: str
        __LAST_REJECT: str
        __IMPORTED: str
        __UPDATED: str
        __INVALID: str
        __FAILED: str

        @property
        def dwh_start(self) -> str:
            return self.__LAST_START

        @property
        def last_write(self) -> str:
            return self.__LAST_WRITE

        @property
        def last_reject(self) -> str:
            return self.__LAST_REJECT

        @property
        def imported(self) -> str:
            return self.__IMPORTED

        @property
        def updated(self) -> str:
            return self.__UPDATED

        @property
        def invalid(self) -> str:
            return self.__INVALID

        @property
        def failed(self) -> str:
            return self.__FAILED

    @dataclass()
    class BrokerNodeError:

        __REPEATS: str
        __TIMESTAMP: str
        __CONTENT: str

        @property
        def repeats(self) -> str:
            return self.__REPEATS

        @property
        def timestamp(self) -> str:
            return self.__TIMESTAMP

        @property
        def content(self) -> str:
            return self.__CONTENT


class ResourceLoader(ABC, metaclass=SingletonABCMeta):
    """
    For everything inside the /resources folder
    """

    def __init__(self):
        self.__DIR_RESOURCES = os.environ['RESOURCES_DIR']

    def _get_resource_as_string(self, name_resource: str, encoding: str) -> str:
        path_resource = os.path.join(self.__DIR_RESOURCES, name_resource)
        with open(path_resource, 'r', encoding=encoding) as resource:
            content = resource.read()
        return content


class ConfluenceConnection(metaclass=SingletonMeta):
    """
    Uses Atlassian Python API to execute CRUD operations on Confluence
    """

    def __init__(self):
        """
        Confluence connection is created on initialization
        """
        confluence_url = os.environ['CONFLUENCE_URL']
        confluence_token = os.environ['CONFLUENCE_TOKEN']
        self.__SPACE = os.environ['CONFLUENCE_SPACE']
        self.__CONFLUENCE = Confluence(url=confluence_url, token=confluence_token)

    def does_page_exists(self, name_page: str) -> bool:
        return self.__CONFLUENCE.page_exists(self.__SPACE, name_page)

    def get_page_content(self, name_page: str) -> str:
        id_page = self.__CONFLUENCE.get_page_id(self.__SPACE, name_page)
        page = self.__CONFLUENCE.get_page_by_id(id_page, expand='body.storage')
        content = page['body']['storage']['value']
        return content

    def upload_csv_as_attachement_to_page(self, name_page: str, path_csv: str):
        """
        Identical named files are automatically replaced on confluence
        """
        id_page = self.__CONFLUENCE.get_page_id(self.__SPACE, name_page)
        self.__CONFLUENCE.attach_file(path_csv, content_type='text/csv', page_id=id_page)

    def create_confluence_page(self, name_page: str, name_parent: str, content: str):
        id_parent = self.__CONFLUENCE.get_page_id(self.__SPACE, name_parent)
        self.__CONFLUENCE.create_page(self.__SPACE, name_page, content, parent_id=id_parent)

    def update_confluence_page(self, name_page: str, content: str):
        id_page = self.__CONFLUENCE.get_page_id(self.__SPACE, name_page)
        self.__CONFLUENCE.update_page(id_page, name_page, content)


class ConfluenceNodeMapper(metaclass=SingletonMeta):
    """
    Maps id of broker node to json file with information related to confluence page
    like institution name, jira query labels and so on
    """

    def __init__(self):
        self.__DICT_MAPPING = self.__load_json_file_as_dict(os.environ['CONFLUENCE_MAPPING_JSON'])

    @staticmethod
    def __load_json_file_as_dict(path_file: str) -> dict:
        with open(path_file, encoding='utf-8') as json_file:
            dict_mapping = json.load(json_file)
        return dict_mapping

    def get_all_keys(self) -> list:
        return self.__DICT_MAPPING.keys()

    def get_node_from_mapping_dict(self, node: str) -> dict:
        if node in self.__DICT_MAPPING:
            return self.__DICT_MAPPING[node]
        else:
            return None

    def get_node_value_from_mapping_dict(self, node: str, key: str) -> str:
        if key in self.__DICT_MAPPING[node]:
            return self.__DICT_MAPPING[node][key]
        else:
            return None


class MailServerConnection(metaclass=SingletonABCMeta):
    """
    Creates connection with extern mail server
    """
    _CONNECTION: smtplib.SMTP_SSL = None

    def __init__(self):
        self._USER = os.environ['EMAIL_USER']
        self.__HOST = os.environ['EMAIL_HOST']
        self.__PASSWORD = os.environ['EMAIL_PASSWORD']

    def _connect(self):
        self._CONNECTION = SMTP(self.__HOST)
        self._CONNECTION.login(self._USER, self.__PASSWORD)

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
        mail['From'] = self._USER
        mail['To'] = ', '.join(list_receiver)
        self._CONNECTION.sendmail(self._USER, list_receiver, mail.as_string())


class MyLogger(metaclass=SingletonMeta):
    """
    This class should be called by every other script on startup!
    """

    @staticmethod
    def init_logger():
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(levelname)s %(message)s',
                            handlers=[logging.StreamHandler()])

    @staticmethod
    def stop_logger():
        [logging.root.removeHandler(handler) for handler in logging.root.handlers[:]]
        logging.shutdown()


class PropertiesReader(metaclass=SingletonMeta):
    """
    This class should be called by every other script on startup!
    Checks given settings file to include required keys and loads key-values as
    environment variables after validation. The environment variables are assumed
    by the classes of other scripts
    """
    __SET_REQUIRED_KEYS = {'BROKER_URL',
                           'ADMIN_API_KEY',
                           'ROOT_DIR',
                           'RESOURCES_DIR',
                           'CONFLUENCE_URL',
                           'CONFLUENCE_SPACE',
                           'CONFLUENCE_TOKEN',
                           'CONFLUENCE_MAPPING_JSON',
                           'EMAIL_HOST',
                           'EMAIL_USER',
                           'EMAIL_PASSWORD',
                           'VERSION_DWH',
                           'VERSION_I2B2'
                           }

    def load_properties_as_env_vars(self, path: str):
        properties = self.__load_properties_file(path)
        self.__validate_properties(properties)
        for key in self.__SET_REQUIRED_KEYS:
            os.environ[key] = properties.get(key)

    @staticmethod
    def __load_properties_file(path: str) -> dict:
        if not os.path.isfile(path):
            raise SystemExit('invalid config file path')
        with open(path) as file_json:
            return json.load(file_json)

    def __validate_properties(self, properties: dict):
        set_found_keys = set(properties.keys())
        set_matched_keys = self.__SET_REQUIRED_KEYS.intersection(set_found_keys)
        if set_matched_keys != self.__SET_REQUIRED_KEYS:
            raise SystemExit('following keys are missing in config file: {0}'.format(self.__SET_REQUIRED_KEYS.difference(set_matched_keys)))
