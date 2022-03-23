# -*- coding: utf-8 -*
# Created on Tue Feb 15 12:00 2022
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
from dataclasses import dataclass
from atlassian import Confluence

import lxml.etree as ET
import pandas as pd
import requests


class CSVHandler:
    __CSV_SEPARATOR: str = ';'
    __CSV_ENCODING: str = 'UTF-8'

    def __init__(self, path_csv: str):
        self.__PATH_CSV = path_csv

    def save_df_as_csv(self, df: pd.DataFrame):
        df.to_csv(self.__PATH_CSV, sep=self.__CSV_SEPARATOR, encoding=self.__CSV_ENCODING, index=False)

    def read_csv_as_df(self) -> pd.DataFrame:
        return pd.read_csv(self.__PATH_CSV, sep=self.__CSV_SEPARATOR, encoding=self.__CSV_ENCODING, dtype=str)


class SingletonMeta(type):
    """
    Meta class to make python classes a Singleton
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class BrokerNodeConnection(metaclass=SingletonMeta):

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

    def get_broker_nodes_list(self) -> list[str]:
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

    def get_broker_node_stats(self, id_node: str):
        url = self.__append_to_broker_url('broker', 'node', id_node, 'stats')
        tree = self.__get_processed_response(url)
        stats = self.BrokerNodeStats(
                tree.find('start').text,
                tree.find('last-write').text if tree.find('last-write') is not None else '',
                tree.find('last-reject').text if tree.find('last-reject') is not None else '',
                tree.find('imported').text,
                tree.find('updated').text,
                tree.find('invalid').text,
                tree.find('failed').text)
        return stats

    def get_broker_node_errors(self, id_node: str):
        url = self.__append_to_broker_url('broker', 'node', id_node, 'stats')
        tree = self.__get_processed_response(url)
        errors = []
        for elem in tree.find('last-errors').getchildren():
            error = self.BrokerNodeError(
                    elem.get('timestamp'),
                    elem.get('repeats') if elem.get('repeats') is not None else '',
                    elem.text)
            errors.append(error)
        return errors

    def get_broker_node_resource(self, id_node: str, resource: str) -> dict:
        """
        Possible resources are 'versions', 'rscript', 'python', 'import-scripts'
        """
        url = self.__append_to_broker_url('broker', 'node', id_node, resource)
        tree = self.__get_processed_response(url)
        resources = {}
        for elem in tree.findall('entry'):
            resources[elem.get('key')] = elem.text
        return resources

    def __get_processed_response(self, url: str) -> ET._ElementTree:
        """
        Returns processed XML tree object without namespace from GET request
        """
        response = requests.get(url, headers=self.__create_basic_header())
        response.raise_for_status()
        tree = ET.fromstring(response.content)
        return self.__remove_namespace_from_tree(tree)

    def __create_basic_header(self) -> dict:
        """
        HTTP header for requests to AKTIN Broker
        """
        return {'Authorization': ' '.join(['Bearer', self.__ADMIN_API_KEY]), 'Connection': 'keep-alive', 'Accept': 'application/xml'}

    @staticmethod
    def __remove_namespace_from_tree(tree: ET._ElementTree) -> ET._ElementTree:
        """
        To enable search via xpath
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

        __DWH_START: str
        __LAST_WRITE: str
        __LAST_REJECT: str
        __IMPORTED: str
        __UPDATED: str
        __INVALID: str
        __FAILED: str

        @property
        def dwh_start(self) -> str:
            return self.__DWH_START

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

        __TIMESTAMP: str
        __REPEATS: str
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


class ConfluenceConnection(metaclass=SingletonMeta):

    def __init__(self):
        confluence_url = os.environ['CONFLUENCE_URL']
        confluence_token = os.environ['CONFLUENCE_TOKEN']
        self.__SPACE = os.environ['CONFLUENCE_SPACE']
        self.__CONFLUENCE = Confluence(url=confluence_url, token=confluence_token)

    def check_page_existence(self, name_page: str) -> bool:
        return self.__CONFLUENCE.page_exists(self.__SPACE, name_page)

    def get_page_content(self, name_page: str) -> str:
        id_page = self.__CONFLUENCE.get_page_id(self.__SPACE, name_page)
        page = self.__CONFLUENCE.get_page_by_id(id_page, expand='body.storage')
        content = page['body']['storage']['value']
        return content

    def upload_csv_as_attachement_to_page(self, name_page: str, path_csv: str):
        """
        Identical files are automatically replaced on confluence
        """
        id_page = self.__CONFLUENCE.get_page_id(self.__SPACE, name_page)
        self.__CONFLUENCE.attach_file(path_csv, content_type='text/csv', page_id=id_page)

    def create_confluence_page(self, name_page: str, name_parent: str, content: str):
        id_parent = self.__CONFLUENCE.get_page_id(self.__SPACE, name_parent)
        self.__CONFLUENCE.create_page(self.__SPACE, name_page, content, parent_id=id_parent)

    def update_confluence_page(self, name_page: str, content: str):
        id_page = self.__CONFLUENCE.get_page_id(self.__SPACE, name_page)
        self.__CONFLUENCE.update_page(id_page, name_page, content)


def __init_logger():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s',
                        handlers=[logging.StreamHandler()])


def __stop_logger():
    [logging.root.removeHandler(handler) for handler in logging.root.handlers[:]]
    logging.shutdown()


def load_properties_file_as_environment(path: str):
    set_required_keys = {'BROKER_URL', 'ADMIN_API_KEY', 'ROOT_DIR', 'CONFLUENCE_RESOURCES_DIR', 'CONFLUENCE_URL',
                         'CONFLUENCE_SPACE', 'CONFLUENCE_TOKEN', 'CONFLUENCE_MAPPING_JSON', 'CONFLUENCE_PARENT_PAGE'}
    if not os.path.isfile(path):
        raise SystemExit('invalid config file path')
    with open(path) as file_json:
        dict_config = json.load(file_json)
    set_found_keys = set(dict_config.keys())
    set_matched_keys = set_required_keys.intersection(set_found_keys)
    if set_matched_keys != set_required_keys:
        raise SystemExit('following keys are missing in config file: {0}'.format(set_required_keys.difference(set_matched_keys)))
    for key in set_required_keys:
        os.environ[key] = dict_config.get(key)


class ConfluenceNodeMapper(metaclass=SingletonMeta):

    def __init__(self):
        self.__DICT_MAPPING = self.__load_json_file_as_dict(os.environ['CONFLUENCE_MAPPING_JSON'])

    @staticmethod
    def __load_json_file_as_dict(path_file: str) -> dict:
        with open(path_file) as json_file:
            dict_mapping = json.load(json_file)
        return dict_mapping

    def get_mapping_dict(self) -> dict:
        return self.__DICT_MAPPING

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
