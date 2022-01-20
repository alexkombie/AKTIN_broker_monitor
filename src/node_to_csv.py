# -*- coding: utf-8 -*
# Created on Wed Jan 19 12:00 2022
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

import bisect
import json
import logging
import os
import sys
from dataclasses import dataclass
import traceback
from abc import ABC, abstractmethod
from bisect import bisect
from datetime import datetime

import lxml.etree as ET
import chardet
import pandas as pd
import requests
import sqlalchemy as db
from sqlalchemy import exc


class PropertiesJsonReader:
    REQUIRED_KEYS = {'BROKER_URL', 'ADMIN_API_KEY', 'ROOT_DIR'}

    def __init__(self, path):
        if not os.path.isfile(path):
            raise SystemExit('invalid config file path')
        with open(path) as file_json:
            dict_config = json.load(file_json)
        set_found_keys = set(dict_config.keys())
        set_matched_keys = self.REQUIRED_KEYS.intersection(set_found_keys)
        if set_matched_keys != self.REQUIRED_KEYS:
            raise SystemExit('following keys are missing in config file: {0}'.format(self.REQUIRED_KEYS.difference(set_matched_keys)))
        for key in self.REQUIRED_KEYS:
            os.environ[key] = dict_config.get(key)


# operations for reading and wirting csv files
# contains configuraiton variables for reading csv files
# gets the encoding of a csv file
class CSVHandler(ABC):
    CSV_SEPARATOR: str = ';'
    CSV_NAME: str

    def __init__(self, path_folder: str):
        self.PATH_CSV = os.path.join(path_folder, self.CSV_NAME)

    @staticmethod
    def get_csv_encoding(path_csv: str) -> str:
        with open(path_csv, 'rb') as csv:
            encoding = chardet.detect(csv.read(5120))['encoding']
        return encoding

    # autocloseable?


class SingletonMeta(type):
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

    def __check_broker_server_availability(self) -> None:
        url = self.__append_to_broker_url('broker', 'status')
        response = requests.head(url)
        if response.status_code != 200:
            raise ConnectionError('Could not connect to AKTIN Broker')

    def __append_to_broker_url(self, *items: str) -> str:
        url = self.__BROKER_URL
        for item in items:
            url = '{}/{}'.format(url, item)
        return url

    def __create_basic_header(self) -> dict:
        return {'Authorization': ' '.join(['Bearer', self.__ADMIN_API_KEY]), 'Connection': 'keep-alive', 'Accept': 'application/xml'}

    @staticmethod
    def __remove_namespace_from_tree(tree: ET._ElementTree) -> ET._ElementTree:
        for elem in tree.getiterator():
            if not hasattr(elem.tag, 'find'):
                continue
            i = elem.tag.find('}')
            if i >= 0:
                elem.tag = elem.tag[i + 1:]
        return tree

    def __get_processed_response(self, url: str) -> ET._ElementTree:
        response = requests.get(url, headers=self.__create_basic_header())
        response.raise_for_status()
        tree = ET.fromstring(response.content)
        return self.__remove_namespace_from_tree(tree)

    def get_broker_node_list(self) -> list[str]:
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
                tree.find('last-write').text,
                tree.find('last-reject').text,
                tree.find('imported').text,
                tree.find('updated').text,
                tree.find('invalid').text,
                tree.find('failed').text)
        return stats

    def get_broker_node_error(self, id_node: str):
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

    @dataclass()
    class BrokerNode:

        __ID: str
        __CERT: str
        __LAST_CONTACT: str

        @property
        def id(self) -> str:
            return self.__ID

        @property
        def cert(self) -> str:
            return self.__CERT

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


# ordner : 003
# utf-8
# csv file : 003_stats_2022-01.csv
# error file : 003_errors_2022-01.csv

# get information of broker nodes and saves them in a csv file
# each node hsa a corresponding csv file
# csv files are rotated each month
# all csv files of a node are collected in a corresponding folder
class BrokerNodeFetcherFactory:

    # hjole alle ids
    # create folder for each id
    # create info und error fetcher for each folder
    # fetcher braucht id und pfad zu csv
    # factory erschafft csv-namen

    def __init__(self):
        #   self._NODE_ID = id_node.rjust(3, '0')
        #   self._DIR_WORKING = self.__init_working_dir_for_fetcher()
        self.__LIST_NODE_IDS = BrokerNodeConnection().get_broker_node_list()
        self.__CURRENT_DATE = self.__get_current_date()

    def create_info_fetcher(self, id_node: str):
        id_node = id_node.rjust(3, '0')
        dir_node = self.__init_working_dir_for_node(id_node)

    @staticmethod
    def __init_working_dir_for_node(id_node: str) -> str:
        dir_root = os.environ['ROOT_DIR']
        dir_working = os.path.join(dir_root, id_node)
        if not os.path.isdir(dir_working):
            os.makedirs(dir_working)
        return dir_working

    @staticmethod
    def __get_current_date():
        month_current = str(datetime.now().month)
        month_current = month_current.rjust(2, '0')
        return '-'.join([datetime.now().year, month_current])


# fetchers broker node contact and startup
# fetcher broker node import stats
# computes daily imports from differecnes to previous days
# computes daily error rate
# each column in dsv file stands for one day
class NodeInfoFetcher:

    def __int__(self):
        self.__ID_NODE = ''
        self.__CURRENT_DATE = ''
        self.__PATH_CSV = ''
        self.__BROKER_NODE_CONNECTION = BrokerNodeConnection()

    def init_new_csv(self):
        if not os.path.isfile(self.__PATH_CSV):
            df = pd.DataFrame(columns=['date', 'last-contact', 'start', 'last-write',
                                       'last-reject', 'imported', 'updated', 'invalid',
                                       'failed', 'daily_imports', 'daily_updates', 'daily_invalids',
                                       'daily_failed', 'error_rate'])
            df.to_csv(self.__PATH_CSV, index=False, sep=';', encoding='UTF-8')

    def add_column_to_csv(self):
        df = pd.read_csv(self.__PATH_CSV, sep=';', encoding='UTF-8', dtype=str)
        node = self.__BROKER_NODE_CONNECTION.get_broker_node(self.__ID_NODE)
        stats = self.__BROKER_NODE_CONNECTION.get_broker_node_stats(self.__ID_NODE)

        new_row = {'date':           self.__CURRENT_DATE,
                   'last-contact':   node.last_contact,
                   'start':          stats.dwh_start,
                   'last-write':     stats.last_write,
                   'last-reject':    stats.last_reject,
                   'imported':       stats.imported,
                   'updated':        stats.updated,
                   'invalid':        stats.invalid,
                   'failed':         stats.failed,
                   'daily_imports':  stats.daily_imports,
                   'daily_updates':  stats.daily_updates,
                   'daily_invalids': stats.daily_invalids,
                   'daily_failed':   stats.daily_failed,
                   'error_rate':     stats.error_rate,
                   }

        df = df.append(new_row, ignore_index=True)
        df.to_csv(self.__PATH_CSV, index=False, sep=';', encoding='UTF-8')


# fetchers broker node errors
# each clumn in csv file stands for one error
class NodeErrorFetcher:
    pass


def __init_logger():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s',
                        handlers=[logging.StreamHandler()])


def __stop_logger():
    [logging.root.removeHandler(handler) for handler in logging.root.handlers[:]]
    logging.shutdown()


def main(path_config: str):
    try:
        __init_logger()
        PropertiesJsonReader(path_config)
        a = BrokerNodeFetcher('1')
        b = BrokerNodeFetcher('2')
        c = BrokerNodeFetcher('3')
        d = BrokerNodeFetcher('4')

        print(id(a.BROKER_NODE_CONNECTION))
        print(id(b.BROKER_NODE_CONNECTION))
        print(id(c.BROKER_NODE_CONNECTION))
        print(id(d.BROKER_NODE_CONNECTION))

    except Exception as e:
        logging.exception(e)
    finally:
        __stop_logger()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise SystemExit('please give path to config file')
    main(sys.argv[1])
