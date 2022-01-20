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
from datetime import timedelta

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


class CSVManager:
    __CSV_SEPARATOR: str = ';'
    __CSV_ENCODING: str = 'UTF-8'

    def __init__(self, path_csv: str):
        self.__PATH_CSV = path_csv

    def save_df_as_csv(self, df: pd.DataFrame):
        df.to_csv(self.__PATH_CSV, sep=self.__CSV_SEPARATOR, encoding=self.__CSV_ENCODING, index=False)

    def read_csv_as_df(self) -> pd.DataFrame:
        return pd.read_csv(self.__PATH_CSV, sep=self.__CSV_SEPARATOR, encoding=self.__CSV_ENCODING, dtype=str)


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

    def __get_processed_response(self, url: str) -> ET._ElementTree:
        response = requests.get(url, headers=self.__create_basic_header())
        response.raise_for_status()
        tree = ET.fromstring(response.content)
        return self.__remove_namespace_from_tree(tree)

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
# csv file : 003_stats_2022.csv
# error file : 003_errors_2022.csv

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


class NodeInfoFetcher:

    def __init__(self, dir_working: str, id_node: str):
        self.__ID_NODE = id_node
        self.__DIR_WORKING = dir_working
        self.__CURRENT_DATE = datetime.now()
        self.__NAME_CSV = self.__generate_csv_name(self.__ID_NODE, self.__CURRENT_DATE)
        self.__PATH_CSV = os.path.join(self.__DIR_WORKING, self.__NAME_CSV)
        self.__CSV_MANAGER = CSVManager(self.__PATH_CSV)
        self.__BROKER_NODE_CONNECTION = BrokerNodeConnection()

    @staticmethod
    def __generate_csv_name(id_node: str, date: datetime):
        id_node = id_node.rjust(3, '0')
        name_csv = '_'.join([id_node, 'stats', str(date.year)])
        return ''.join([name_csv, '.csv'])

    def init_new_csv(self):
        if not os.path.isfile(self.__PATH_CSV):
            df = pd.DataFrame(columns=['date', 'last_contact', 'start', 'last_write', 'last_reject',
                                       'imported', 'updated', 'invalid', 'failed',
                                       'daily_imported', 'daily_updated', 'daily_invalid', 'daily_failed', 'error_rate'])
            self.__CSV_MANAGER.save_df_as_csv(df)

    def fetch_to_csv(self):
        row_reference = pd.Series(dtype=str)
        node = self.__BROKER_NODE_CONNECTION.get_broker_node(self.__ID_NODE)
        stats = self.__BROKER_NODE_CONNECTION.get_broker_node_stats(self.__ID_NODE)
        df = self.__CSV_MANAGER.read_csv_as_df()
        df = self.__delete_todays_row_if_exists(df)
        if df.empty:
            path_csv_last_year = self.__get_last_years_csv_path()
            if os.path.isfile(path_csv_last_year):
                csv_manager = CSVManager(path_csv_last_year)
                df_last_year = csv_manager.read_csv_as_df()
                last_row = df_last_year.iloc[-1]
                if self.__check_end_of_year_transition(last_row.date):
                    row_reference = last_row
        else:
            row_reference = df.iloc[-1]
        if row_reference.empty:
            map_daily = self.__generate_empty_daily_stats()
        else:
            map_daily = self.__compute_daily_stats(row_reference, stats)
        new_row = {'date':         self.__extract_year_month_day(self.__CURRENT_DATE),
                   'last_contact': node.last_contact,
                   'start':        stats.dwh_start,
                   'last_write':   stats.last_write,
                   'last_reject':  stats.last_reject,
                   'imported':     stats.imported,
                   'updated':      stats.updated,
                   'invalid':      stats.invalid,
                   'failed':       stats.failed}
        new_row.update(map_daily)
        df = df.append(new_row, ignore_index=True)
        self.__CSV_MANAGER.save_df_as_csv(df)

    def __delete_todays_row_if_exists(self, df: pd.DataFrame) -> pd.DataFrame:
        if not df.empty:
            current_date = self.__extract_year_month_day(self.__CURRENT_DATE)
            last_row = df.iloc[-1]
            if last_row.date == current_date:
                df = df.head(-1)
            if any(df['date'] == current_date):
                raise SystemExit('Date of today was found in past!!')
        return df

    @staticmethod
    def __extract_year_month_day(date: datetime):
        month = str(date.month)
        month = month.rjust(2, '0')
        day = str(date.day)
        day = day.rjust(2, '0')
        return '-'.join([str(date.year), month, day])

    def __get_last_years_csv_path(self):
        date_last_year = self.__CURRENT_DATE.replace(year=self.__CURRENT_DATE.year - 1)
        name_csv = self.__generate_csv_name(self.__ID_NODE, date_last_year)
        return os.path.join(self.__DIR_WORKING, name_csv)

    def __check_end_of_year_transition(self, date_end_of_year: str) -> bool:
        date_yesterday = self.__extract_year_month_day(self.__CURRENT_DATE - timedelta(days=1))
        return date_end_of_year == date_yesterday

    @staticmethod
    def __generate_empty_daily_stats() -> dict:
        return {'daily_imported': '-',
                'daily_updated':  '-',
                'daily_invalid':  '-',
                'daily_failed':   '-',
                'error_rate':     '-'}

    @staticmethod
    def __compute_daily_stats(stats_previous, stats_current) -> dict:
        daily_imported = int(stats_current.imported) - int(stats_previous.imported)
        daily_updated = int(stats_current.updated) - int(stats_previous.updated)
        daily_invalid = int(stats_current.invalid) - int(stats_previous.invalid)
        daily_failed = int(stats_current.failed) - int(stats_previous.failed)
        if (daily_imported + daily_updated) > 0:
            error_rate = (daily_failed + daily_invalid) / (daily_imported + daily_updated)
            error_rate = round(error_rate, 2)
        else:
            error_rate = '-'
        return {'daily_imported': daily_imported,
                'daily_updated':  daily_updated,
                'daily_invalid':  daily_invalid,
                'daily_failed':   daily_failed,
                'error_rate':     error_rate}


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
        a = NodeInfoFetcher('', '1')
        a.init_new_csv()
        a.add_column_to_csv()

    except Exception as e:
        logging.exception(e)
    finally:
        __stop_logger()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise SystemExit('please give path to config file')
    main(sys.argv[1])
