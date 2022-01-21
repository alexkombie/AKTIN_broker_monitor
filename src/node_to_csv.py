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

import json
import logging
import os
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta

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


class BrokerNodeFetcher(ABC):
    _CSV_CATEGORY: str
    _CSV_COLUMNS: list[str]

    def __init__(self, id_node: str, dir_working=''):
        self._ID_NODE = id_node
        self._DIR_WORKING = dir_working
        self._CURRENT_DATE = datetime.now()
        self._NAME_CSV = self._generate_csv_name(self._ID_NODE, self._CURRENT_DATE)
        self._PATH_CSV = os.path.join(self._DIR_WORKING, self._NAME_CSV)
        self._CSV_HANDLER = CSVHandler(self._PATH_CSV)
        self._BROKER_NODE_CONNECTION = BrokerNodeConnection()

    def _generate_csv_name(self, id_node: str, date: datetime):
        id_node = id_node.rjust(3, '0')
        name_csv = '_'.join([id_node, self._CSV_CATEGORY, str(date.year)])
        return ''.join([name_csv, '.csv'])

    def init_working_csv(self):
        if not os.path.isfile(self._PATH_CSV):
            df = pd.DataFrame(columns=self._CSV_COLUMNS)
            self._CSV_HANDLER.save_df_as_csv(df)

    @staticmethod
    def _convert_broker_time_to_local(date: str):
        ts = pd.Timestamp(date).tz_convert('Europe/Berlin')
        return ts.strftime("%Y-%m-%d %H:%M:%S")

    @abstractmethod
    def fetch_to_csv(self):
        pass


class NodeInfoFetcher(BrokerNodeFetcher):
    _CSV_CATEGORY = 'stats'
    _CSV_COLUMNS = ['date', 'last_contact', 'start', 'last_write', 'last_reject',
                    'imported', 'updated', 'invalid', 'failed',
                    'daily_imported', 'daily_updated', 'daily_invalid', 'daily_failed', 'error_rate']

    def fetch_to_csv(self):
        row_reference = pd.Series(dtype=str)
        node = self._BROKER_NODE_CONNECTION.get_broker_node(self._ID_NODE)
        stats = self._BROKER_NODE_CONNECTION.get_broker_node_stats(self._ID_NODE)
        df = self._CSV_HANDLER.read_csv_as_df()
        df = self.__delete_todays_row_if_exists(df)
        if df.empty:
            path_csv_last_year = self.__get_last_years_csv_path()
            if os.path.isfile(path_csv_last_year):
                tmp_csv_manager = CSVHandler(path_csv_last_year)
                df_last_year = tmp_csv_manager.read_csv_as_df()
                last_row = df_last_year.iloc[-1]
                if self.__check_end_of_year_transition(last_row.date):
                    row_reference = last_row
        else:
            row_reference = df.iloc[-1]
        if row_reference.empty:
            map_daily = self.__generate_empty_daily_stats()
        else:
            map_daily = self.__compute_daily_stats(row_reference, stats)
        new_row = {'date':         self.__extract_year_month_day(self._CURRENT_DATE),
                   'last_contact': self._convert_broker_time_to_local(node.last_contact),
                   'start':        self._convert_broker_time_to_local(stats.dwh_start),
                   'last_write':   self._convert_broker_time_to_local(stats.last_write) if stats.last_write else '-',
                   'last_reject':  self._convert_broker_time_to_local(stats.last_reject) if stats.last_reject else '-',
                   'imported':     stats.imported,
                   'updated':      stats.updated,
                   'invalid':      stats.invalid,
                   'failed':       stats.failed}
        new_row.update(map_daily)
        df = df.append(new_row, ignore_index=True)
        self._CSV_HANDLER.save_df_as_csv(df)

    def __delete_todays_row_if_exists(self, df: pd.DataFrame) -> pd.DataFrame:
        if not df.empty:
            current_date = self.__extract_year_month_day(self._CURRENT_DATE)
            last_row = df.iloc[-1]
            if last_row.date == current_date:
                df = df.head(-1)
            if any(df['date'] == current_date):
                raise SystemExit('Date of today was found in multiple rows!!')
        return df

    @staticmethod
    def __extract_year_month_day(date: datetime):
        month = str(date.month)
        month = month.rjust(2, '0')
        day = str(date.day)
        day = day.rjust(2, '0')
        return '-'.join([str(date.year), month, day])

    def __get_last_years_csv_path(self):
        date_last_year = self._CURRENT_DATE.replace(year=self._CURRENT_DATE.year - 1)
        name_csv = self._generate_csv_name(self._ID_NODE, date_last_year)
        return os.path.join(self._DIR_WORKING, name_csv)

    def __check_end_of_year_transition(self, date_end_of_year: str) -> bool:
        date_yesterday = self.__extract_year_month_day(self._CURRENT_DATE - timedelta(days=1))
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
            error_rate = round(error_rate * 100, 2)
        else:
            error_rate = '-'
        return {'daily_imported': daily_imported,
                'daily_updated':  daily_updated,
                'daily_invalid':  daily_invalid,
                'daily_failed':   daily_failed,
                'error_rate':     error_rate}


class NodeErrorFetcher(BrokerNodeFetcher):
    _CSV_CATEGORY = 'errors'
    _CSV_COLUMNS = ['timestamp', 'repeats', 'content']

    def fetch_to_csv(self):
        pass
        """
        errors = self.__BROKER_NODE_CONNECTION.get_broker_node_errors(self.__ID_NODE)
        df = self.__CSV_MANAGER.read_csv_as_df()
        for error in errors:
            new_row = {
                'timestamp': error.timestamp,
                'repeats':   error.repeats if error.repeats is not None else '1',
                'content':   error.content}
            df = df.append(new_row, ignore_index=True)
        self.__CSV_MANAGER.save_df_as_csv(df)
        """


class BrokerNodeFetcherManager:

    def __init__(self):
        self.__LIST_NODE_IDS = BrokerNodeConnection().get_broker_nodes_list()

    def fetch_broker_node_information(self):
        for id_node in self.__LIST_NODE_IDS:
            name_folder = id_node.rjust(3, '0')
            dir_working = self.__init_working_dir_for_node(name_folder)
            self.__fetch_broker_node_stats(id_node, dir_working)
            self.__fetch_broker_node_errors(id_node, dir_working)

    @staticmethod
    def __fetch_broker_node_stats(id_node: str, dir_working: str):
        info = NodeInfoFetcher(id_node, dir_working)
        info.init_working_csv()
        info.fetch_to_csv()

    @staticmethod
    def __fetch_broker_node_errors(id_node: str, dir_working: str):
        error = NodeErrorFetcher(id_node, dir_working)
        error.init_working_csv()
        error.fetch_to_csv()

    @staticmethod
    def __init_working_dir_for_node(name_folder: str) -> str:
        dir_root = os.environ['ROOT_DIR']
        dir_working = os.path.join(dir_root, name_folder)
        if not os.path.isdir(dir_working):
            os.makedirs(dir_working)
        return dir_working


def __init_logger():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s',
                        handlers=[logging.StreamHandler()])


def __stop_logger():
    [logging.root.removeHandler(handler) for handler in logging.root.handlers[:]]
    logging.shutdown()


def __load_properties_file_as_environment(path: str):
    set_required_keys = {'BROKER_URL', 'ADMIN_API_KEY', 'ROOT_DIR'}
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


def main(path_config: str):
    try:
        __init_logger()
        __load_properties_file_as_environment(path_config)
        fetcher = BrokerNodeFetcherManager()
        fetcher.fetch_broker_node_information()
    except Exception as e:
        logging.exception(e)
    finally:
        __stop_logger()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise SystemExit('please give path to config file')
    main(sys.argv[1])
