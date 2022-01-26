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
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import singledispatchmethod

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
    Meta class to make BrokerNodeConnection a Singleton
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
                    elem.get('timestamp'),
                    elem.get('repeats'),
                    elem.text)
            errors.append(error)
        return errors

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


class BrokerNodeFetcher(ABC):
    _CSV_CATEGORY: str
    _CSV_COLUMNS: list[str]

    def __init__(self, id_node: str, dir_working=''):
        self._ID_NODE = id_node
        self._DIR_WORKING = dir_working
        self._TIMEZONE = 'Europe/Berlin'
        self._CURRENT_DATE = pd.Timestamp.now().tz_localize(self._TIMEZONE)
        self._NAME_CSV = self._generate_csv_name(self._ID_NODE, self._CURRENT_DATE)
        self._PATH_CSV = os.path.join(self._DIR_WORKING, self._NAME_CSV)
        self._CSV_HANDLER = CSVHandler(self._PATH_CSV)
        self._BROKER_NODE_CONNECTION = BrokerNodeConnection()

    def _generate_csv_name(self, id_node: str, date: pd.Timestamp) -> str:
        id_node = id_node.rjust(3, '0')
        name_csv = '_'.join([id_node, self._CSV_CATEGORY, str(date.year)])
        return ''.join([name_csv, '.csv'])

    def init_working_csv(self):
        if not os.path.isfile(self._PATH_CSV):
            df = pd.DataFrame(columns=self._CSV_COLUMNS)
            self._CSV_HANDLER.save_df_as_csv(df)

    @singledispatchmethod
    def _extract_YMD_HMS(self, date) -> str:
        pass

    @_extract_YMD_HMS.register
    def _(self, date: pd.Timestamp) -> str:
        ts = date.tz_convert(self._TIMEZONE)
        return ts.strftime("%Y-%m-%d %H:%M:%S")

    @_extract_YMD_HMS.register
    def _(self, date: str) -> str:
        ts = pd.Timestamp(date).tz_convert(self._TIMEZONE)
        return ts.strftime("%Y-%m-%d %H:%M:%S")

    @singledispatchmethod
    def _extract_YMD(self, date) -> str:
        pass

    @_extract_YMD.register
    def _(self, date: pd.Timestamp) -> str:
        ts = date.tz_convert(self._TIMEZONE)
        return ts.strftime("%Y-%m-%d")

    @_extract_YMD.register
    def _(self, date: str) -> str:
        ts = pd.Timestamp(date).tz_convert(self._TIMEZONE)
        return ts.strftime("%Y-%m-%d")

    @abstractmethod
    def fetch_to_csv(self):
        pass


class NodeInfoFetcher(BrokerNodeFetcher):
    _CSV_CATEGORY = 'stats'
    _CSV_COLUMNS = ['date', 'last_contact', 'start', 'last_write', 'last_reject',
                    'imported', 'updated', 'invalid', 'failed', 'error_rate',
                    'daily_imported', 'daily_updated', 'daily_invalid', 'daily_failed', 'daily_error_rate']

    def fetch_to_csv(self):
        """
        Calls AKTIN Broker Endpoints to get import statistics of connected node and writes response in csv file.
        One row in csv file equals the status of one day. Running the method multiple times will overwrite the row
        of the current day each time. Computes differences to last row in csv file (assuming it contains the
        import statistic of yesterday). If the csv file is empty or newly created, the existence of last years csv
        file is checked as csv files are rotated each year. Missing/Not computable values are added as '-'.
        All date information from response is converted into a local, human-readable format. The vars 'last-reject'
        and 'last-write' from broker response can be None if no data was imported/no error occured.
        """
        node = self._BROKER_NODE_CONNECTION.get_broker_node(self._ID_NODE)
        stats = self._BROKER_NODE_CONNECTION.get_broker_node_stats(self._ID_NODE)
        df = self._CSV_HANDLER.read_csv_as_df()
        df = self.__delete_todays_row_if_exists(df)
        if df.empty:
            row_reference = self.__get_last_row_of_last_years_csv_if_exists()
        else:
            row_reference = df.iloc[-1]
        if row_reference is not None:
            if self.__was_csv_date_yesterday(row_reference.date) and self.__are_dwh_start_date_equal(row_reference, stats):
                map_daily = self.__compute_daily_stats(row_reference, stats)
            else:
                map_daily = self.__generate_empty_daily_stats()
        else:
            map_daily = self.__generate_empty_daily_stats()
        map_stats = self.__generate_row_stats(node, stats)
        map_stats.update(map_daily)
        df = df.append(map_stats, ignore_index=True)
        self._CSV_HANDLER.save_df_as_csv(df)

    def __delete_todays_row_if_exists(self, df: pd.DataFrame) -> pd.DataFrame:
        if not df.empty:
            current_date = self._extract_YMD(self._CURRENT_DATE)
            last_date = df.iloc[-1].date
            if not re.match(r"\d{4}-\d{2}-\d{2}", last_date):
                raise SystemExit("invalid date of date_csv")
            if last_date == current_date:
                df = df.head(-1)
            if any(df['date'] == current_date):
                raise SystemExit('date of today was found in multiple rows!!')
        return df

    def __get_last_row_of_last_years_csv_if_exists(self) -> pd.Series | None:
        path_csv_last_year = self.__get_last_years_csv_path()
        if os.path.isfile(path_csv_last_year):
            tmp_csv_manager = CSVHandler(path_csv_last_year)
            df_last_year = tmp_csv_manager.read_csv_as_df()
            last_row = df_last_year.iloc[-1]
            if self.__was_csv_date_yesterday(last_row.date):
                return last_row
        return None

    def __get_last_years_csv_path(self) -> str:
        date_last_year = self._CURRENT_DATE.replace(year=self._CURRENT_DATE.year - 1)
        name_csv = self._generate_csv_name(self._ID_NODE, date_last_year)
        return os.path.join(self._DIR_WORKING, name_csv)

    def __was_csv_date_yesterday(self, date_csv: str) -> bool:
        if not re.match(r"\d{4}-\d{2}-\d{2}", date_csv):
            raise SystemExit("invalid date format of date_csv")
        date_yesterday = self._extract_YMD(self._CURRENT_DATE - pd.Timedelta(days=1))
        return date_csv == date_yesterday

    def __are_dwh_start_date_equal(self, reference: pd.Series, stats: BrokerNodeConnection.BrokerNodeStats):
        start_reference = reference.start
        if not re.match(r"\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}", start_reference):
            raise SystemExit("invalid date format of date_csv")
        start_dwh = self._extract_YMD_HMS(stats.dwh_start)
        return start_reference == start_dwh

    @staticmethod
    def __generate_empty_daily_stats() -> dict:
        return {'daily_imported':   '-',
                'daily_updated':    '-',
                'daily_invalid':    '-',
                'daily_failed':     '-',
                'daily_error_rate': '-'}

    def __compute_daily_stats(self, stats_previous, stats_current) -> dict:
        daily_imported = int(stats_current.imported) - int(stats_previous.imported)
        daily_updated = int(stats_current.updated) - int(stats_previous.updated)
        daily_invalid = int(stats_current.invalid) - int(stats_previous.invalid)
        daily_failed = int(stats_current.failed) - int(stats_previous.failed)
        daily_error_rate = self.__compute_error_rate(daily_imported, daily_updated, daily_invalid, daily_failed)
        return {'daily_imported':   daily_imported,
                'daily_updated':    daily_updated,
                'daily_invalid':    daily_invalid,
                'daily_failed':     daily_failed,
                'daily_error_rate': daily_error_rate}

    def __generate_row_stats(self, node, stats) -> dict:
        imported = int(stats.imported)
        updated = int(stats.updated)
        invalid = int(stats.invalid)
        failed = int(stats.failed)
        return {'date':         self._extract_YMD(self._CURRENT_DATE),
                'last_contact': self._extract_YMD_HMS(node.last_contact),
                'start':        self._extract_YMD_HMS(stats.dwh_start),
                'last_write':   self._extract_YMD_HMS(stats.last_write) if stats.last_write else '-',
                'last_reject':  self._extract_YMD_HMS(stats.last_reject) if stats.last_reject else '-',
                'imported':     imported,
                'updated':      updated,
                'invalid':      invalid,
                'failed':       failed,
                'error_rate':   self.__compute_error_rate(imported, updated, invalid, failed)}

    @staticmethod
    def __compute_error_rate(imported: int, updated: int, invalid: int, failed: int) -> str:
        if (imported + updated) > 0:
            error_rate = (invalid + failed) / (imported + updated)
            error_rate = str(round(error_rate * 100, 2))
        else:
            if (invalid + failed) > 0:
                error_rate = '100.0'
            else:
                error_rate = '-'
        return error_rate


class NodeErrorFetcher(BrokerNodeFetcher):
    _CSV_CATEGORY = 'errors'
    _CSV_COLUMNS = ['timestamp', 'repeats', 'content']

    def fetch_to_csv(self):
        """
        Calls AKTIN Broker Endpoints to get noted errors of connected node and writes response in csv file.
        One row in csv file equals one occured error. Logged Errors can be updated on the broker side, in
        which the var 'timestamp' is updated and 'repeats' is incremented. Updates in csv are done by deleting
        and re-appending corresponding row. Only errors of the current year are tracked in the csv file
        to limit file size. Csv file is rotated each year.
        """
        errors = self._BROKER_NODE_CONNECTION.get_broker_node_errors(self._ID_NODE)
        df = self._CSV_HANDLER.read_csv_as_df()
        for error in errors:
            if self.__did_error_appear_this_year(error):
                new_row = {
                    'timestamp': self._extract_YMD_HMS(error.timestamp),
                    'repeats':   error.repeats if error.repeats is not None else '1',
                    'content':   error.content}
                if self.__is_error_already_logged(df, error):
                    if self.__did_error_repeats_change(df, error):
                        df = self.__delete_old_error_row(df, error)
                        df = df.append(new_row, ignore_index=True)
                else:
                    df = df.append(new_row, ignore_index=True)
        df = df.sort_values(by='timestamp')
        self._CSV_HANDLER.save_df_as_csv(df)

    def __did_error_appear_this_year(self, error: BrokerNodeConnection.BrokerNodeError) -> bool:
        date_local = pd.Timestamp(error.timestamp).tz_convert(self._TIMEZONE)
        return self._CURRENT_DATE.year == date_local.year

    @staticmethod
    def __is_error_already_logged(df: pd.DataFrame, error: BrokerNodeConnection.BrokerNodeError) -> bool:
        return any(df['content'] == error.content)

    @staticmethod
    def __did_error_repeats_change(df: pd.DataFrame, error: BrokerNodeConnection.BrokerNodeError) -> bool:
        idx = df.index[df['content'] == error.content][0]
        return df['repeats'][idx] != error.repeats

    @staticmethod
    def __delete_old_error_row(df: pd.DataFrame, error: BrokerNodeConnection.BrokerNodeError) -> pd.DataFrame:
        idx = df.index[df['content'] == error.content][0]
        return df.drop(index=idx)


class BrokerNodeFetcherManager:
    """
    Main class to run broker information fetching. Creates working dir for each connected node
    (if necessary) and initializes fetcher. Creates csv files for fetcher if necessary.
    """

    def __init__(self):
        self.LIST_NODE_IDS = BrokerNodeConnection().get_broker_nodes_list()

    def fetch_broker_node_information(self):
        for id_node in self.LIST_NODE_IDS:
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
