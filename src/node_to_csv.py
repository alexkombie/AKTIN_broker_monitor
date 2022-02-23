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

import logging
import os
import re
import sys
from abc import ABC, abstractmethod
from functools import singledispatchmethod

from common import CSVHandler
from common import BrokerNodeConnection
from common import load_properties_file_as_environment
from common import __init_logger
from common import __stop_logger

import pandas as pd


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

    def init_working_csv(self) -> str:
        if not os.path.isfile(self._PATH_CSV):
            df = pd.DataFrame(columns=self._CSV_COLUMNS)
            self._CSV_HANDLER.save_df_as_csv(df)
        return self._PATH_CSV

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
        import statistic of yesterday's date). Csv file is rotated each year. If the csv file is empty or newly created,
        the existence of last years csv file is checked. Condition to compute differences only to yesterday still applies.
        Missing/Not computable values are added as '-'. All date information from broker-server is converted into a
        local, human-readable format. The vars 'last-reject' and 'last-write' from broker-server can be None if no data
        was imported/no error occured.
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

    def __get_last_row_of_last_years_csv_if_exists(self):
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
                error_rate = ''.join([str(invalid + failed), '00.0'])
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
    Main class to run broker node info fetching. Creates working dir for each connected node
    (if necessary). Initializes and executes fetcher.
    """

    def __init__(self):
        self.DIR_ROOT = os.environ['ROOT_DIR']
        self.LIST_NODE_IDS = BrokerNodeConnection().get_broker_nodes_list()

    def fetch_broker_node_information(self):
        for id_node in self.LIST_NODE_IDS:
            name_folder = id_node.rjust(3, '0')
            dir_working = self.init_working_directory(name_folder)
            self.__fetch_broker_node_stats(id_node, dir_working)
            self.__fetch_broker_node_errors(id_node, dir_working)

    def init_working_directory(self, name_folder: str) -> str:
        dir_working = os.path.join(self.DIR_ROOT, name_folder)
        if not os.path.isdir(dir_working):
            os.makedirs(dir_working)
        return dir_working

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


def main(path_config: str):
    try:
        __init_logger()
        load_properties_file_as_environment(path_config)
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
