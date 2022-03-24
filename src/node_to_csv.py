# -*- coding: utf-8 -*
# Created on Wed Jan 19 12:00 2022
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
import re
import sys
from abc import ABC, abstractmethod

from common import CSVHandler
from common import ErrorCSVHandler
from common import InfoCSVHandler
from common import BrokerNodeConnection
from common import load_properties_file_as_environment
from common import __init_logger
from common import __stop_logger

import pandas as pd


class BrokerNodeFetcher(ABC):
    _CSV_HANDLER: CSVHandler

    def __init__(self, id_node: str, dir_working=''):
        self._ID_NODE = id_node
        self._DIR_WORKING = dir_working
        self._TIMEZONE = 'Europe/Berlin'
        self._CURRENT_DATE = pd.Timestamp.now().tz_localize(self._TIMEZONE)
        self._NAME_CSV = self._CSV_HANDLER.generate_csv_name(id_node)
        self._PATH_CSV = os.path.join(self._DIR_WORKING, self._NAME_CSV)
        self._BROKER_NODE_CONNECTION = BrokerNodeConnection()

    def _init_working_csv(self):
        if not os.path.isfile(self._PATH_CSV):
            df = pd.DataFrame(columns=self._CSV_HANDLER.get_csv_columns())
            self._CSV_HANDLER.save_df_to_csv(df, self._PATH_CSV)

    def _extract_YMD_HMS(self, date: pd.Timestamp) -> str:
        ts = date.tz_convert(self._TIMEZONE)
        return ts.strftime('%Y-%m-%d %H:%M:%S')

    def _extract_YMD_HMS_from_string(self, date: str) -> str:
        ts = pd.Timestamp(date).tz_convert(self._TIMEZONE)
        return ts.strftime('%Y-%m-%d %H:%M:%S')

    def _extract_YMD(self, date: pd.Timestamp) -> str:
        ts = date.tz_convert(self._TIMEZONE)
        return ts.strftime('%Y-%m-%d')

    @abstractmethod
    def fetch_to_csv(self):
        pass


class NodeInfoFetcher(BrokerNodeFetcher):
    _CSV_HANDLER = InfoCSVHandler()

    def fetch_to_csv(self):
        """
        Calls AKTIN Broker Endpoints to get import statistics of connected node and writes response in csv file.
        * One row in csv file equals the status of one day.
        * Computes differences to last row in csv file (assuming it contains the import statistic of yesterday's date).
        * Running the method multiple times will overwrite the row of the current day each time.
        * If the csv file is empty or newly created, the existence of last years csv file is checked.
        * Missing/Not computable values are added as '-'.
        * All date information from broker-server is converted into a local, human-readable format.
        * The vars 'last-reject' and 'last-write' from broker-server can be None if no data was imported/no error occured.
        * Csv file is rotated each year to limit file size.
        """
        self._init_working_csv()
        node = self._BROKER_NODE_CONNECTION.get_broker_node(self._ID_NODE)
        stats = self._BROKER_NODE_CONNECTION.get_broker_node_stats(self._ID_NODE)
        df = self._CSV_HANDLER.read_csv_as_df(self._PATH_CSV)
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
        dict_stats = pd.DataFrame(map_stats, index=[0])
        df = pd.concat([df, dict_stats])
        self._CSV_HANDLER.save_df_to_csv(df, self._PATH_CSV)

    def __delete_todays_row_if_exists(self, df: pd.DataFrame) -> pd.DataFrame:
        if not df.empty:
            current_date = self._extract_YMD(self._CURRENT_DATE)
            last_date = df.iloc[-1].date
            if not re.match(r"\d{4}-\d{2}-\d{2}", last_date):
                raise ValueError("invalid date format of input")
            if last_date == current_date:
                df = df.head(-1)
            if any(df['date'] == current_date):
                raise SystemExit('date of today was found in multiple rows!!')
        return df

    def __get_last_row_of_last_years_csv_if_exists(self) -> pd.DataFrame:
        path_csv_last_year = self.__get_last_years_csv_path()
        if os.path.isfile(path_csv_last_year):
            df_last_year = self._CSV_HANDLER.read_csv_as_df(path_csv_last_year)
            last_row = df_last_year.iloc[-1]
            if self.__was_csv_date_yesterday(last_row.date):
                return last_row
        return None

    def __get_last_years_csv_path(self) -> str:
        date_last_year = self._CURRENT_DATE.replace(year=self._CURRENT_DATE.year - 1)
        name_csv_last_year = self._CSV_HANDLER.generate_csv_name_with_custom_year(self._ID_NODE, str(date_last_year.year))
        return os.path.join(self._DIR_WORKING, name_csv_last_year)

    def __was_csv_date_yesterday(self, date_csv: str) -> bool:
        if not re.match(r"\d{4}-\d{2}-\d{2}", date_csv):
            raise ValueError("invalid date format of input")
        date_yesterday = self._extract_YMD(self._CURRENT_DATE - pd.Timedelta(days=1))
        return date_csv == date_yesterday

    def __are_dwh_start_date_equal(self, reference: pd.DataFrame, stats: BrokerNodeConnection.BrokerNodeStats) -> bool:
        start_reference = reference.start
        if not re.match(r"\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}", start_reference):
            raise ValueError("invalid date format of input")
        start_dwh = self._extract_YMD_HMS_from_string(stats.dwh_start)
        return start_reference == start_dwh

    @staticmethod
    def __generate_empty_daily_stats() -> dict:
        return {'daily_imported':   '-',
                'daily_updated':    '-',
                'daily_invalid':    '-',
                'daily_failed':     '-',
                'daily_error_rate': '-'}

    def __compute_daily_stats(self, stats_csv: pd.DataFrame, stats_current: BrokerNodeConnection.BrokerNodeStats) -> dict:
        daily_imported = int(stats_current.imported) - int(stats_csv.imported)
        daily_updated = int(stats_current.updated) - int(stats_csv.updated)
        daily_invalid = int(stats_current.invalid) - int(stats_csv.invalid)
        daily_failed = int(stats_current.failed) - int(stats_csv.failed)
        daily_error_rate = self.__compute_error_rate(daily_imported, daily_updated, daily_invalid, daily_failed)
        return {'daily_imported':   daily_imported,
                'daily_updated':    daily_updated,
                'daily_invalid':    daily_invalid,
                'daily_failed':     daily_failed,
                'daily_error_rate': daily_error_rate}

    def __generate_row_stats(self, node: BrokerNodeConnection.BrokerNode, stats: BrokerNodeConnection.BrokerNodeStats) -> dict:
        imported = int(stats.imported)
        updated = int(stats.updated)
        invalid = int(stats.invalid)
        failed = int(stats.failed)
        return {'date':         self._extract_YMD(self._CURRENT_DATE),
                'last_contact': self._extract_YMD_HMS_from_string(node.last_contact),
                'start':        self._extract_YMD_HMS_from_string(stats.dwh_start),
                'last_write':   self._extract_YMD_HMS_from_string(stats.last_write) if stats.last_write else '-',
                'last_reject':  self._extract_YMD_HMS_from_string(stats.last_reject) if stats.last_reject else '-',
                'imported':     imported,
                'updated':      updated,
                'invalid':      invalid,
                'failed':       failed,
                'error_rate':   self.__compute_error_rate(imported, updated, invalid, failed)}

    @staticmethod
    def __compute_error_rate(imported: int, updated: int, invalid: int, failed: int) -> str:
        sum_success = imported + updated
        sum_failed = invalid + failed
        if sum_failed > 0 and sum_success == 0:
            error_rate = '100.0'
        elif sum_failed == 0 and sum_success == 0:
            error_rate = '-'
        else:
            error_rate = sum_failed / (sum_success + sum_failed)
            error_rate = str(round(error_rate * 100, 2))
        return error_rate


class NodeErrorFetcher(BrokerNodeFetcher):
    _CSV_HANDLER = ErrorCSVHandler()

    def fetch_to_csv(self):
        """
        Calls AKTIN Broker Endpoints to get noted errors of connected node and writes response in csv file.
        * One row in csv file equals one occured error.
        * Logged Errors can be updated on the broker side, in which the var 'timestamp' is updated and 'repeats' is incremented.
        * Updates in csv are done by deleting and re-appending corresponding row.
        * Only errors of the current year are tracked in the csv file to limit file size.
        """
        self._init_working_csv()
        errors = self._BROKER_NODE_CONNECTION.get_broker_node_errors(self._ID_NODE)
        df = self._CSV_HANDLER.read_csv_as_df(self._PATH_CSV)
        for error in errors:
            if self.__did_error_appear_this_year(error):
                new_row = {
                    'timestamp': self._extract_YMD_HMS_from_string(error.timestamp),
                    'repeats':   error.repeats if error.repeats else '1',
                    'content':   error.content}
                dict_new_row = pd.DataFrame(new_row, index=[0])
                if self.__is_error_already_logged(df, error):
                    if self.__did_error_repeats_change(df, error):
                        df = self.__delete_old_error_row(df, error)
                        df = pd.concat([df, dict_new_row])
                else:
                    df = pd.concat([df, dict_new_row])
        df = df.sort_values(by='timestamp', ascending=False)
        self._CSV_HANDLER.save_df_to_csv(df, self._PATH_CSV)

    def __did_error_appear_this_year(self, error: BrokerNodeConnection.BrokerNodeError) -> bool:
        date_error = pd.Timestamp(error.timestamp).tz_convert(self._TIMEZONE)
        return self._CURRENT_DATE.year == date_error.year

    @staticmethod
    def __is_error_already_logged(reference: pd.DataFrame, error: BrokerNodeConnection.BrokerNodeError) -> bool:
        return any(reference['content'] == error.content)

    @staticmethod
    def __did_error_repeats_change(reference: pd.DataFrame, error: BrokerNodeConnection.BrokerNodeError) -> bool:
        idx = reference.index[reference['content'] == error.content][0]
        return reference['repeats'][idx] != error.repeats

    @staticmethod
    def __delete_old_error_row(df: pd.DataFrame, error: BrokerNodeConnection.BrokerNodeError) -> pd.DataFrame:
        idx = df.index[df['content'] == error.content][0]
        return df.drop(index=idx)


class BrokerNodeResourceFetcher:

    def __init__(self, id_node: str, dir_working=''):
        self.__ID_NODE = id_node
        self.__DIR_WORKING = dir_working
        self.__BROKER_NODE_CONNECTION = BrokerNodeConnection()

    def fetch_broker_node_versions(self):
        self.__fetch_broker_node_resource_to_text_file('versions')

    def fetch_broker_node_rscript(self):
        self.__fetch_broker_node_resource_to_text_file('rscript')

    def fetch_broker_node_python(self):
        self.__fetch_broker_node_resource_to_text_file('python')

    def fetch_broker_node_import_scripts(self):
        self.__fetch_broker_node_resource_to_text_file('import-scripts')

    def __fetch_broker_node_resource_to_text_file(self, resource: str):
        response = self.__BROKER_NODE_CONNECTION.get_broker_node_resource(self.__ID_NODE, resource)
        response = self.__clean_dictionary(response)
        self.__save_dict_as_txt_file(response, resource)

    @staticmethod
    def __clean_dictionary(dictionary: dict) -> dict:
        return {key: value if value is not None else '-' for key, value in dictionary.items()}

    def __save_dict_as_txt_file(self, dictionary: dict, type_resource: str):
        name_file = ''.join([self.__ID_NODE, '_', type_resource, '.txt'])
        path_file = os.path.join(self.__DIR_WORKING, name_file)
        with open(path_file, 'w') as file:
            file.write(json.dumps(dictionary))


class BrokerNodeFetcherManager:

    def __init__(self):
        self.__DIR_ROOT = os.environ['ROOT_DIR']
        self.__LIST_NODE_IDS = BrokerNodeConnection().get_broker_nodes_list()

    def fetch_broker_node_information(self):
        for id_node in self.__LIST_NODE_IDS:
            dir_working = self.init_working_directory(id_node)
            self.__fetch_broker_node_stats(id_node, dir_working)
            self.__fetch_broker_node_errors(id_node, dir_working)
            self.__fetch_broker_node_resources(id_node, dir_working)

    def init_working_directory(self, name_folder: str) -> str:
        dir_working = os.path.join(self.__DIR_ROOT, name_folder)
        if not os.path.isdir(dir_working):
            os.makedirs(dir_working)
        return dir_working

    @staticmethod
    def __fetch_broker_node_stats(id_node: str, dir_working: str):
        info = NodeInfoFetcher(id_node, dir_working)
        info.fetch_to_csv()

    @staticmethod
    def __fetch_broker_node_errors(id_node: str, dir_working: str):
        error = NodeErrorFetcher(id_node, dir_working)
        error.fetch_to_csv()

    @staticmethod
    def __fetch_broker_node_resources(id_node: str, dir_working: str):
        resources = BrokerNodeResourceFetcher(id_node, dir_working)
        resources.fetch_broker_node_versions()
        resources.fetch_broker_node_rscript()
        resources.fetch_broker_node_python()
        resources.fetch_broker_node_import_scripts()


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
    if len(sys.argv) == 1:
        raise SystemExit('path to config file is missing')
    if len(sys.argv) > 2:
        raise SystemExit('invalid number of input arguments')
    main(sys.argv[1])
