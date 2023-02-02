# -*- coding: utf-8 -*
# Created on Wed Jan 19 12:00 2022
# @version: 1.2

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

import pandas as pd

from common import BrokerNodeConnection, ErrorCSVHandler, InfoCSVHandler, MyLogger, PropertiesReader, SingletonABCMeta, TimestampHandler
from my_error_notifier import MyErrorNotifier


class BrokerNodeFetcher(ABC, metaclass=SingletonABCMeta):

    def __init__(self):
        self.__DIR_ROOT = os.environ['ROOT_DIR']
        self._BROKER_NODE_CONNECTION = BrokerNodeConnection()
        self._TIMESTAMP_HANDLER = TimestampHandler()

    def _init_working_directory_if_nonexisting(self, name_folder: str) -> str:
        dir_working = os.path.join(self.__DIR_ROOT, name_folder)
        if not os.path.isdir(dir_working):
            os.makedirs(dir_working)
        return dir_working

    @abstractmethod
    def fetch_broker_data_to_file(self, id_node: str):
        pass


class NodeInfoFetcher(BrokerNodeFetcher):
    """
    Fetches import and connection information from broker node to csv file
    """

    def __init__(self):
        super().__init__()
        self.__CSV_HANDLER = InfoCSVHandler()

    # TODO: On change of the year, two rows for New Years Eve are created?
    # csv old year : last row with check on 01-01 00:35
    # csv new year : first row with check on 01-01 18:00
    def fetch_broker_data_to_file(self, id_node: str):
        """
        Calls AKTIN Broker Endpoints to get import statistics of connected node and writes response in csv file.
        * One row in csv file equals the status of one day.
        * Computes differences to last row in csv file (assuming it contains the import statistic of yesterday).
        * Import stats are reseted on DWH when a restart occurs. Therefore, no daily differences are calculated either.
        * Running the method multiple times will overwrite the row of the current day each time.
        * All date information from broker-server is converted into a local, human-readable format.
        * The vars 'last-reject' and 'last-write' from broker-server can be None if no data was imported/no error occured.
        * Missing/Not computable values are added as '-'.
        * Csv file is rotated each year to limit file size.
        * If the csv file is empty or newly created, the existence of last years csv file is checked.
        """
        name_csv = self.__CSV_HANDLER.generate_csv_name(id_node)
        dir_working = self._init_working_directory_if_nonexisting(id_node)
        path_csv = self.__CSV_HANDLER.init_csv_file_if_not_exists(dir_working, name_csv)
        node = self._BROKER_NODE_CONNECTION.get_broker_node(id_node)
        stats = self._BROKER_NODE_CONNECTION.get_broker_node_stats(id_node)
        df = self.__CSV_HANDLER.read_csv_as_df(path_csv)
        df = self.__delete_todays_row_if_exists(df)
        if df.empty:
            row_csv = self.__get_last_row_of_last_years_csv_if_exists(id_node, dir_working)
        else:
            row_csv = df.iloc[-1]
        if row_csv is not None:
            if self.__was_last_check_yesterday(row_csv) and self.__are_dwh_start_date_equal(row_csv, stats):
                map_daily = self.__compute_daily_stats(row_csv, stats)
            else:
                map_daily = self.__generate_empty_daily_stats()
        else:
            map_daily = self.__generate_empty_daily_stats()
        map_stats = self.__generate_row_stats(node, stats)
        map_stats.update(map_daily)
        dict_stats = pd.DataFrame(map_stats, index=[0])
        df = pd.concat([df, dict_stats])
        self.__CSV_HANDLER.save_df_to_csv(df, path_csv)

    def __delete_todays_row_if_exists(self, csv: pd.DataFrame) -> pd.DataFrame:
        """
        Each row should equal one day. No duplicates allowed.
        """
        if not csv.empty:
            date_current = self._TIMESTAMP_HANDLER.get_current_date()
            YMD_current = self._TIMESTAMP_HANDLER.get_YMD_from_date_string(date_current)
            YMD_csv_last = self._TIMESTAMP_HANDLER.get_YMD_from_date_string(csv.iloc[-1].date)
            if YMD_csv_last == YMD_current:
                csv = csv.head(-1)
            if any(csv['date'] == YMD_current):
                raise SystemExit('date of today was found in multiple rows!!')
        return csv

    def __get_last_row_of_last_years_csv_if_exists(self, id_node: str, dir_working: str) -> pd.DataFrame:
        """
        Important at the turn of the year. Each csv file shall only include the data of one year.
        Necessary to compute daily differences if yesterday's date was New Year's Eve.
        """
        year_current = self._TIMESTAMP_HANDLER.get_current_year()
        year_last_year = str(int(year_current) - 1)
        name_csv_last_year = self.__CSV_HANDLER.generate_csv_name_with_custom_year(id_node, year_last_year)
        path_csv_last_year = os.path.join(dir_working, name_csv_last_year)
        if os.path.isfile(path_csv_last_year):
            df_last_year = self.__CSV_HANDLER.read_csv_as_df(path_csv_last_year)
            return df_last_year.iloc[-1]
        return None

    def __was_last_check_yesterday(self, row_csv: pd.DataFrame) -> bool:
        """
        Consistency check. Can't compute today's stats without a yesterday.
        """
        date_yesterday = self._TIMESTAMP_HANDLER.get_yesterdays_date()
        YMD_yesterday = self._TIMESTAMP_HANDLER.get_YMD_from_date_string(date_yesterday)
        YMD_csv_last = self._TIMESTAMP_HANDLER.get_YMD_from_date_string(row_csv.date)
        return YMD_csv_last == YMD_yesterday

    def __are_dwh_start_date_equal(self, row_csv: pd.DataFrame, stats: BrokerNodeConnection.BrokerNodeStats) -> bool:
        """
        Consistency check. Import stats of AKTIN DWH are reseted on each restart.
        """
        start_dwh = self._TIMESTAMP_HANDLER.get_YMD_HMS_from_date_string(stats.dwh_start)
        return row_csv.last_start == start_dwh

    @staticmethod
    def __generate_empty_daily_stats() -> dict:
        return {'daily_imported':  '-',
                'daily_updated':   '-',
                'daily_invalid':   '-',
                'daily_failed':    '-',
                'daily_error_rate':'-'}

    def __compute_daily_stats(self, stats_csv: pd.DataFrame, stats_node: BrokerNodeConnection.BrokerNodeStats) -> dict:
        daily_imported = int(stats_node.imported) - int(stats_csv.imported)
        daily_updated = int(stats_node.updated) - int(stats_csv.updated)
        daily_invalid = int(stats_node.invalid) - int(stats_csv.invalid)
        daily_failed = int(stats_node.failed) - int(stats_csv.failed)
        daily_error_rate = self.__compute_error_rate(daily_imported, daily_updated, daily_invalid, daily_failed)
        return {'daily_imported':  daily_imported,
                'daily_updated':   daily_updated,
                'daily_invalid':   daily_invalid,
                'daily_failed':    daily_failed,
                'daily_error_rate':daily_error_rate}

    def __generate_row_stats(self, node_broker: BrokerNodeConnection.BrokerNode, node_stats: BrokerNodeConnection.BrokerNodeStats) -> dict:
        """
        'last_contact' seems to be in UTC, the other dates are in local timezone
        """
        imported = int(node_stats.imported)
        updated = int(node_stats.updated)
        invalid = int(node_stats.invalid)
        failed = int(node_stats.failed)
        last_contact = self._TIMESTAMP_HANDLER.convert_utc_to_local_date_string(node_broker.last_contact)
        return {'date':        self._TIMESTAMP_HANDLER.get_YMD_HMS_from_date_string(self._TIMESTAMP_HANDLER.get_current_date()),
                'last_contact':self._TIMESTAMP_HANDLER.get_YMD_HMS_from_date_string(last_contact),
                'last_start':  self._TIMESTAMP_HANDLER.get_YMD_HMS_from_date_string(node_stats.dwh_start),
                'last_write':  self._TIMESTAMP_HANDLER.get_YMD_HMS_from_date_string(node_stats.last_write) if node_stats.last_write is not None else '-',
                'last_reject': self._TIMESTAMP_HANDLER.get_YMD_HMS_from_date_string(node_stats.last_reject) if node_stats.last_reject is not None else '-',
                'imported':    imported,
                'updated':     updated,
                'invalid':     invalid,
                'failed':      failed,
                'error_rate':  self.__compute_error_rate(imported, updated, invalid, failed)}

    @staticmethod
    def __compute_error_rate(imported: int, updated: int, invalid: int, failed: int) -> str:
        sum_success = imported + updated
        sum_failure = invalid + failed
        if sum_failure > 0 and sum_success == 0:
            error_rate = '100.0'
        elif sum_failure == 0 and sum_success == 0:
            error_rate = '-'
        else:
            error_rate = sum_failure / (sum_success + sum_failure)
            error_rate = str(round(error_rate * 100, 2))
        return error_rate


class NodeErrorFetcher(BrokerNodeFetcher):
    """
    Fetches import errors from broker node to csv file
    """

    def __init__(self):
        super().__init__()
        self.__CSV_HANDLER = ErrorCSVHandler()

    def fetch_broker_data_to_file(self, id_node: str):
        """
        Calls AKTIN Broker Endpoints to get errors of connected node and writes response in csv file.
        * One row in csv file equals one occured error.
        * Logged Errors can be updated on the broker side, in which 'timestamp' is updated and 'repeats' is incremented.
        * Updates in csv are done by deleting and re-appending corresponding row.
        * Only errors of the current year are tracked in the csv file to limit file size.
        * As with NodeInfoFetcher, the Csv file is rotated each year.
        """
        name_csv = self.__CSV_HANDLER.generate_csv_name(id_node)
        dir_working = self._init_working_directory_if_nonexisting(id_node)
        path_csv = self.__CSV_HANDLER.init_csv_file_if_not_exists(dir_working, name_csv)
        errors = self._BROKER_NODE_CONNECTION.get_broker_node_errors(id_node)
        df = self.__CSV_HANDLER.read_csv_as_df(path_csv)
        for error in errors:
            if self.__did_error_appear_this_year(error):
                row_error = self.__convert_error_to_row(error)
                if self.__is_error_already_logged(df, row_error):
                    if self.__did_error_row_repeats_change(df, row_error):
                        df = self.__delete_old_error_row(df, row_error)
                        df = pd.concat([df, row_error])
                else:
                    df = pd.concat([df, row_error])
        df = df.sort_values(by='timestamp', ascending=False)
        self.__CSV_HANDLER.save_df_to_csv(df, path_csv)

    def __did_error_appear_this_year(self, error: BrokerNodeConnection.BrokerNodeError) -> bool:
        year_current = self._TIMESTAMP_HANDLER.get_current_year()
        year_error = self._TIMESTAMP_HANDLER.get_year_from_date_string(error.timestamp)
        return year_current == year_error

    def __convert_error_to_row(self, error: BrokerNodeConnection.BrokerNodeError) -> pd.DataFrame:
        """
        The var 'repeats' from broker-server can be None, if the error occured just once.
        Var 'timestamp' is in local timezone.
        """
        new_row = {
            'timestamp':self._TIMESTAMP_HANDLER.get_YMD_HMS_from_date_string(error.timestamp),
            'repeats':  error.repeats if error.repeats is not None else '1',
            'content':  error.content}
        return pd.DataFrame(new_row, index=[0])

    @staticmethod
    def __is_error_already_logged(csv: pd.DataFrame, row_error: pd.DataFrame) -> bool:
        set_csv = set(csv['content'])
        set_error = set(row_error['content'])
        return any(set_csv.intersection(set_error))

    @staticmethod
    def __did_error_row_repeats_change(csv: pd.DataFrame, row_error: pd.DataFrame) -> bool:
        df_intersection = pd.merge(csv, row_error, on='content', how='inner')
        repeats_csv = df_intersection['repeats_x'].values
        repeats_row_error = df_intersection['repeats_y'].values
        return repeats_csv != repeats_row_error

    @staticmethod
    def __delete_old_error_row(csv: pd.DataFrame, row_error: pd.DataFrame) -> pd.DataFrame:
        idx = csv.index[csv['content'] == row_error['content'][0]]
        return csv.drop(index=idx)


class NodeResourceFetcher(BrokerNodeFetcher):
    """
    Fetches broker node resources like installed versions or packages to a text file
    """

    def fetch_broker_data_to_file(self, id_node: str):
        dir_working = self._init_working_directory_if_nonexisting(id_node)
        self.__fetch_broker_node_resource_to_text_file('versions', id_node, dir_working)
        self.__fetch_broker_node_resource_to_text_file('rscript', id_node, dir_working)
        self.__fetch_broker_node_resource_to_text_file('python', id_node, dir_working)
        self.__fetch_broker_node_resource_to_text_file('import-scripts', id_node, dir_working)

    def __fetch_broker_node_resource_to_text_file(self, type_resource: str, id_node: str, dir_working: str):
        """
        Logs changes in resource items by comparing existing information in file (from previous day)
        with current information from broker. Older information is overwritten with newer after
        logging.
        """
        response = self._BROKER_NODE_CONNECTION.get_broker_node_resource(id_node, type_resource)
        response = self.__clean_dictionary(response)
        path_resource = self.__generate_resource_file_path(type_resource, id_node, dir_working)
        if os.path.exists(path_resource):
            resource = self.__load_txt_file_as_dict(path_resource)
            path_log = self.__generate_resource_log_path(type_resource, id_node, dir_working)
            self.__log_new_and_updated_items(path_log, response, resource)
            self.__log_deleted_items(path_log, response, resource)
        self.__save_dict_as_txt_file(response, path_resource)

    def __log_new_and_updated_items(self, path_log: str, broker: dict, resource: dict):
        set_broker = set(broker.items())
        set_resource = set(resource.items())
        set_diff = set_broker.difference(set_resource)
        if set_diff:
            for change in set_diff:
                name_resource = change[0]
                version_old = resource.get(name_resource)
                version_new = change[1]
                with open(path_log, 'a') as log:
                    current = self._TIMESTAMP_HANDLER.get_current_date()
                    version_old = 'NEW' if version_old is None else version_old
                    log.write("{0} : [{1}] {2} --> {3}\n".format(current, name_resource, version_old, version_new))

    def __log_deleted_items(self, path_log: str, broker: dict, resource: dict):
        set_broker = set(broker.keys())
        set_resource = set(resource.keys())
        set_del = set_resource.difference(set_broker)
        if set_del:
            for name_resource in set_del:
                version_old = resource.get(name_resource)
                with open(path_log, 'a') as log:
                    current = self._TIMESTAMP_HANDLER.get_current_date()
                    log.write("{0} : [{1}] {2} --> DELETED\n".format(current, name_resource, version_old))

    @staticmethod
    def __clean_dictionary(dictionary: dict) -> dict:
        """
        Set dict values with None to be '-', as file.write() throws exception when a None occurs
        Remove possible \n from values for better (human) readability
        """
        for key, value in dictionary.copy().items():
            if value is None:
                dictionary[key] = '-'
            else:
                dictionary[key] = value.replace('\n', '')
        return dictionary

    @staticmethod
    def __generate_resource_file_path(type_resource: str, id_node: str, dir_working: str) -> str:
        name_file = ''.join([id_node, '_', type_resource, '.txt'])
        return os.path.join(dir_working, name_file)

    @staticmethod
    def __generate_resource_log_path(type_resource: str, id_node: str, dir_working: str) -> str:
        name_file = ''.join([id_node, '_log_', type_resource, '.log'])
        return os.path.join(dir_working, name_file)

    @staticmethod
    def __save_dict_as_txt_file(dictionary: dict, path_file: str):
        with open(path_file, 'w') as file:
            file.write(json.dumps(dictionary))

    @staticmethod
    def __load_txt_file_as_dict(path_file: str) -> dict:
        with open(path_file, 'r') as file:
            return json.load(file)


class NodeFetcherManager:

    def __init__(self):
        self.__LIST_NODE_IDS = BrokerNodeConnection().get_broker_nodes()
        self.__INFO_FETCHER = NodeInfoFetcher()
        self.__ERROR_FETCHER = NodeErrorFetcher()
        self.__RESOURCES_FETCHER = NodeResourceFetcher()

    def fetch_broker_node_information(self):
        for id_node in self.__LIST_NODE_IDS:
            self.__INFO_FETCHER.fetch_broker_data_to_file(id_node)
            self.__ERROR_FETCHER.fetch_broker_data_to_file(id_node)
            self.__RESOURCES_FETCHER.fetch_broker_data_to_file(id_node)


def main(path_config: str):
    logger = MyLogger()
    reader = PropertiesReader()
    try:
        logger.init_logger()
        reader.load_properties_as_env_vars(path_config)
        manager = NodeFetcherManager()
        manager.fetch_broker_node_information()
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
