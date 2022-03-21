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

import bs4
import os
import re
import json
import pprint
from datetime import datetime
from datetime import timedelta

from bs4.element import Tag

from common import CSVHandler
from common import BrokerNodeConnection
from common import load_properties_file_as_environment
from common import __init_logger
from common import __stop_logger
from common import ConfluenceConnection
from common import load_json_file_as_dict


class CSVBackupManager:

    def __init__(self, id_node: str, dir_working=''):
        self.__DIR_WORKING = dir_working
        global dict_mapping
        self.__COMMON_NAME = dict_mapping[id_node]['COMMON']
        self.__CONFLUENCE = ConfluenceConnection()

    def backup_csv_files(self):
        list_csv_files = self.__get_all_csv_files_in_directory()
        for name_csv in list_csv_files:
            path_csv = os.path.join(self.__DIR_WORKING, name_csv)
            self.__CONFLUENCE.upload_csv_as_attachement_to_page(self.__COMMON_NAME, path_csv)

    def __get_all_csv_files_in_directory(self) -> list:
        return [name_file for name_file in os.listdir(self.__DIR_WORKING) if name_file.endswith('.csv')]


class TemplateResourceHandler(ABC):

    def _get_resource_as_soup(self, path_resource: str):
        with open(path_resource, 'r') as resource:
            content = resource.read()
        return self._convert_resource_to_soup(content)

    @staticmethod
    def _convert_resource_to_soup(resource_template: str):
        return bs4.BeautifulSoup(resource_template, 'html.parser')


class TemplatePageLoader(TemplateResourceHandler, ABC):
    _PAGE_TEMPLATE: bs4.BeautifulSoup

    def _load_template_page_as_soup(self, page_template: str):
        self._PAGE_TEMPLATE = self._convert_resource_to_soup(page_template)


class NodeResourceFetcher:

    def __init__(self, id_node: str):
        self.__ID_NODE = id_node
        self.__BROKER_NODE_CONNECTION = BrokerNodeConnection()

    def fetch_broker_node_versions(self) -> dict:
        return self.__BROKER_NODE_CONNECTION.get_broker_node_resource(self.__ID_NODE, 'versions')

    def fetch_broker_node_rscript(self) -> dict:
        return self.__BROKER_NODE_CONNECTION.get_broker_node_resource(self.__ID_NODE, 'rscript')

    def fetch_broker_node_python(self) -> dict:
        return self.__BROKER_NODE_CONNECTION.get_broker_node_resource(self.__ID_NODE, 'python')

    def fetch_broker_node_import_scripts(self) -> dict:
        return self.__BROKER_NODE_CONNECTION.get_broker_node_resource(self.__ID_NODE, 'import-scripts')


class TemplatePageNodeResourceWriter(TemplatePageLoader):

    def __init__(self, id_node: str):
        self.__ID_NODE = id_node
        self.__FETCHER = NodeResourceFetcher(id_node)

    def add_resources_to_template_page(self, page_template: str) -> str:
        self._load_template_page_as_soup(page_template)
        self.__add_versions_to_template_soup()
        self.__add_rscript_to_template_soup()
        self.__add_python_to_template_soup()
        self.__add_import_scripts_to_template_soup()
        return str(self._PAGE_TEMPLATE)

    def __add_versions_to_template_soup(self):
        dict_versions = self.__FETCHER.fetch_broker_node_versions()
        self._PAGE_TEMPLATE.find(class_='os').string.replace_with(dict_versions.get('os'))
        self._PAGE_TEMPLATE.find(class_='kernel').string.replace_with(dict_versions.get('kernel'))
        self._PAGE_TEMPLATE.find(class_='java').string.replace_with(dict_versions.get('java'))
        self._PAGE_TEMPLATE.find(class_='wildfly').string.replace_with(dict_versions.get('j2ee-impl'))
        self._PAGE_TEMPLATE.find(class_='apache2').string.replace_with(dict_versions.get('apache2'))
        self._PAGE_TEMPLATE.find(class_='postgresql').string.replace_with(dict_versions.get('postgres'))
        self._PAGE_TEMPLATE.find(class_='dwh_api').string.replace_with(dict_versions.get('dwh-api'))
        self._PAGE_TEMPLATE.find(class_='dwh_j2ee').string.replace_with(dict_versions.get('dwh-j2ee'))

    def __add_rscript_to_template_soup(self):
        dict_rscript = self.__FETCHER.fetch_broker_node_rscript()
        rscript = self.__concat_dict_items_as_string(dict_rscript)
        self._PAGE_TEMPLATE.find(class_='rscript').string.replace_with(rscript)

    def __add_python_to_template_soup(self):
        dict_python = self.__FETCHER.fetch_broker_node_python()
        python = self.__concat_dict_items_as_string(dict_python)
        self._PAGE_TEMPLATE.find(class_='python').string.replace_with(python)

    def __add_import_scripts_to_template_soup(self):
        dict_import_scripts = self.__FETCHER.fetch_broker_node_import_scripts()
        import_scripts = self.__concat_dict_items_as_string(dict_import_scripts)
        self._PAGE_TEMPLATE.find(class_='import_scripts').string.replace_with(import_scripts)

    @staticmethod
    def __concat_dict_items_as_string(input_dict: dict) -> str:
        tmp_list = []
        for key, value in input_dict.items():
            value = '?' if value is None else value
            item = ''.join([key, ' ', '(', value, ')'])
            tmp_list.append(item)
        return ', '.join(tmp_list)


class CSVExtractor(CSVHandler):

    def __init__(self, path_csv: str):
        super().__init__(path_csv)
        self.__DF = self.read_csv_as_df()

    def get_last_row_as_dict(self) -> dict:
        return self.__DF.iloc[-1].to_dict()

    def get_first_rows_as_list_of_dicts(self, num_rows: int) -> list[dict]:
        df = self.__DF.head(num_rows)
        return df.to_dict('records')


# TODO input just id??
class TemplatePageCSVInfoWriter(TemplatePageLoader):

    def __init__(self, path_csv: str):
        self.__EXTRACTOR = CSVExtractor(path_csv)

    def add_node_stats_to_template_page(self, page_template: str) -> str:
        self._load_template_page_as_soup(page_template)
        self.__add_dates_to_template_soup()
        self.__add_global_imports_to_template_soup()
        self.__add_daily_imports_to_template_soup()
        return str(self._PAGE_TEMPLATE)

    def __add_dates_to_template_soup(self):
        dict_row = self.__EXTRACTOR.get_last_row_as_dict()
        self._PAGE_TEMPLATE.find(class_='last_check').string.replace_with(dict_row.get('date'))
        self._PAGE_TEMPLATE.find(class_='last_contact').string.replace_with(dict_row.get('last_contact'))
        self._PAGE_TEMPLATE.find(class_='last_start').string.replace_with(dict_row.get('start'))
        self._PAGE_TEMPLATE.find(class_='last_write').string.replace_with(dict_row.get('last_write'))
        self._PAGE_TEMPLATE.find(class_='last_reject').string.replace_with(dict_row.get('last_reject'))

    def __add_global_imports_to_template_soup(self):
        dict_row = self.__EXTRACTOR.get_last_row_as_dict()
        self._PAGE_TEMPLATE.find(class_='imported_global').string.replace_with(dict_row.get('imported'))
        self._PAGE_TEMPLATE.find(class_='updated_global').string.replace_with(dict_row.get('updated'))
        self._PAGE_TEMPLATE.find(class_='invalid_global').string.replace_with(dict_row.get('invalid'))
        self._PAGE_TEMPLATE.find(class_='failed_global').string.replace_with(dict_row.get('failed'))
        self._PAGE_TEMPLATE.find(class_='error_rate_global').string.replace_with(dict_row.get('error_rate'))

    def __add_daily_imports_to_template_soup(self):
        dict_row = self.__EXTRACTOR.get_last_row_as_dict()
        self._PAGE_TEMPLATE.find(class_='imported_daily').string.replace_with(dict_row.get('daily_imported'))
        self._PAGE_TEMPLATE.find(class_='updated_daily').string.replace_with(dict_row.get('daily_updated'))
        self._PAGE_TEMPLATE.find(class_='invalid_daily').string.replace_with(dict_row.get('daily_invalid'))
        self._PAGE_TEMPLATE.find(class_='failed_daily').string.replace_with(dict_row.get('daily_failed'))
        self._PAGE_TEMPLATE.find(class_='error_rate_daily').string.replace_with(dict_row.get('daily_error_rate'))


# TODO input just id??
class TemplatePageCSVErrorWriter(TemplatePageLoader):
    __FILENAME_TABLE_ERRORS: str = 'template_table_errors.html'

    def __init__(self, path_csv: str):
        self.__EXTRACTOR = CSVExtractor(path_csv)
        self.__NUM_ERRORS = 20

    def add_node_errors_to_template_page(self, page_template: str) -> str:
        self._load_template_page_as_soup(page_template)
        self.__add_node_errors_to_template_soup()
        return str(self._PAGE_TEMPLATE)

    def __add_node_errors_to_template_soup(self):
        table_errors = self.__create_confluence_error_table()
        self._PAGE_TEMPLATE.find(class_='table_errors_body').replace_with(table_errors)

    def __create_confluence_error_table(self) -> bs4.BeautifulSoup:
        table_errors = self.__load_error_table_template_as_soup()
        list_dicts_error = self.__EXTRACTOR.get_first_rows_as_list_of_dicts(self.__NUM_ERRORS)
        list_error_rows = []
        for dict_error in list_dicts_error:
            error_row = self.__create_error_table_row(dict_error['timestamp'], dict_error['repeats'], dict_error['content'])
            list_error_rows.append(error_row)
        table_errors.find('tbody').extend(list_error_rows)
        return table_errors

    def __load_error_table_template_as_soup(self) -> bs4.BeautifulSoup:
        path_error_table = os.path.join(os.environ['CONFLUENCE_TEMPLATES_DIR'], self.__FILENAME_TABLE_ERRORS)
        return self._get_resource_as_soup(path_error_table)

    def __create_error_table_row(self, timestamp: str, repeats: str, content: str) -> Tag:
        column_timestamp = self.__create_table_row_column(timestamp, {'style': 'text-align: center;'})
        column_repeats = self.__create_table_row_column(repeats, {'style': 'text-align: center;'})
        column_content = self.__create_table_row_column(content)
        row_error = bs4.BeautifulSoup().new_tag('tr')
        row_error.extend([column_timestamp, column_repeats, column_content])
        return row_error

    @staticmethod
    def __create_table_row_column(content: str, attributes=None) -> Tag:
        attributes = {} if attributes is None else attributes
        row_column = bs4.BeautifulSoup().new_tag('td', attrs=attributes)
        row_column.append(content)
        return row_column


class TemplatePageStatusChecker(TemplatePageLoader):
    __FILENAME_STATUS: str = 'template_element_status.html'
    __WITDH_IMPORT_THRESHOLD: float = 0.25

    def __init__(self, id_node: str):
        self.__ID_NODE = id_node
        self.__DAILY_IMPORT_TRESHOLD = self.__get_daily_import_threshold_from_mapping_table(id_node)

    @staticmethod
    def __get_daily_import_threshold_from_mapping_table(id_node: str) -> str | None:
        global dict_mapping
        if 'DAILY_IMPORT_THRESHOLD' in dict_mapping[id_node]:
            return dict_mapping[id_node]['DAILY_IMPORT_THRESHOLD']
        else:
            return None

    def check_and_set_status_of_template_page(self, page_template: str) -> str:
        self._load_template_page_as_soup(page_template)
        self.__check_and_set_status_of_template_soup()
        return str(self._PAGE_TEMPLATE)

    def __check_and_set_status_of_template_soup(self):
        if self.__is_template_soup_offline():
            status = self.__create_status_element('OFFLINE', 'Red')
        elif self.__is_template_soup_not_importing():
            status = self.__create_status_element('NO IMPORTS', 'Red')
        elif self.__are_template_soup_imports_deviating():
            status = self.__create_status_element('DEVIATING IMPORTS', 'Orange')
        elif self.__is_template_soup_daily_error_rate_above_one():
            status = self.__create_status_element('HIGH ERROR RATE', 'Orange')
        else:
            status = self.__create_status_element('ONLINE', 'GREEN')
        self._PAGE_TEMPLATE.find(class_='status').replace_with(status)

    def __is_template_soup_offline(self) -> bool:
        last_contact = self._PAGE_TEMPLATE.find(class_='last_contact').string
        return self.__is_date_longer_ago_than_yesterday(last_contact)

    def __is_template_soup_not_importing(self) -> bool:
        last_write = self._PAGE_TEMPLATE.find(class_='last_write').string
        return self.__is_date_longer_ago_than_yesterday(last_write)

    def __are_template_soup_imports_deviating(self) -> bool:
        if self.__DAILY_IMPORT_TRESHOLD is not None:
            threshold = int(self.__DAILY_IMPORT_TRESHOLD)
            border_lower = threshold * (1 - self.__WITDH_IMPORT_THRESHOLD)
            border_upper = threshold * (1 + self.__WITDH_IMPORT_THRESHOLD)
            imported = int(self._PAGE_TEMPLATE.find(class_='imported_daily').string)
            updated = int(self._PAGE_TEMPLATE.find(class_='updated_daily').string)
            if border_lower <= (imported + updated) <= border_upper:
                return False
            else:
                return True
        return False

    def __is_template_soup_daily_error_rate_above_one(self) -> bool:
        error_rate = float(self._PAGE_TEMPLATE.find(class_='error_rate_daily'))
        return error_rate >= 1.0

    @staticmethod
    def __is_date_longer_ago_than_yesterday(date: str) -> bool:
        date_input = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        date_now = datetime.now()
        if date_now - date_input > timedelta(days=1):
            return True
        else:
            return False

    def __create_status_element(self, title: str, color: str) -> bs4.BeautifulSoup:
        status = self.__load_status_template_as_soup()
        status_title = self.__create_status_element_parameter('title', title)
        status_color = self.__create_status_element_parameter('colour', color)
        status.find('ac:structured-macro').extend([status_title, status_color])
        return status

    def __load_status_template_as_soup(self) -> bs4.BeautifulSoup:
        path_status = os.path.join(os.environ['CONFLUENCE_TEMPLATES_DIR'], self.__FILENAME_STATUS)
        return self._get_resource_as_soup(path_status)

    @staticmethod
    def __create_status_element_parameter(p_type: str, p_content: str) -> Tag:
        parameter = bs4.BeautifulSoup().new_tag('ac:parameter', attrs={'ac:name': p_type})
        parameter.append(p_content)
        return parameter


"""

# INIT PARENT
# CREATE NEW PAGES BY MAPPING TABLE (with jira table)
# UPDATE PAGES
# MERGE PAGES


"""

# upload_attachement()

# update element
# html.find(class_='version_template').string.replace_with('Version 2.0')

# update jira
# html.find(class_='table_jira').replace_with('abcedf')

# html = bs(content, 'html.parser')
# _ = html.find(id='it-contact').string.replace_with('12345')

# broker connection for version/rscript/python/import-scripts


"""
class ConfluencePageHandler:

    def __init__(self, id_node: str, dir_working=''):
        self.__ID_NODE = id_node
        self.__DIR_WORKING = dir_working
        self.__EXTRACTOR_STATS = self.__generate_csv_extractor(id_node, 'stats')
        self.__EXTRACTOR_ERRORS = self.__generate_csv_extractor(id_node, 'errors')
        self.__RESOURCE_FETCHER = NodeResourceFetcher(id_node)
        self.__TEMPLATE_CONFLUENCE = self.__load_project_resource_as_html_soup('template_page.html')
        global dict_mapping
        self.__COMMON_NAME = dict_mapping[id_node]['CN']
        self.__JIRA_LABELS = dict_mapping[id_node]['JIRA']
        self.__CONFLUENCE = ConfluenceConnection()
        self.upload_node_information_as_confluence_page()

    def __generate_csv_extractor(self, id_node: str, csv_category: str) -> CSVExtractor:
        """
        Builds the name of the csv according to the specifications of node_to_csv.py
        """
        id_node_spread = id_node.rjust(3, '0')
        current_year = str(datetime.now().year)
        name_csv = ''.join([id_node_spread, '_', csv_category, '_', current_year, '.csv'])
        path_csv = os.path.join(self.__DIR_WORKING, name_csv)
        return CSVExtractor(path_csv)

    @staticmethod
    def __load_project_resource_as_html_soup(name_resource: str) -> bs4.BeautifulSoup:
        dir_resources = os.environ['RESOURCES_DIR']
        path_resource = os.path.join(dir_resources, name_resource)
        with open(path_resource, 'r') as file:
            resource = file.read()
        soup = bs4.BeautifulSoup(resource, 'html.parser')
        return soup

    def upload_node_information_as_confluence_page(self):
        if not self.__CONFLUENCE.check_page_existence(self.__COMMON_NAME):
            print('create new page dummy')
            # set jira labels
        else:
            print('get page content')
        content = self.__CONFLUENCE.get_page_content('Dummy Broker-Monitor')
        print(type(content))
        # get page version
        # if newer version available
        # migrate static information

        # html = bs(test, 'html.parser')

        # fetch broker resources to page
        # fetch csv stats to page
        # fetch csv errors to page
        # create or update page

    # TODO set status

    def __put_jira_table_to_confluence_page(self, page_confluence: bs4.BeautifulSoup) -> bs4.BeautifulSoup:
        table_jira = self.__generate_jira_table_for_confluence()
        page_confluence.find(class_='table_jira').replace_with(table_jira)
        return page_confluence

    def __generate_jira_table_for_confluence(self) -> bs4.BeautifulSoup:
        query = self.__generate_jira_query()
        table = self.__load_project_resource_as_html_soup('table_jira.html')
        table = str(table).replace('[MY_JQL_QUERY]', query)
        table = bs4.BeautifulSoup(table, 'html.parser')
        return table

    def __generate_jira_query(self) -> str:
        tmp_labels = self.__JIRA_LABELS
        for idx, label in enumerate(tmp_labels):
            tmp_labels[idx] = ''.join(['Labels=\"', label, '\"'])
        query = ' OR '.join(tmp_labels)
        return query


# html.replace('[MY_JQL_QUERY]', '\"ABCD\"')
# jira_query = 'Labels="klinikum-fuerth" OR Labels="fuerth" OR Labels="fürth"'
# [MY_JQL_QUERY]
# 'Labels="klinikum-fuerth" OR Labels="fuerth" OR Labels="fürth"'
# table_jira = ''
# print(table_jira)
#
# return html

"""
class ConfluenceHandlerManager:

    def __init__(self):
        self.DIR_ROOT = os.environ['ROOT_DIR']
        global dict_mapping
        self.DICT_MAPPING = dict_mapping

    def upload_broker_node_csv_to_confluence(self):
        for id_node in self.LIST_NODE_IDS:
            self.__fetch_broker_node_stats(id_node, dir_working)
            self.__fetch_broker_node_errors(id_node, dir_working)

    def get_working_directory(self, id_node: str) -> str:
        name_folder = id_node.rjust(3, '0')
        dir_working = os.path.join(self.DIR_ROOT, name_folder)
        return dir_working
"""


def main(path_config: str, path_mapping: str):
    try:
        __init_logger()
        load_properties_file_as_environment(path_config)
        global dict_mapping
        dict_mapping = load_mapping_table_as_dict(path_mapping)
    except Exception as e:
        logging.exception(e)
    finally:
        __stop_logger()


if __name__ == '__main__':
    if len(sys.argv) == 1:
        raise SystemExit('path to config file is missing')
    if len(sys.argv) == 2:
        raise SystemExit('path to mapping file is missing')
    if len(sys.argv) > 3:
        raise SystemExit('invalid number of input arguments')
    main(sys.argv[1], sys.argv[2])
