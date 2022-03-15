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

from common import CSVHandler
from common import BrokerNodeConnection
from common import load_properties_file_as_environment
from common import __init_logger
from common import __stop_logger
from common import ConfluenceConnection
from common import load_json_file_as_dict

"""

# CREATE NEW PAGES BY MAPPING TABLE
# UPDATE PAGES
# CREATE BACKUP

"""


# upload_attachement()

# update element
# html.find(class_='version_template').string.replace_with('Version 2.0')

# update jira
# html.find(class_='table_jira').replace_with('abcedf')

# html = bs(content, 'html.parser')
# _ = html.find(id='it-contact').string.replace_with('12345')

# broker connection for version/rscript/python/import-scripts

# needs mapping settings (node number to common name)

# for each node folder in workdir:
# check if in aktin confluence exists page:
# else create page:
# grap content:
# extract info
# update own temp html with content
# update confluence page

# !!!!!!!!!!!!!!!!!! update template if different version

class CSVBackupManager:

    def __init__(self, id_node: str, dir_working=''):
        self.__DIR_WORKING = dir_working
        global dict_mapping
        self.__COMMON_NAME = dict_mapping[id_node]['CN']
        self.__CONFLUENCE = ConfluenceConnection()

    def backup_csv_files(self):
        list_csv_files = self.__get_all_csv_files_in_directory()
        for name_csv in list_csv_files:
            path_csv = os.path.join(self.__DIR_WORKING, name_csv)
            self.__CONFLUENCE.upload_csv_as_attachement_to_page(self.__COMMON_NAME, path_csv)

    def __get_all_csv_files_in_directory(self) -> list:
        return [name_file for name_file in os.listdir(self.__DIR_WORKING) if name_file.endswith('.csv')]


class TemplatePageLoader(ABC):
    _PAGE_TEMPLATE: bs4.BeautifulSoup

    def _load_template_page_as_soup(self, page_template: str):
        self._PAGE_TEMPLATE = bs4.BeautifulSoup(page_template, 'html.parser')


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


class TemplatePageCSVInfoWriter(TemplatePageWriter):

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


class TemplatePageCSVErrorWriter(TemplatePageLoader):

    def __init__(self, path_csv: str):
        self.__EXTRACTOR = CSVExtractor(path_csv)
        self.__NUM_ERRORS = 20

    def add_node_errors_to_template_page(self, page_template: str) -> str:
        self._load_template_page_as_soup(page_template)
        self.__add_node_errors_to_template_soup()
        return str(self._PAGE_TEMPLATE)

    def __load_template_page_as_soup(self, page_template: str):
        self._PAGE_TEMPLATE = bs4.BeautifulSoup(page_template, 'html.parser')


# TODO  Status setter


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
