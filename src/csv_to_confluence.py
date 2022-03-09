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

from bs4 import BeautifulSoup as bs
import os
import re
import json
import pprint

from common import CSVHandler
from common import BrokerNodeConnection
from common import load_properties_file_as_environment
from common import __init_logger
from common import __stop_logger
from common import ConfluenceConnection
from common import load_mapping_table_as_dict

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
        self.__PAGE_CONFLUENCE = dict_mapping[id_node]['CN']
        self.__CONFLUENCE = ConfluenceConnection()

    def backup_csv_files(self):
        list_csv_files = self.__get_all_csv_files_in_directory()
        for name_csv in list_csv_files:
            path_csv = os.path.join(self.__DIR_WORKING, name_csv)
            self.__CONFLUENCE.upload_csv_as_attachement_to_page(self.__PAGE_CONFLUENCE, path_csv)

    def __get_all_csv_files_in_directory(self) -> list:
        return [name_file for name_file in os.listdir(self.__DIR_WORKING) if name_file.endswith('.csv')]


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


class CSVExtractor(CSVHandler):

    def __init__(self, path_csv: str):
        super().__init__(path_csv)
        self.__DF = self.read_csv_as_df()

    def get_last_row_as_dict(self) -> dict:
        return self.__DF.iloc[-1].to_dict()

    def get_first_rows_as_list_of_dicts(self, num_rows: int) -> list[dict]:
        df = self.__DF.head(num_rows)
        return df.to_dict('records')


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
