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

import pprint

from common import CSVHandler
from common import BrokerNodeConnection
from common import load_properties_file_as_environment
from common import __init_logger
from common import __stop_logger

from atlassian import Confluence

SPACE = 'AKTIN'
URL_CONFLUENCE = 'https://confluence-imi.ukaachen.de'
TOKEN = 'CHANGEME'

confluence = Confluence(
        url=URL_CONFLUENCE,
        token=TOKEN)


# CREATE NEW PAGES BY MAPPING TABLE
# UPDATE PAGES
# CREATE BACKUP
# confluence.page_exists(SPACE, 'Dummy Broker-Monitor')

def get_page():
    page_id = confluence.get_page_id(SPACE, 'Dummy Broker-Monitor')
    page = confluence.get_page_by_id(page_id, expand='body.storage')
    content = page['body']['storage']['value']
    print(content)


def init_new_page():
    page_id = confluence.get_page_id(SPACE, 'Dummy Broker-Monitor')
    page = confluence.get_page_by_id(page_id, expand='body.storage')
    current_folder = os.getcwd()
    path_html = os.path.join(current_folder, 'resources', 'template_page.html')
    with open(path_html, 'r') as file:
        html = file.read()
    page['body']['storage']['value'] = html
    confluence.update_page(page_id, page['title'], html)


def update_page():
    page_id = confluence.get_page_id(SPACE, 'Dummy Broker-Monitor')
    page = confluence.get_page_by_id(page_id, expand='body.storage')
    content = page['body']['storage']['value']
    html = bs(content, 'html.parser')
    confluence.update_page(page_id, page['title'], str(html))


def upload_attachement():
    page_id = confluence.get_page_id(SPACE, 'Dummy Broker-Monitor')
    current_folder = os.getcwd()
    path_html = os.path.join(current_folder, 'resources', 'template_page.html')
    confluence.attach_file(path_html, content_type='text/html', page_id=page_id)


def delete_attachement():
    page_id = confluence.get_page_id(SPACE, 'Dummy Broker-Monitor')
    confluence.delete_attachment(page_id, 'template_page.html')


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

# !!!!!!!!!!!!!!!!!! send csv as attachements

# !!!!!!!!!!!!!!!!!! update template if different version



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


def main(path_config: str):
    try:
        __init_logger()
        load_properties_file_as_environment(path_config)
        # TODO
    except Exception as e:
        logging.exception(e)
    finally:
        __stop_logger()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise SystemExit('please give path to config file')
    main(sys.argv[1])
