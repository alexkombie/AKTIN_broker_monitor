# -*- coding: utf-8 -*
"""
Created on 14.02.2024
@AUTHOR=WILIAM HOY (whoy@ukaachen.de)
@VERSION=1.3
"""
#
#      Copyright (c) 2022  AKTIN
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


import os
from abc import ABC, ABCMeta
from atlassian import Confluence  # use "pip install atlassian-python-api" to install atlassian (used pip 24.0)
import json


# Implementation of classes needed from the original "common.py"
class SingletonMeta(type):
    """
    Meta class to make python classes a Singleton
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class SingletonABCMeta(ABCMeta):
    """
    Meta class to make abstract python classes a Singleton -> All implementing classes
    are automatically Singletons
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonABCMeta, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class ConfluenceNodeMapper(metaclass=SingletonMeta):
    """
    Maps the ID of a broker node to a JSON file with information related to a Confluence page,
    such as institution name and Jira query labels.
    """

    def __init__(self):
        path_mapping_json = os.getenv('CONFLUENCE.MAPPING_JSON')
        self.__dict_mapping = self.__load_json_file_as_dict(path_mapping_json)

    @staticmethod
    def __load_json_file_as_dict(filepath: str) -> dict:
        with open(filepath, encoding='utf-8') as json_file:
            return json.load(json_file)

    def get_all_keys(self) -> list:
        return list(self.__dict_mapping.keys())

    def get_node_from_mapping_dict(self, node_id: str) -> dict:
        return self.__dict_mapping.get(node_id)

    def get_node_value_from_mapping_dict(self, node_id: str, key: str):
        node_mapping = self.__dict_mapping.get(node_id)
        if node_mapping:
            return node_mapping.get(key)
        return None


class ConfluenceConnection(metaclass=SingletonMeta):
    """
    Uses Atlassian Python API to execute CRUD operations on Confluence
    """

    def __init__(self):
        """
        Confluence connection is created on initialization
        """
        confluence_url = os.getenv('CONFLUENCE.URL')
        confluence_token = os.getenv('CONFLUENCE.TOKEN')
        self.__space = os.getenv('CONFLUENCE.SPACE')
        self.__confluence = Confluence(url=confluence_url, token=confluence_token)

    def does_page_exists(self, pagename: str) -> bool:
        return self.__confluence.page_exists(self.__space, pagename)

    def get_page_content(self, pagename: str) -> str:
        page_id = self.__confluence.get_page_id(self.__space, pagename)
        page = self.__confluence.get_page_by_id(page_id, expand='body.storage')
        content = page['body']['storage']['value']
        return content

    def upload_file_as_attachement_to_page(self, pagename: str, filepath: str):
        """
        Identical named files are automatically replaced on confluence
        """
        page_id = self.__confluence.get_page_id(self.__space, pagename)
        self.__confluence.attach_file(filepath, content_type='text/csv', page_id=page_id)

    def create_confluence_page(self, pagename: str, parentname: str, content: str):
        parent_id = self.__confluence.get_page_id(self.__space, parentname)
        self.__confluence.create_page(self.__space, pagename, content, parent_id=parent_id)

    def update_confluence_page(self, pagename: str, content: str):
        page_id = self.__confluence.get_page_id(self.__space, pagename)
        self.__confluence.update_page(page_id, pagename, content)


# Implementation of classes needed from the original "csv_to_confluence.py"
class ConfluenceHandler(ABC, metaclass=SingletonABCMeta):
    _confluence_root_page: str = 'Support'
    _confluence_parent_page: str = 'Support Log Broker-Monitor'

    def __init__(self):
        self._mapper = ConfluenceNodeMapper()
        self._confluence = ConfluenceConnection()


class FileBackupManager(ConfluenceHandler):
    """
    Backs up all files of the corresponding broker node ID on its confluence page.
    Identically named attachments are overwritten when uploaded to Confluence.
    """

    def __init__(self):
        super().__init__()
        self.__working_dir = os.getenv('DIR.WORKING')

    def backup_files(self, node_id: str):
        """
        Backs up files of the specified broker node ID by uploading them as attachments to the Confluence page.
        """
        self.__backup_files_with_line_ending(node_id, 'csv')
        self.__backup_files_with_line_ending(node_id, 'txt')
        self.__backup_files_with_line_ending(node_id, 'log')

    def __backup_files_with_line_ending(self, node_id: str, line_ending: str):
        node_dir = os.path.join(self.__working_dir, node_id)
        files_list = self.__get_all_files_in_directory_with_line_ending(node_dir, line_ending)
        name = self._mapper.get_node_value_from_mapping_dict(node_id, 'COMMON_NAME')
        for filename in files_list:
            filepath = os.path.join(node_dir, filename)
            self._confluence.upload_file_as_attachement_to_page(name, filepath)

    @staticmethod
    def __get_all_files_in_directory_with_line_ending(directory: str, line_ending: str) -> list:
        return [name_file for name_file in os.listdir(directory) if name_file.endswith(line_ending)]
