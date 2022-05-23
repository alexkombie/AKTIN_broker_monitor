# -*- coding: utf-8 -*
# Created on Mon May 23 12:00 2022
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
from datetime import datetime
from datetime import timedelta

from common import ConfluenceConnection
from common import ConfluenceNodeMapper
from common import CSVHandler
from common import InfoCSVHandler
from common import ErrorCSVHandler
from common import SingletonMeta
from common import __init_logger
from common import __stop_logger
from common import load_properties_file_as_environment


class MailServerConnection(metaclass=SingletonMeta):
    pass
    # verbindet sich mit Email Server
    # geht in settings rein um infos zu holen
    # os.environ und so weiter


class MailSender(metaclass=SingletonMeta):
    pass
    # bekommt template, bekommt adressanten
    # sendet email dorthin
    # braucht MailServerConnection


class MailTemplateLoader(metaclass=SingletonMeta):

    def __init__(self):
        self.__DIR_TEMPLATES = os.environ['RESOURCES_DIR']

    def get_resource_as_string(self, name_resource: str) -> str:
        path_resource = os.path.join(self.__DIR_TEMPLATES, name_resource)
        with open(path_resource, 'r') as resource:
            content = resource.read()
        return content

    # lädt template resource
    # befüllt resource mit infos


class ConfluencePageInfoExtractor(metaclass=SingletonMeta):
    pass
    # gibt status einer seite zurück
    # gibt liste von email adressanten zurück (IT)
    # gibt liste von email adressanten zurück (ED)
    # braucht input id
    # self.__DICT_MAPPING = self.__MAPPER.get_mapping_dict()
    # self.__CONFLUENCE = ConfluenceConnection()


class NodeEventNotifier:
    pass


class NodeEventNotifierManager:

    def __init__(self):
        self.__MAPPER = ConfluenceNodeMapper()
        self.__DICT_MAPPING = self.__MAPPER.get_mapping_dict()
        self.__CONFLUENCE = ConfluenceConnection()

    def dummy(self):
        for id_node in self.__DICT_MAPPING.keys():
            page_name = self.__MAPPER.get_node_value_from_mapping_dict(id_node, 'COMMON')
            page = self.__CONFLUENCE.get_page_content(page_name)
            pass
            # check page for status
            # if status OFFLINE or NO IMPORTS
            # get email contacts from page
            # filter contacts by !
            # load template for status
            # fill template with info
            # send template via email server to recipients


def main(path_config: str):
    try:
        __init_logger()
        load_properties_file_as_environment(path_config)
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
