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

from src.csv_to_confluence import ConfluenceHandler


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