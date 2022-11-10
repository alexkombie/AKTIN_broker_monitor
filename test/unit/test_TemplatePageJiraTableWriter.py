import os
import sys
import unittest
from pathlib import Path

import bs4

this_path = Path(os.path.realpath(__file__))
path_src = os.path.join(this_path.parents[2], 'src')
sys.path.insert(0, path_src)

from common import PropertiesReader
from csv_to_confluence import TemplatePageLoader, TemplatePageJiraTableWriter


class TestTemplatePageJiraTableWriter(unittest.TestCase):
    __TEMPLATE: str = None

    @classmethod
    def setUpClass(cls):
        path_settings = os.path.join(this_path.parents[1], 'resources', 'settings.json')
        PropertiesReader().load_properties_as_env_vars(path_settings)
        cls.__JIRA_TABLE_WRITER = TemplatePageJiraTableWriter()

    def setUp(self):
        loader = TemplatePageLoader()
        self.__TEMPLATE = loader.get_template_page()

    def test_jira_table_1(self):
        page = self.__JIRA_TABLE_WRITER.add_content_to_template_page(self.__TEMPLATE, '1')
        self.__check_jira_table_element_on_page(page, 'server', 'Jira IMI UK Aachen')
        self.__check_jira_table_element_on_page(page, 'columnIds', 'issuekey,summary,issuetype,created,updated,duedate,assignee,reporter,priority,status,resolution')
        self.__check_jira_table_element_on_page(page, 'columns', 'key,summary,type,created,updated,due,assignee,reporter,priority,status,resolution')
        self.__check_jira_table_element_on_page(page, 'maximumIssues', '25')
        self.__check_jira_table_element_on_page(page, 'jqlQuery', 'project=AKTIN AND ( Labels="label1" OR Labels="label2" )')

    def test_jira_table_2(self):
        page = self.__JIRA_TABLE_WRITER.add_content_to_template_page(self.__TEMPLATE, '2')
        self.__check_jira_table_element_on_page(page, 'jqlQuery', 'project=AKTIN AND ( Labels="label3" )')

    def test_jira_table_3(self):
        page = self.__JIRA_TABLE_WRITER.add_content_to_template_page(self.__TEMPLATE, '3')
        self.__check_jira_table_element_on_page(page, 'jqlQuery', 'project=AKTIN AND Labels="empty"')

    def test_jira_table_4(self):
        page = self.__JIRA_TABLE_WRITER.add_content_to_template_page(self.__TEMPLATE, '10')
        self.__check_jira_table_element_on_page(page, 'jqlQuery', 'project=AKTIN AND Labels="empty"')

    def __check_jira_table_element_on_page(self, page: str, name_element: str, expected_value: str):
        html = bs4.BeautifulSoup(page, 'html.parser')
        jira = html.find(class_='table_jira')
        element = jira.findAll('ac:parameter', {'ac:name': name_element})
        value = element[0].string
        self.assertEqual(expected_value, value)


if __name__ == '__main__':
    unittest.main()
