import unittest

import bs4
from csv_to_confluence import TemplateResourceLoader
from csv_to_confluence import TemplatePageJiraTableWriter
from common import load_properties_file_as_environment


class TestTemplatePageJiraTableWriter(unittest.TestCase):
    __TEMPLATE: str = None

    @classmethod
    def setUpClass(cls):
        load_properties_file_as_environment('settings.json')

    def setUp(self):
        loader = TemplateResourceLoader()
        self.__TEMPLATE = loader.get_resource_as_string('template_page.html')

    def test_jira_table_1(self):
        writer = TemplatePageJiraTableWriter('1')
        page = writer.add_content_to_template_page(self.__TEMPLATE)
        self.__check_jira_table_element_on_page(page, 'server', 'Jira IMI UK Aachen')
        self.__check_jira_table_element_on_page(page, 'columnIds', 'issuekey,summary,issuetype,created,updated,duedate,assignee,reporter,priority,status,resolution')
        self.__check_jira_table_element_on_page(page, 'columns', 'key,summary,type,created,updated,due,assignee,reporter,priority,status,resolution')
        self.__check_jira_table_element_on_page(page, 'maximumIssues', '25')
        self.__check_jira_table_element_on_page(page, 'jqlQuery', 'project=AKTIN AND ( Labels="label1" OR Labels="label2" )')

    def test_jira_table_2(self):
        writer = TemplatePageJiraTableWriter('2')
        page = writer.add_content_to_template_page(self.__TEMPLATE)
        self.__check_jira_table_element_on_page(page, 'jqlQuery', 'project=AKTIN AND ( Labels="label3" )')

    def test_jira_table_3(self):
        writer = TemplatePageJiraTableWriter('3')
        page = writer.add_content_to_template_page(self.__TEMPLATE)
        self.__check_jira_table_element_on_page(page, 'jqlQuery', 'project=AKTIN AND Labels="empty"')

    def test_jira_table_4(self):
        writer = TemplatePageJiraTableWriter('10')
        page = writer.add_content_to_template_page(self.__TEMPLATE)
        self.__check_jira_table_element_on_page(page, 'jqlQuery', 'project=AKTIN AND Labels="empty"')

    def __check_jira_table_element_on_page(self, page: str, name_element: str, expected_value: str):
        html = bs4.BeautifulSoup(page, 'html.parser')
        jira = html.find(class_='table_jira')
        element = jira.findAll('ac:parameter', {'ac:name': name_element})
        value = element[0].string
        self.assertEqual(expected_value, value)


if __name__ == '__main__':
    unittest.main()
