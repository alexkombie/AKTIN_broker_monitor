import os
import sys
import unittest
from pathlib import Path

import bs4

this_path = Path(os.path.realpath(__file__))
path_src = os.path.join(this_path.parents[2], 'src')
sys.path.insert(0, path_src)

from csv_to_confluence import TemplatePageElementCreator


class TestTemplatePageElementCreator(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.__ELEMENT_CREATOR = TemplatePageElementCreator()

    def test_create_ac_parameter_element(self):
        element = self.__ELEMENT_CREATOR.create_ac_parameter_element('name', 'content')
        expected = '<ac:parameter ac:name="name">content</ac:parameter>'
        self.assertEqual(bs4.Tag, type(element))
        self.assertEqual(expected, str(element))

    def test_create_ac_parameter_element_empty(self):
        element = self.__ELEMENT_CREATOR.create_ac_parameter_element('', '')
        expected = '<ac:parameter ac:name=""></ac:parameter>'
        self.assertEqual(bs4.Tag, type(element))
        self.assertEqual(expected, str(element))

    def test_create_ac_parameter_element_none(self):
        with self.assertRaises(ValueError):
            _ = self.__ELEMENT_CREATOR.create_ac_parameter_element(None, None)

    def test_create_ac_macro_element(self):
        element = self.__ELEMENT_CREATOR.create_ac_macro_element('name')
        expected = '<ac:structured-macro ac:name="name" ac:schema-version="1"></ac:structured-macro>'
        self.assertEqual(bs4.Tag, type(element))
        self.assertEqual(expected, str(element))

    def test_create_ac_link_element(self):
        element = self.__ELEMENT_CREATOR.create_ac_link_element('name_page')
        expected = '<ac:link><ri:page ri:content-title="name_page"></ri:page></ac:link>'
        self.assertEqual(bs4.BeautifulSoup, type(element))
        self.assertEqual(expected, str(element))

    def test_create_table_header_element(self):
        element = self.__ELEMENT_CREATOR.create_table_header_element('name')
        expected = '<th style="text-align: center;">name</th>'
        self.assertEqual(bs4.Tag, type(element))
        self.assertEqual(expected, str(element))

    def test_create_table_data_element(self):
        element = self.__ELEMENT_CREATOR.create_table_data_element('name')
        expected = '<td>name</td>'
        self.assertEqual(bs4.Tag, type(element))
        self.assertEqual(expected, str(element))

    def test_create_centered_table_data_element(self):
        element = self.__ELEMENT_CREATOR.create_table_data_element('name', True)
        expected = '<td style="text-align: center;">name</td>'
        self.assertEqual(bs4.Tag, type(element))
        self.assertEqual(expected, str(element))

    def test_create_html_element(self):
        element = self.__ELEMENT_CREATOR.create_html_element('p')
        expected = '<p></p>'
        self.assertEqual(bs4.Tag, type(element))
        self.assertEqual(expected, str(element))

    def test_create_html_element_with_attributes(self):
        element = self.__ELEMENT_CREATOR.create_html_element('p', {'class': 'classname', 'style': 'stylename'})
        expected = '<p class="classname" style="stylename"></p>'
        self.assertEqual(bs4.Tag, type(element))
        self.assertEqual(expected, str(element))

    def test_convert_element_to_soup(self):
        element = '<p></p>'
        self.assertEqual(str, type(element))
        element2 = self.__ELEMENT_CREATOR.convert_element_to_soup(element)
        self.assertEqual(bs4.BeautifulSoup, type(element2))


if __name__ == '__main__':
    unittest.main()
