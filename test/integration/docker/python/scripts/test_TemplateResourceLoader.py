import os.path
import unittest

import bs4
from csv_to_confluence import TemplateResourceLoader
from common import load_properties_file_as_environment


class TestTemplateResourceLoader(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        load_properties_file_as_environment('settings.json')
        cls.__TEMPLATE_RESOURCE_LOADER = TemplateResourceLoader()

    def test_get_resource_as_string(self):
        resource = self.__TEMPLATE_RESOURCE_LOADER.get_resource_as_string('template_page.html')
        self.assertEqual(str, type(resource))

    def test_get_resource_as_soup(self):
        resource = self.__TEMPLATE_RESOURCE_LOADER.get_resource_as_soup('template_page.html')
        self.assertEqual(bs4.BeautifulSoup, type(resource))

    def test_get_nonexisting_resource_as_soup(self):
        with self.assertRaises(FileNotFoundError):
            _ = self.__TEMPLATE_RESOURCE_LOADER.get_resource_as_soup('nonexisting')


if __name__ == '__main__':
    unittest.main()
