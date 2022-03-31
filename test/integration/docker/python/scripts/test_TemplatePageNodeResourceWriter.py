import os
import unittest

import bs4
from csv_to_confluence import TemplatePageNodeResourceWriter
from csv_to_confluence import TemplateResourceLoader
from common import load_properties_file_as_environment


class TestTemplatePageNodeResourceWriter(unittest.TestCase):
    __DEFAULT_NODE_ID: str = '0'
    __DIR_WORKING: str = None
    __TEMPLATE: str = None

    @classmethod
    def setUpClass(cls):
        load_properties_file_as_environment('settings.json')
        cls.__DIR_WORKING = os.environ['ROOT_DIR'] if os.environ['ROOT_DIR'] else os.getcwd()

    def setUp(self):
        loader = TemplateResourceLoader()
        self.__TEMPLATE = loader.get_resource_as_string('template_page.html')

    def tearDown(self):
        [os.remove(name) for name in os.listdir(self.__DIR_WORKING) if '.txt' in name]

    def test_write_resources_into_template(self):
        self.__create_versions_resource_file()
        self.__create_rscript_resource_file()
        self.__create_python_resource_file()
        self.__create_import_scripts_resource_file()
        page = self.__write_current_resouces_into_template_page()
        self.__check_key_of_template(page, 'java', 'Ubuntu/11.0.11')
        self.__check_key_of_template(page, 'os', 'Ubuntu 20.04.2 LTS')
        self.__check_key_of_template(page, 'apache2', '2.4.41-4ubuntu3.8')
        self.__check_key_of_template(page, 'kernel', '-')
        self.__check_key_of_template(page, 'dwh-api', '0.9')
        self.__check_key_of_template(page, 'j2ee-impl', '18.0.0.Final')
        self.__check_key_of_template(page, 'postgres', '[not installed]')
        self.__check_key_of_template(page, 'dwh-j2ee', 'dwh-j2ee-1.5rc1')
        self.__check_key_of_template(page, 'rscript', 'r-base-core (3.6.3), r-cran-tidyverse (-), r-cran-lattice (-)')
        self.__check_key_of_template(page, 'python', 'python3-numpy (1:1.17.4-5ubuntu3), python3-psycopg2 (-)')
        self.__check_key_of_template(page, 'import-scripts', 'p21 (1.5), p22 (-), p23 (-), p24 (?)')

    def test_write_missing_resource_file_into_template(self):
        self.__create_python_resource_file()
        self.__create_import_scripts_resource_file()
        page = self.__write_current_resouces_into_template_page()
        self.__check_key_of_template(page, 'java', '-')
        self.__check_key_of_template(page, 'os', '-')
        self.__check_key_of_template(page, 'apache2', '-')
        self.__check_key_of_template(page, 'kernel', '-')
        self.__check_key_of_template(page, 'dwh-api', '-')
        self.__check_key_of_template(page, 'j2ee-impl', '-')
        self.__check_key_of_template(page, 'postgres', '-')
        self.__check_key_of_template(page, 'dwh-j2ee', '-')
        self.__check_key_of_template(page, 'rscript', '-')
        self.__check_key_of_template(page, 'python', 'python3-numpy (1:1.17.4-5ubuntu3), python3-psycopg2 (-)')
        self.__check_key_of_template(page, 'import-scripts', 'p21 (1.5), p22 (-), p23 (-), p24 (?)')

    def test_write_empty_resource_file_into_template(self):
        self.__create_empty_resource_file('versions')
        self.__create_empty_resource_file('rscript')
        self.__create_python_resource_file()
        self.__create_import_scripts_resource_file()
        page = self.__write_current_resouces_into_template_page()
        self.__check_key_of_template(page, 'java', '-')
        self.__check_key_of_template(page, 'os', '-')
        self.__check_key_of_template(page, 'apache2', '-')
        self.__check_key_of_template(page, 'kernel', '-')
        self.__check_key_of_template(page, 'dwh-api', '-')
        self.__check_key_of_template(page, 'j2ee-impl', '-')
        self.__check_key_of_template(page, 'postgres', '-')
        self.__check_key_of_template(page, 'dwh-j2ee', '-')
        self.__check_key_of_template(page, 'rscript', '-')
        self.__check_key_of_template(page, 'python', 'python3-numpy (1:1.17.4-5ubuntu3), python3-psycopg2 (-)')
        self.__check_key_of_template(page, 'import-scripts', 'p21 (1.5), p22 (-), p23 (-), p24 (?)')

    def __create_versions_resource_file(self):
        path_versions = self.__generate_resource_file_path('versions')
        with open(path_versions, 'w') as file:
            file.write('{"java": "Ubuntu/11.0.11", '
                       '"os": "Ubuntu 20.04.2 LTS", '
                       '"apache2": "2.4.41-4ubuntu3.8", '
                       '"kernel": "", '
                       '"dwh-api": "0.9", '
                       '"j2ee-impl": "18.0.0.Final", '
                       '"postgres": "[not installed]", '
                       '"dwh-j2ee": "dwh-j2ee-1.5rc1"}'
                       )

    def __create_rscript_resource_file(self):
        path_rscript = self.__generate_resource_file_path('rscript')
        with open(path_rscript, 'w') as file:
            file.write('{"r-base-core": "3.6.3", '
                       '"r-cran-tidyverse": "-", '
                       '"r-cran-lattice": ""}'
                       )

    def __create_python_resource_file(self):
        path_python = self.__generate_resource_file_path('python')
        with open(path_python, 'w') as file:
            file.write('{"python3-numpy": "1:1.17.4-5ubuntu3", '
                       '"python3-psycopg2": ""}'
                       )

    def __create_import_scripts_resource_file(self):
        path_scripts = self.__generate_resource_file_path('import-scripts')
        with open(path_scripts, 'w') as file:
            file.write('{"p21": "1.5", '
                       '"p22": "", '
                       '"p23": "-", '
                       '"p24": "?"}'
                       )

    def __create_empty_resource_file(self, name_resource: str):
        path_resource = self.__generate_resource_file_path(name_resource)
        with open(path_resource, 'w') as resource:
            resource.write('{}')

    def __generate_resource_file_path(self, name_resource: str) -> str:
        name_file = ''.join([self.__DEFAULT_NODE_ID, '_', name_resource, '.txt'])
        return os.path.join(self.__DIR_WORKING, name_file)

    def __check_key_of_template(self, page_template: str, key: str, value_expected: str):
        page = bs4.BeautifulSoup(page_template, 'html.parser')
        self.assertEqual(value_expected, page.find(class_=key).string)

    def __write_current_resouces_into_template_page(self) -> str:
        writer = TemplatePageNodeResourceWriter(self.__DEFAULT_NODE_ID, self.__DIR_WORKING)
        page = writer.add_content_to_template_page(self.__TEMPLATE)
        return page


if __name__ == '__main__':
    unittest.main()
