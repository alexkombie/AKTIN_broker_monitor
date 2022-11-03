import os
import unittest
from shutil import rmtree

import bs4
from common import PropertiesReader
from csv_to_confluence import TemplatePageLoader, TemplatePageNodeResourceWriter


class TestTemplatePageNodeResourceWriter(unittest.TestCase):
    __DEFAULT_NODE_ID: str = '0'
    __DIR_ROOT: str = None
    __TEMPLATE: str = None

    @classmethod
    def setUpClass(cls):
        PropertiesReader().load_properties_as_env_vars('settings.json')
        cls.__DIR_ROOT = os.environ['ROOT_DIR'] if os.environ['ROOT_DIR'] else os.getcwd()
        cls.__NODE_RESOURCE_WRITER = TemplatePageNodeResourceWriter()

    def setUp(self):
        loader = TemplatePageLoader()
        self.__TEMPLATE = loader.get_template_page()
        dir_working = os.path.join(self.__DIR_ROOT, self.__DEFAULT_NODE_ID)
        if not os.path.exists(dir_working):
            os.makedirs(dir_working)

    def tearDown(self):
        [rmtree(name) for name in os.listdir(self.__DIR_ROOT) if os.path.isdir(name) and len(name) <= 2]

    def test_write_resources_into_template(self):
        self.__create_versions_resource_file()
        self.__create_rscript_resource_file()
        self.__create_python_resource_file()
        self.__create_import_scripts_resource_file()
        page = self.__NODE_RESOURCE_WRITER.add_content_to_template_page(self.__TEMPLATE, self.__DEFAULT_NODE_ID)
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
        page = self.__NODE_RESOURCE_WRITER.add_content_to_template_page(self.__TEMPLATE, self.__DEFAULT_NODE_ID)
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
        page = self.__NODE_RESOURCE_WRITER.add_content_to_template_page(self.__TEMPLATE, self.__DEFAULT_NODE_ID)
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
        return os.path.join(self.__DIR_ROOT, self.__DEFAULT_NODE_ID, name_file)

    def __check_key_of_template(self, page_template: str, key: str, value_expected: str):
        page = bs4.BeautifulSoup(page_template, 'html.parser')
        self.assertEqual(value_expected, page.find(class_=key).string)


if __name__ == '__main__':
    unittest.main()
