import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime
from datetime import timedelta

import bs4
from bs4.element import Tag

from common import BrokerNodeConnection
from common import CSVHandler
from common import ConfluenceConnection
from common import __init_logger
from common import __stop_logger
from common import load_json_file_as_dict
from common import load_properties_file_as_environment
from common import SingletonMeta


class ConfluenceNodeMapper(metaclass=SingletonMeta):

    def __init__(self):
        self.__DICT_MAPPING = load_json_file_as_dict(os.environ['CONFLUENCE_MAPPING_JSON'])

    def get_mapping_dict(self) -> dict:
        return self.__DICT_MAPPING

    def get_node_from_mapping_dict(self, node: str) -> dict | None:
        if node in self.__DICT_MAPPING:
            return self.__DICT_MAPPING[node]
        else:
            return None

    def get_node_value_from_mapping_dict(self, node: str, key: str) -> str | None:
        if key in self.__DICT_MAPPING[node]:
            return self.__DICT_MAPPING[node][key]
        else:
            return None


class TemplateResourceGetter(metaclass=SingletonMeta):

    def __init__(self):
        self.__DIR_TEMPLATES = os.environ['CONFLUENCE_RESOURCES_DIR']

    def get_resource_as_string(self, name_resource: str):
        path_resource = os.path.join(self.__DIR_TEMPLATES, name_resource)
        with open(path_resource, 'r') as resource:
            content = resource.read()
        return content

    def get_resource_as_soup(self, name_resource: str):
        path_resource = os.path.join(self.__DIR_TEMPLATES, name_resource)
        with open(path_resource, 'r') as resource:
            content = resource.read()
        return bs4.BeautifulSoup(content, 'html.parser')


class TemplatePageContentWriter(ABC):
    _PAGE_TEMPLATE: bs4.BeautifulSoup

    def _add_content_to_template_page(self, page_template: str):
        self._load_template_page_into_memory(page_template)
        self._add_content_to_template_soup()
        return str(self._PAGE_TEMPLATE)

    def _load_template_page_into_memory(self, page_template: str):
        self._PAGE_TEMPLATE = bs4.BeautifulSoup(page_template, 'html.parser')

    @abstractmethod
    def _add_content_to_template_soup(self):
        pass


class NodeResourceFetcher:

    def __init__(self, id_node: str):
        self.__ID_NODE = id_node
        self.__BROKER_NODE_CONNECTION = BrokerNodeConnection()

    def fetch_broker_node_versions(self) -> dict:
        response = self.__BROKER_NODE_CONNECTION.get_broker_node_resource(self.__ID_NODE, 'versions')
        return self.__clean_dictionary(response)

    def fetch_broker_node_rscript(self) -> dict:
        response = self.__BROKER_NODE_CONNECTION.get_broker_node_resource(self.__ID_NODE, 'rscript')
        return self.__clean_dictionary(response)

    def fetch_broker_node_python(self) -> dict:
        response = self.__BROKER_NODE_CONNECTION.get_broker_node_resource(self.__ID_NODE, 'python')
        return self.__clean_dictionary(response)

    def fetch_broker_node_import_scripts(self) -> dict:
        response = self.__BROKER_NODE_CONNECTION.get_broker_node_resource(self.__ID_NODE, 'import-scripts')
        return self.__clean_dictionary(response)

    @staticmethod
    def __clean_dictionary(dictionary: dict) -> dict:
        return {key: value if value is not None else '-' for key, value in dictionary.items()}


class TemplatePageNodeResourceWriter(TemplatePageContentWriter):

    def __init__(self, id_node: str):
        self.__FETCHER = NodeResourceFetcher(id_node)

    def add_resources_to_template_page(self, page_template: str) -> str:
        return self._add_content_to_template_page(page_template)

    def _add_content_to_template_soup(self):
        self.__add_versions_to_template_soup()
        self.__add_rscript_to_template_soup()
        self.__add_python_to_template_soup()
        self.__add_import_scripts_to_template_soup()

    def __add_versions_to_template_soup(self):
        dict_versions = self.__FETCHER.fetch_broker_node_versions()
        self._PAGE_TEMPLATE.find(class_='os').string.replace_with(dict_versions.get('os'))
        self._PAGE_TEMPLATE.find(class_='kernel').string.replace_with(dict_versions.get('kernel'))
        self._PAGE_TEMPLATE.find(class_='java').string.replace_with(dict_versions.get('java'))
        self._PAGE_TEMPLATE.find(class_='wildfly').string.replace_with(dict_versions.get('j2ee-impl'))
        self._PAGE_TEMPLATE.find(class_='apache2').string.replace_with(dict_versions.get('apache2'))
        self._PAGE_TEMPLATE.find(class_='postgresql').string.replace_with(dict_versions.get('postgres'))
        self._PAGE_TEMPLATE.find(class_='dwh_api').string.replace_with(dict_versions.get('dwh-api'))
        self._PAGE_TEMPLATE.find(class_='dwh_j2ee').string.replace_with(dict_versions.get('dwh-j2ee'))

    def __add_rscript_to_template_soup(self):
        dict_rscript = self.__FETCHER.fetch_broker_node_rscript()
        rscript = self.__concat_dict_items_as_string(dict_rscript)
        self._PAGE_TEMPLATE.find(class_='rscript').string.replace_with(rscript)

    def __add_python_to_template_soup(self):
        dict_python = self.__FETCHER.fetch_broker_node_python()
        python = self.__concat_dict_items_as_string(dict_python)
        self._PAGE_TEMPLATE.find(class_='python').string.replace_with(python)

    def __add_import_scripts_to_template_soup(self):
        dict_import_scripts = self.__FETCHER.fetch_broker_node_import_scripts()
        import_scripts = self.__concat_dict_items_as_string(dict_import_scripts)
        self._PAGE_TEMPLATE.find(class_='import_scripts').string.replace_with(import_scripts)

    @staticmethod
    def __concat_dict_items_as_string(input_dict: dict) -> str:
        tmp_list = []
        for key, value in input_dict.items():
            value = '?' if value is None else value
            item = ''.join([key, ' ', '(', value, ')'])
            tmp_list.append(item)
        return ', '.join(tmp_list)


class CSVExtractor(CSVHandler):

    def __init__(self, path_csv: str):
        super().__init__(path_csv)
        self.__DF = self.read_csv_as_df()

    def get_last_row_as_dict(self) -> dict:
        return self.__DF.iloc[-1].to_dict()

    def get_first_rows_as_list_of_dicts(self, num_rows: int) -> list[dict]:
        df = self.__DF.head(num_rows)
        return df.to_dict('records')


class TemplatePageCSVInfoWriter(TemplatePageContentWriter):

    def __init__(self, path_csv: str):
        self.__EXTRACTOR = CSVExtractor(path_csv)

    def add_node_stats_to_template_page(self, page_template: str) -> str:
        return self._add_content_to_template_page(page_template)

    def _add_content_to_template_soup(self):
        self.__add_dates_to_template_soup()
        self.__add_global_imports_to_template_soup()
        self.__add_daily_imports_to_template_soup()

    def __add_dates_to_template_soup(self):
        dict_row = self.__EXTRACTOR.get_last_row_as_dict()
        self._PAGE_TEMPLATE.find(class_='last_check').string.replace_with(dict_row.get('date'))
        self._PAGE_TEMPLATE.find(class_='last_contact').string.replace_with(dict_row.get('last_contact'))
        self._PAGE_TEMPLATE.find(class_='last_start').string.replace_with(dict_row.get('start'))
        self._PAGE_TEMPLATE.find(class_='last_write').string.replace_with(dict_row.get('last_write'))
        self._PAGE_TEMPLATE.find(class_='last_reject').string.replace_with(dict_row.get('last_reject'))

    def __add_global_imports_to_template_soup(self):
        dict_row = self.__EXTRACTOR.get_last_row_as_dict()
        self._PAGE_TEMPLATE.find(class_='imported_global').string.replace_with(dict_row.get('imported'))
        self._PAGE_TEMPLATE.find(class_='updated_global').string.replace_with(dict_row.get('updated'))
        self._PAGE_TEMPLATE.find(class_='invalid_global').string.replace_with(dict_row.get('invalid'))
        self._PAGE_TEMPLATE.find(class_='failed_global').string.replace_with(dict_row.get('failed'))
        self._PAGE_TEMPLATE.find(class_='error_rate_global').string.replace_with(dict_row.get('error_rate'))

    def __add_daily_imports_to_template_soup(self):
        dict_row = self.__EXTRACTOR.get_last_row_as_dict()
        self._PAGE_TEMPLATE.find(class_='imported_daily').string.replace_with(dict_row.get('daily_imported'))
        self._PAGE_TEMPLATE.find(class_='updated_daily').string.replace_with(dict_row.get('daily_updated'))
        self._PAGE_TEMPLATE.find(class_='invalid_daily').string.replace_with(dict_row.get('daily_invalid'))
        self._PAGE_TEMPLATE.find(class_='failed_daily').string.replace_with(dict_row.get('daily_failed'))
        self._PAGE_TEMPLATE.find(class_='error_rate_daily').string.replace_with(dict_row.get('daily_error_rate'))


class TemplatePageCSVErrorWriter(TemplatePageContentWriter):
    __FILENAME_TABLE_ERRORS: str = 'template_table_errors.html'
    __NUM_ERRORS: int = 20

    def __init__(self, path_csv: str):
        self.__EXTRACTOR = CSVExtractor(path_csv)
        self.__RESOURCE_GETTER = TemplateResourceGetter()

    def add_node_errors_to_template_page(self, page_template: str) -> str:
        return self._add_content_to_template_page(page_template)

    def _add_content_to_template_soup(self):
        table_errors = self.__create_confluence_error_table()
        self._PAGE_TEMPLATE.find(class_='table_errors_body').replace_with(table_errors)

    def __create_confluence_error_table(self) -> bs4.BeautifulSoup:
        table_errors = self.__RESOURCE_GETTER.get_resource_as_soup(self.__FILENAME_TABLE_ERRORS)
        list_dicts_error = self.__EXTRACTOR.get_first_rows_as_list_of_dicts(self.__NUM_ERRORS)
        list_error_rows = []
        for dict_error in list_dicts_error:
            error_row = self.__create_error_table_row(dict_error['timestamp'], dict_error['repeats'], dict_error['content'])
            list_error_rows.append(error_row)
        table_errors.find('tbody').extend(list_error_rows)
        return table_errors

    def __create_error_table_row(self, timestamp: str, repeats: str, content: str) -> Tag:
        column_timestamp = self.__create_table_row_data(timestamp, {'style': 'text-align: center;'})
        column_repeats = self.__create_table_row_data(repeats, {'style': 'text-align: center;'})
        column_content = self.__create_table_row_data(content)
        row_error = bs4.BeautifulSoup().new_tag('tr')
        row_error.extend([column_timestamp, column_repeats, column_content])
        return row_error

    @staticmethod
    def __create_table_row_data(content: str, attributes=None) -> Tag:
        attributes = {} if attributes is None else attributes
        row_column = bs4.BeautifulSoup().new_tag('td', attrs=attributes)
        row_column.append(content)
        return row_column


class TemplatePageStatusChecker(TemplatePageContentWriter):
    __FILENAME_STATUS: str = 'template_element_status.html'
    __WITDH_IMPORT_THRESHOLD: float = 0.25

    def __init__(self, id_node: str):
        mapper = ConfluenceNodeMapper()
        self.__DAILY_IMPORT_TRESHOLD = mapper.get_node_value_from_mapping_dict(id_node, 'DAILY_IMPORT_THRESHOLD')
        self.__RESOURCE_GETTER = TemplateResourceGetter()

    def check_and_set_status_of_template_page(self, page_template: str) -> str:
        return self._add_content_to_template_page(page_template)

    def _add_content_to_template_soup(self):
        if self.__is_template_soup_offline():
            status = self.__create_status_element('OFFLINE', 'Red')
        elif self.__is_template_soup_not_importing():
            status = self.__create_status_element('NO IMPORTS', 'Red')
        elif self.__are_template_soup_imports_deviating():
            status = self.__create_status_element('DEVIATING IMPORTS', 'Orange')
        elif self.__is_template_soup_daily_error_rate_above_one():
            status = self.__create_status_element('HIGH ERROR RATE', 'Orange')
        else:
            status = self.__create_status_element('ONLINE', 'GREEN')
        self._PAGE_TEMPLATE.find(class_='status').replace_with(status)

    def __is_template_soup_offline(self) -> bool:
        last_contact = self._PAGE_TEMPLATE.find(class_='last_contact').string
        return self.__is_date_longer_ago_than_yesterday(last_contact)

    def __is_template_soup_not_importing(self) -> bool:
        last_write = self._PAGE_TEMPLATE.find(class_='last_write').string
        return self.__is_date_longer_ago_than_yesterday(last_write)

    @staticmethod
    def __is_date_longer_ago_than_yesterday(date: str) -> bool:
        date_input = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        date_now = datetime.now()
        if date_now - date_input > timedelta(days=1):
            return True
        else:
            return False

    def __are_template_soup_imports_deviating(self) -> bool:
        if self.__DAILY_IMPORT_TRESHOLD is not None:
            threshold = int(self.__DAILY_IMPORT_TRESHOLD)
            border_lower = threshold * (1 - self.__WITDH_IMPORT_THRESHOLD)
            border_upper = threshold * (1 + self.__WITDH_IMPORT_THRESHOLD)
            imported = int(self._PAGE_TEMPLATE.find(class_='imported_daily').string)
            updated = int(self._PAGE_TEMPLATE.find(class_='updated_daily').string)
            if border_lower <= (imported + updated) <= border_upper:
                return False
            else:
                return True
        return False

    def __is_template_soup_daily_error_rate_above_one(self) -> bool:
        error_rate = float(self._PAGE_TEMPLATE.find(class_='error_rate_daily'))
        return error_rate >= 1.0

    def __create_status_element(self, title: str, color: str) -> bs4.BeautifulSoup:
        status = self.__RESOURCE_GETTER.get_resource_as_soup(self.__FILENAME_STATUS)
        status_title = self.__create_status_element_parameter('title', title)
        status_color = self.__create_status_element_parameter('colour', color)
        status.find('ac:structured-macro').extend([status_title, status_color])
        return status

    @staticmethod
    def __create_status_element_parameter(p_type: str, p_content: str) -> Tag:
        parameter = bs4.BeautifulSoup().new_tag('ac:parameter', attrs={'ac:name': p_type})
        parameter.append(p_content)
        return parameter


class TemplatePageJiraTableWriter(TemplatePageContentWriter):
    __FILENAME_TABLE_JIRA: str = 'template_table_jira.html'

    def __init__(self, id_node: str):
        mapper = ConfluenceNodeMapper()
        self.__LABELS_JIRA = mapper.get_node_value_from_mapping_dict(id_node, 'JIRA_LABELS')
        self.__RESOURCE_GETTER = TemplateResourceGetter()

    def add_jira_table_to_template_page(self, page_template: str) -> str:
        return self._add_content_to_template_page(page_template)

    def _add_content_to_template_soup(self):
        table_jira = self.__generate_jira_table_for_confluence()
        self._PAGE_TEMPLATE.find(class_='table_jira').replace_with(table_jira)

    def __generate_jira_table_for_confluence(self) -> bs4.BeautifulSoup:
        query = self.__generate_jira_query_from_labels()
        table = self.__RESOURCE_GETTER.get_resource_as_soup(self.__FILENAME_TABLE_JIRA)
        table = str(table).replace('[MY_JQL_QUERY]', query)
        table = bs4.BeautifulSoup(table, 'html.parser')
        return table

    def __generate_jira_query_from_labels(self) -> str:
        tmp_labels = []
        for label in self.__LABELS_JIRA:
            tmp_labels.append(''.join(['Labels=\"', label, '\"']))
        query = ' OR '.join(tmp_labels)
        return query


class TemplatePageMigrator(TemplatePageContentWriter):
    __FILENAME_TEMPLATE_PAGE: str = 'template_page.html'

    def __init__(self):
        self.__RESOURCE_GETTER = TemplateResourceGetter()

    def is_template_page_outdated(self, page_template: str) -> bool:
        template_new = self.__RESOURCE_GETTER.get_resource_as_soup(self.__FILENAME_TEMPLATE_PAGE)
        version_new = template_new.find(class_='version_template').string
        template_old = bs4.BeautifulSoup(page_template, 'html.parser')
        version_old = template_old.find(class_='version_template').string
        return version_new == version_old

    def migrate_template_to_newer_version(self, page_template: str) -> str:
        return self._add_content_to_template_page(page_template)

    def _add_content_to_template_soup(self):
        template_new = self.__RESOURCE_GETTER.get_resource_as_soup(self.__FILENAME_TEMPLATE_PAGE)
        template_new = self.__migrate_clinic_information_to_new_template(template_new)
        template_new = self.__migrate_id_information_to_new_template(template_new)
        self._PAGE_TEMPLATE = template_new

    def __migrate_clinic_information_to_new_template(self, soup: bs4.BeautifulSoup) -> bs4.BeautifulSoup:
        soup = self.__migrate_key_to_new_template('clinic_name', soup)
        soup = self.__migrate_key_to_new_template('clinic_since', soup)
        soup = self.__migrate_key_to_new_template('information_system', soup)
        soup = self.__migrate_key_to_new_template('interface_import', soup)
        soup = self.__migrate_key_to_new_template('contact_ed', soup)
        soup = self.__migrate_key_to_new_template('contact_it', soup)
        return soup

    def __migrate_id_information_to_new_template(self, soup: bs4.BeautifulSoup) -> bs4.BeautifulSoup:
        soup = self.__migrate_key_to_new_template('root_patient', soup)
        soup = self.__migrate_key_to_new_template('format_patient', soup)
        soup = self.__migrate_key_to_new_template('root_encounter', soup)
        soup = self.__migrate_key_to_new_template('format_encounter', soup)
        soup = self.__migrate_key_to_new_template('root_billing', soup)
        soup = self.__migrate_key_to_new_template('format_billing', soup)
        return soup

    def __migrate_key_to_new_template(self, key_class: str, soup: bs4.BeautifulSoup) -> bs4.BeautifulSoup:
        value = self._PAGE_TEMPLATE.find(class_=key_class).string
        soup.find(class_=key_class).string.replace_with(value)
        return soup


class ConfluencePageHandler:
    __FILENAME_TEMPLATE_PAGE: str = 'template_page.html'

    def __init__(self, id_node: str, dir_working=''):
        mapper = ConfluenceNodeMapper()
        self.__COMMON_NAME = mapper.get_node_value_from_mapping_dict(id_node, 'COMMON')
        self.__CONFLUENCE = ConfluenceConnection()
        self.__CONFLUENCE_PARENT_PAGE = os.environ['CONFLUENCE_PARENT_PAGE']
        self.__RESOURCE_GETTER = TemplateResourceGetter()
        self.__NODE_RESOURCE_WRITER = TemplatePageNodeResourceWriter(id_node)
        path_csv_info = self.__genereate_csv_path(id_node, dir_working, '_stats_')
        self.__CSV_INFO_WRITER = TemplatePageCSVInfoWriter(path_csv_info)
        path_csv_error = self.__genereate_csv_path(id_node, dir_working, '_errors_')
        self.__CSV_ERROR_WRITER = TemplatePageCSVErrorWriter(path_csv_error)
        self.__STATUS_CHECKER = TemplatePageStatusChecker(id_node)
        self.__JIRA_TABLE_WRITER = TemplatePageJiraTableWriter(id_node)
        self.__TEMPLATE_MIGRATOR = TemplatePageMigrator()

    @staticmethod
    def __genereate_csv_path(id_node: str, dir_working: str, type_csv: str) -> str:
        # Builds the name of the csv path according to the specifications of node_to_csv.py
        # type_csv shall be _stats_ or _errors_
        id_node_spread = id_node.rjust(3, '0')
        current_year = str(datetime.now().year)
        name_csv = ''.join([id_node_spread, type_csv, current_year, '.csv'])
        return os.path.join(dir_working, name_csv)

    def upload_node_information_as_confluence_page(self):
        if not self.__CONFLUENCE.check_page_existence(self.__COMMON_NAME):
            page = self.__generate_new_page_template()
            self.__CONFLUENCE.create_confluence_page(self.__COMMON_NAME, self.__CONFLUENCE_PARENT_PAGE, page)
        else:
            page = self.__CONFLUENCE.get_page_content(self.__COMMON_NAME)
            if self.__TEMPLATE_MIGRATOR.is_template_page_outdated(page):
                page = self.__TEMPLATE_MIGRATOR.migrate_template_to_newer_version(page)
        page = self.__write_content_to_page_template(page)
        self.__CONFLUENCE.update_confluence_page(self.__COMMON_NAME, page)

    def __generate_new_page_template(self):
        template = self.__RESOURCE_GETTER.get_resource_as_string(self.__FILENAME_TEMPLATE_PAGE)
        template = self.__JIRA_TABLE_WRITER.add_jira_table_to_template_page(template)
        return template

    def __write_content_to_page_template(self, template: str) -> str:
        template = self.__NODE_RESOURCE_WRITER.add_resources_to_template_page(template)
        template = self.__CSV_INFO_WRITER.add_node_stats_to_template_page(template)
        template = self.__CSV_ERROR_WRITER.add_node_errors_to_template_page(template)
        template = self.__STATUS_CHECKER.check_and_set_status_of_template_page(template)
        return template


class CSVBackupManager:

    def __init__(self, id_node: str, dir_working=''):
        self.__DIR_WORKING = dir_working
        mapper = ConfluenceNodeMapper()
        self.__COMMON_NAME = mapper.get_node_value_from_mapping_dict(id_node, 'COMMON')
        self.__CONFLUENCE = ConfluenceConnection()

    def backup_csv_files(self):
        list_csv_files = self.__get_all_csv_files_in_directory()
        for name_csv in list_csv_files:
            path_csv = os.path.join(self.__DIR_WORKING, name_csv)
            self.__CONFLUENCE.upload_csv_as_attachement_to_page(self.__COMMON_NAME, path_csv)

    def __get_all_csv_files_in_directory(self) -> list:
        return [name_file for name_file in os.listdir(self.__DIR_WORKING) if name_file.endswith('.csv')]


class ConfluencePageHandlerManager:
    __FILENAME_PARENT_PAGE: str = 'parent_page.html'
    __CONFLUENCE_ROOT_PAGE_NAME = 'Support'

    def __init__(self):
        mapper = ConfluenceNodeMapper()
        self.__DICT_MAPPING = mapper.get_mapping_dict()
        self.__DIR_ROOT = os.environ['ROOT_DIR']
        self.__CONFLUENCE = ConfluenceConnection()
        self.__CONFLUENCE_PARENT_PAGE = os.environ['CONFLUENCE_PARENT_PAGE']
        self.__RESOURCE_GETTER = TemplateResourceGetter()
        self.__init_parent_page()

    def __init_parent_page(self):
        if not self.__CONFLUENCE.check_page_existence(self.__CONFLUENCE_PARENT_PAGE):
            page_parent = self.__RESOURCE_GETTER.get_resource_as_string(self.__FILENAME_PARENT_PAGE)
            self.__CONFLUENCE.create_confluence_page(self.__CONFLUENCE_PARENT_PAGE, self.__CONFLUENCE_ROOT_PAGE_NAME, page_parent)

    def upload_csv_files_as_confluence_pages(self):
        for id_node in self.__DICT_MAPPING.keys():
            dir_working = self.__get_working_dir(id_node)
            handler = ConfluencePageHandler(id_node, dir_working)
            handler.upload_node_information_as_confluence_page()
            backup = CSVBackupManager(id_node, dir_working)
            backup.backup_csv_files()

    def __get_working_dir(self, id_node: str) -> str:
        name_folder = id_node.rjust(3, '0')
        return os.path.join(self.__DIR_ROOT, name_folder)


def main(path_config: str):
    try:
        __init_logger()
        load_properties_file_as_environment(path_config)
        manager = ConfluencePageHandlerManager()
        manager.upload_csv_files_as_confluence_pages()
    except Exception as e:
        logging.exception(e)
    finally:
        __stop_logger()
