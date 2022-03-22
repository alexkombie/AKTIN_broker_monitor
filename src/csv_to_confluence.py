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


class CSVBackupManager:

    def __init__(self, id_node: str, dir_working=''):
        self.__DIR_WORKING = dir_working
        global dict_mapping
        self.__COMMON_NAME = dict_mapping[id_node]['COMMON']
        self.__CONFLUENCE = ConfluenceConnection()

    def backup_csv_files(self):
        list_csv_files = self.__get_all_csv_files_in_directory()
        for name_csv in list_csv_files:
            path_csv = os.path.join(self.__DIR_WORKING, name_csv)
            self.__CONFLUENCE.upload_csv_as_attachement_to_page(self.__COMMON_NAME, path_csv)

    def __get_all_csv_files_in_directory(self) -> list:
        return [name_file for name_file in os.listdir(self.__DIR_WORKING) if name_file.endswith('.csv')]


class TemplateResourceGetter(metaclass=SingletonMeta):

    def __init__(self):
        self.__DIR_TEMPLATES = os.environ['CONFLUENCE_TEMPLATES_DIR']

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
        return self.__BROKER_NODE_CONNECTION.get_broker_node_resource(self.__ID_NODE, 'versions')

    def fetch_broker_node_rscript(self) -> dict:
        return self.__BROKER_NODE_CONNECTION.get_broker_node_resource(self.__ID_NODE, 'rscript')

    def fetch_broker_node_python(self) -> dict:
        return self.__BROKER_NODE_CONNECTION.get_broker_node_resource(self.__ID_NODE, 'python')

    def fetch_broker_node_import_scripts(self) -> dict:
        return self.__BROKER_NODE_CONNECTION.get_broker_node_resource(self.__ID_NODE, 'import-scripts')


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
        self.__DAILY_IMPORT_TRESHOLD = self.__get_daily_import_threshold_from_mapping_table(id_node)
        self.__RESOURCE_GETTER = TemplateResourceGetter()

    @staticmethod
    def __get_daily_import_threshold_from_mapping_table(id_node: str) -> str | None:
        global dict_mapping
        if 'DAILY_IMPORT_THRESHOLD' in dict_mapping[id_node]:
            return dict_mapping[id_node]['DAILY_IMPORT_THRESHOLD']
        else:
            return None

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
        global dict_mapping
        self.__LABELS_JIRA = dict_mapping[id_node]['JIRA_LABELS']
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
        for idx, label in enumerate(self.__LABELS_JIRA):
            tmp_labels[idx] = ''.join(['Labels=\"', label, '\"'])
        query = ' OR '.join(tmp_labels)
        return query


def main(path_config: str):
    try:
        __init_logger()
        load_properties_file_as_environment(path_config)
        global dict_mapping
        dict_mapping = load_json_file_as_dict(os.environ['CONFLUENCE_MAPPING_JSON'])

        path_html = os.path.join(os.environ['CONFLUENCE_TEMPLATES_DIR'], 'template_page.html')
        with open(path_html, 'r') as file:
            html2 = file.read()

        c1 = TemplatePageStatusChecker('2')
        c1.check_and_set_status_of_template_page(html2)
        a = c1.create_status_element('ONLINE', 'Green')

        print(a)
    except Exception as e:
        logging.exception(e)
    finally:
        __stop_logger()
