# -*- coding: utf-8 -*
# Created on Tue Mar 22 12:00 2022
# @version: 1.2

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
from itertools import islice, tee

import bs4
import pandas as pd
from bs4.element import Tag
from packaging import version

from common import ConfluenceConnection, ConfluenceNodeMapper, CSVHandler, ErrorCSVHandler, InfoCSVHandler, MyLogger, \
    PropertiesReader, ResourceLoader, SingletonABCMeta, SingletonMeta, TimestampHandler
from my_error_notifier import MyErrorNotifier


class TemplatePageElementCreator(metaclass=SingletonMeta):
    """
    Creates commonly used html and confluence elements. Is also used to convert string
    template of confluence page to searchable html soup
    """
    __PARSER: str = 'html.parser'

    def create_ac_parameter_element(self, name: str, content: str) -> Tag:
        parameter = bs4.BeautifulSoup(features=self.__PARSER).new_tag('ac:parameter', attrs={'ac:name': name})
        parameter.append(content)
        return parameter

    def create_ac_macro_element(self, name: str) -> Tag:
        attributes = {'ac:name': name, 'ac:schema-version': '1'}
        macro = bs4.BeautifulSoup(features=self.__PARSER).new_tag('ac:structured-macro', attrs=attributes)
        return macro

    def create_ac_link_element(self, name_page: str) -> bs4.BeautifulSoup:
        """
        bs4 seems not to be supporting self-closing HTML-Tags
        """
        link = '<ac:link><ri:page ri:content-title="{0}" /></ac:link>'.format(name_page)
        link = self.convert_element_to_soup(link)
        return link

    def create_table_header_element(self, name_header: str) -> Tag:
        th = bs4.BeautifulSoup(features=self.__PARSER).new_tag('th', attrs={'style': 'text-align: center;'})
        th.append(name_header)
        return th

    def create_table_data_element(self, content: str, centered=False) -> Tag:
        attributes = {'style': 'text-align: center;'} if centered is True else {}
        td = bs4.BeautifulSoup(features=self.__PARSER).new_tag('td', attrs=attributes)
        td.append(content)
        return td

    def create_html_element(self, type_element: str, attributes=None) -> Tag:
        attributes = {} if attributes is None else attributes
        return bs4.BeautifulSoup(features=self.__PARSER).new_tag(type_element, attrs=attributes)

    def convert_element_to_soup(self, element) -> bs4.BeautifulSoup:
        return bs4.BeautifulSoup(str(element), self.__PARSER)


class TemplatePageLoader(ResourceLoader):
    __FILENAME_TEMPLATE_PAGE: str = 'template_page.html'
    __ENCODING: str = 'utf-8'

    def get_template_page(self) -> str:
        return self._get_resource_as_string(self.__FILENAME_TEMPLATE_PAGE, self.__ENCODING)


class TemplatePageCSVContentWriter(ABC, metaclass=SingletonABCMeta):
    """
    Used to write content from csv file to confluence page
    """
    _CSV_HANDLER: CSVHandler

    def __init__(self):
        self._DIR_ROOT = os.environ['ROOT_DIR']
        self._ID_NODE = None
        self._PAGE_TEMPLATE = None
        self._DF = None

    def add_content_to_template_page(self, page_template: str, id_node: str) -> str:
        self._ID_NODE = id_node
        self._PAGE_TEMPLATE = bs4.BeautifulSoup(page_template, 'html.parser')
        dir_working = os.path.join(self._DIR_ROOT, id_node)
        self._DF = self.__load_csv_as_df(id_node, dir_working)
        self._add_content_to_template_soup()
        return str(self._PAGE_TEMPLATE)

    def __load_csv_as_df(self, id_node: str, dir_working: str) -> pd.DataFrame:
        name_csv = self._CSV_HANDLER.generate_csv_name(id_node)
        path_csv = os.path.join(dir_working, name_csv)
        return self._CSV_HANDLER.read_csv_as_df(path_csv)

    @abstractmethod
    def _add_content_to_template_soup(self):
        pass


class TemplatePageCSVInfoWriter(TemplatePageCSVContentWriter):
    """
    Writes content of info csv (of a single node) into predefined elements of the template
    """

    def __init__(self):
        super().__init__()
        self._CSV_HANDLER = InfoCSVHandler()

    def _add_content_to_template_soup(self):
        self.__add_dates_to_template_soup()
        self.__add_weekly_imports_to_template_soup()
        self.__add_daily_imports_to_template_soup()

    def __add_dates_to_template_soup(self):
        dict_last_row = self._DF.iloc[-1].to_dict()
        self._PAGE_TEMPLATE.find(class_='last_check').string.replace_with(dict_last_row.get('date'))
        self._PAGE_TEMPLATE.find(class_='last_contact').string.replace_with(dict_last_row.get('last_contact'))
        self._PAGE_TEMPLATE.find(class_='last_start').string.replace_with(dict_last_row.get('last_start'))
        self._PAGE_TEMPLATE.find(class_='last_write').string.replace_with(dict_last_row.get('last_write'))
        self._PAGE_TEMPLATE.find(class_='last_reject').string.replace_with(dict_last_row.get('last_reject'))

    @staticmethod
    def __get_mean_of_series(series: pd.Series) -> str:
        return str(series.astype(float).mean().round(2))

    def __add_weekly_imports_to_template_soup(self):
        last_week = self._DF.tail(7)
        imported = self.__get_mean_of_series(last_week['imported'])
        updated = self.__get_mean_of_series(last_week['updated'])
        invalid = self.__get_mean_of_series(last_week['invalid'])
        failed = self.__get_mean_of_series(last_week['failed'])
        error_rate = self.__get_mean_of_series(last_week['error_rate'])
        self._PAGE_TEMPLATE.find(class_='imported').string.replace_with(imported)
        self._PAGE_TEMPLATE.find(class_='updated').string.replace_with(updated)
        self._PAGE_TEMPLATE.find(class_='invalid').string.replace_with(invalid)
        self._PAGE_TEMPLATE.find(class_='failed').string.replace_with(failed)
        self._PAGE_TEMPLATE.find(class_='error_rate').string.replace_with(error_rate)

    def __add_total_imports_to_template_soup(self):
        dict_last_row = self._DF.iloc[-1].to_dict()
        self._PAGE_TEMPLATE.find(class_='imported').string.replace_with(dict_last_row.get('imported'))
        self._PAGE_TEMPLATE.find(class_='updated').string.replace_with(dict_last_row.get('updated'))
        self._PAGE_TEMPLATE.find(class_='invalid').string.replace_with(dict_last_row.get('invalid'))
        self._PAGE_TEMPLATE.find(class_='failed').string.replace_with(dict_last_row.get('failed'))
        self._PAGE_TEMPLATE.find(class_='error_rate').string.replace_with(dict_last_row.get('error_rate'))

    def __add_daily_imports_to_template_soup(self):
        dict_last_row = self._DF.iloc[-1].to_dict()
        self._PAGE_TEMPLATE.find(class_='daily_imported').string.replace_with(dict_last_row.get('daily_imported'))
        self._PAGE_TEMPLATE.find(class_='daily_updated').string.replace_with(dict_last_row.get('daily_updated'))
        self._PAGE_TEMPLATE.find(class_='daily_invalid').string.replace_with(dict_last_row.get('daily_invalid'))
        self._PAGE_TEMPLATE.find(class_='daily_failed').string.replace_with(dict_last_row.get('daily_failed'))
        self._PAGE_TEMPLATE.find(class_='daily_error_rate').string.replace_with(dict_last_row.get('daily_error_rate'))


class TemplatePageCSVErrorWriter(TemplatePageCSVContentWriter):
    """
    CWrites content of error csv (of a single node) into predefined elements of the template
    """
    __NUM_ERRORS: int = 20

    def __init__(self):
        super().__init__()
        self._CSV_HANDLER = ErrorCSVHandler()
        self.__ELEMENT_CREATOR = TemplatePageElementCreator()

    def _add_content_to_template_soup(self):
        table_errors = self.__create_confluence_error_table()
        self._PAGE_TEMPLATE.find(class_='table_errors_body').replace_with(table_errors)

    def __create_confluence_error_table(self) -> Tag:
        list_dicts_error = self._DF.head(self.__NUM_ERRORS).to_dict('records')
        list_error_rows = []
        for error in list_dicts_error:
            row = self.__create_error_table_row(error['timestamp'], error['repeats'], error['content'])
            list_error_rows.append(row)
        table = self.__generate_empty_error_table()
        table.find('tbody').extend(list_error_rows)
        return table

    def __create_error_table_row(self, timestamp: str, repeats: str, content: str) -> Tag:
        column_timestamp = self.__ELEMENT_CREATOR.create_table_data_element(timestamp, centered=True)
        column_repeats = self.__ELEMENT_CREATOR.create_table_data_element(repeats, centered=True)
        column_content = self.__ELEMENT_CREATOR.create_table_data_element(content)
        row = self.__ELEMENT_CREATOR.create_html_element('tr')
        row.extend([column_timestamp, column_repeats, column_content])
        return row

    def __generate_empty_error_table(self) -> bs4.BeautifulSoup:
        header_error = self.__create_error_table_header()
        table = self.__ELEMENT_CREATOR.create_html_element('tbody', {'class': 'table_errors_body'})
        table.append(header_error)
        table = self.__ELEMENT_CREATOR.convert_element_to_soup(table)
        return table

    def __create_error_table_header(self) -> Tag:
        header_timestamp = self.__ELEMENT_CREATOR.create_table_header_element('timestamp')
        header_repeats = self.__ELEMENT_CREATOR.create_table_header_element('repeats')
        header_content = self.__ELEMENT_CREATOR.create_table_header_element('content')
        header = self.__ELEMENT_CREATOR.create_html_element('tr')
        header.extend([header_timestamp, header_repeats, header_content])
        return header


class TemplatePageStatusChecker(TemplatePageCSVContentWriter):
    """
    Checks import and connection status inside (template of) confluence page and sets
    status as a custom html element.
    Should always be the last class called in the processing pipeline!
    """

    def __init__(self):
        super().__init__()
        self._CSV_HANDLER = InfoCSVHandler()
        self.__ELEMENT_CREATOR = TemplatePageElementCreator()
        self.__TIMESTAMP_HANDLER = TimestampHandler()
        self.__MAPPER = ConfluenceNodeMapper()

    def __append_last_year_rows_to_df_if_necessary(self):
        """
        If the csv has less than 4 rows, try to append the last 10 rows of last years csv
        to this csv
        """
        if len(self._DF) < 4:
            year_current = self.__TIMESTAMP_HANDLER.get_current_year()
            year_last_year = str(int(year_current) - 1)
            name_csv_last_year = self._CSV_HANDLER.generate_csv_name_with_custom_year(self._ID_NODE, year_last_year)
            path_csv_last_year = os.path.join(self._DIR_ROOT, self._ID_NODE, name_csv_last_year)
            if os.path.isfile(path_csv_last_year):
                df_last_year = self._CSV_HANDLER.read_csv_as_df(path_csv_last_year)
                df_last_year = df_last_year.iloc[-10:]
                self._DF = pd.concat([df_last_year, self._DF], ignore_index=True)

    def _add_content_to_template_soup(self):
        self.__append_last_year_rows_to_df_if_necessary()
        if self.__has_csv_a_gap_in_broker_connection():
            status = self.__create_status_element('GAP IN MONITORING', 'Red')
        elif self.__is_template_soup_still_testing():
            status = self.__create_status_element('TESTING', 'Blue')
        elif self.__is_template_soup_offline():
            status = self.__create_status_element('OFFLINE', 'Red')
        elif self.__is_template_soup_not_importing():
            status = self.__create_status_element('NO IMPORTS', 'Red')
        elif self.__is_template_soup_daily_error_rate_above_threshold(10.0):
            status = self.__create_status_element('EXTREME ERROR RATE', 'Red')
        elif self.__is_template_soup_daily_error_rate_above_threshold(5.0):
            status = self.__create_status_element('HIGH ERROR RATE', 'Yellow')
        elif self.__is_template_soup_daily_error_rate_above_threshold(1.0):
            status = self.__create_status_element('LOW ERROR RATE', 'Yellow')
        else:
            status = self.__create_status_element('ONLINE', 'Green')
        self._PAGE_TEMPLATE.find(class_='status').replace_with(status)

    def __has_csv_a_gap_in_broker_connection(self) -> bool:
        """
        Each day a row is appended to the csv file with the current import stats.
        If the broker connection fails, no row is appended and a gap appears, which can be detected.
        """
        series = self._DF['date']
        if series.empty:
            return False
        csv_today = series.iloc[-1]
        date_current = self.__TIMESTAMP_HANDLER.get_current_date()
        delta = self.__TIMESTAMP_HANDLER.get_timedelta_in_absolute_hours(date_current, csv_today)
        if delta > 24:
            return True
        else:
            if len(series) >= 2:
                csv_yesterday = series.iloc[-2]
                delta2 = self.__TIMESTAMP_HANDLER.get_timedelta_in_absolute_hours(csv_yesterday, csv_today)
                if delta2 > 24:
                    return True
        return False

    def __is_template_soup_still_testing(self) -> bool:
        """
        Node is still testing if it does not have 3 consecutive days of imports
        """
        series = self._DF['daily_imported']
        if len(series) < 3:
            return False
        series = series.str.replace('-', '0')
        series = pd.to_numeric(series)
        iterators = tee(series, 3)
        for idx, it in enumerate(iterators):
            next(islice(it, idx, idx), None)
        slices_consecutive_imports = (iterslice for iterslice in zip(*iterators) if all(x > 0 for x in iterslice))
        return not any(slices_consecutive_imports)

    def __is_template_soup_offline(self) -> bool:
        last_contact = self._PAGE_TEMPLATE.find(class_='last_contact').string
        return self.__is_date_longer_ago_than_set_hours(last_contact)

    def __is_date_longer_ago_than_set_hours(self, date_input: str) -> bool:
        threshold_hours = self.__MAPPER.get_node_value_from_mapping_dict(self._ID_NODE, 'THRESHOLD_HOURS_FAILURE')
        if not threshold_hours or threshold_hours is None:
            threshold_hours = 24
        date_input = self.__TIMESTAMP_HANDLER.get_YMD_HMS_from_date_string(date_input)
        date_now = self.__TIMESTAMP_HANDLER.get_current_date()
        delta = self.__TIMESTAMP_HANDLER.get_timedelta_in_absolute_hours(date_input, date_now)
        if delta > threshold_hours:
            return True
        else:
            return False

    def __is_template_soup_not_importing(self) -> bool:
        last_write = self._PAGE_TEMPLATE.find(class_='last_write').string
        if last_write == '-':
            return True
        return self.__is_date_longer_ago_than_set_hours(last_write)

    def __is_template_soup_daily_error_rate_above_threshold(self, threshold: float) -> bool:
        error_rate = self._PAGE_TEMPLATE.find(class_='daily_error_rate').string
        if error_rate == '-':
            return False
        return float(error_rate) >= threshold

    def __create_status_element(self, title: str, color: str) -> Tag:
        param_title = self.__ELEMENT_CREATOR.create_ac_parameter_element('title', title)
        param_color = self.__ELEMENT_CREATOR.create_ac_parameter_element('color', color)
        frame = self.__ELEMENT_CREATOR.create_ac_macro_element('status')
        frame.extend([param_title, param_color])
        status = self.__ELEMENT_CREATOR.create_html_element('td', {'style': 'text-align:center;', 'class': 'status'})
        status.append(frame)
        return status


class TemplatePageMonitoringStartDateWriter(TemplatePageCSVContentWriter):
    """
    Just adds monitoring start date for corresponding node to confluence page
    """

    def __init__(self):
        super().__init__()
        self._CSV_HANDLER = InfoCSVHandler()
        self.__ELEMENT_CREATOR = TemplatePageElementCreator()
        self.__TIMESTAMP_HANDLER = TimestampHandler()

    def _add_content_to_template_soup(self):
        first_monitoring = self._DF['date'].iloc[0]
        start_monitoring = self.__TIMESTAMP_HANDLER.get_YMD_from_date_string(first_monitoring)
        element_time = self.__ELEMENT_CREATOR.create_html_element('time', {'datetime': start_monitoring})
        td = self.__ELEMENT_CREATOR.create_html_element('td', {'class': 'online_since'})
        td.append(element_time)
        self._PAGE_TEMPLATE.find(class_='online_since').replace_with(td)


class TemplatePageContentWriter(ABC, metaclass=SingletonABCMeta):
    """
    Used to write (non-csv) content to confluence page
    """

    def __init__(self):
        self.__DIR_ROOT = os.environ['ROOT_DIR']
        self._ID_NODE = None
        self._DIR_WORKING = None
        self._PAGE_TEMPLATE = None

    def add_content_to_template_page(self, page_template: str, id_node: str) -> str:
        self._ID_NODE = id_node
        self._DIR_WORKING = os.path.join(self.__DIR_ROOT, id_node)
        self._PAGE_TEMPLATE = bs4.BeautifulSoup(page_template, 'html.parser')
        self._add_content_to_template_soup()
        return str(self._PAGE_TEMPLATE)

    @abstractmethod
    def _add_content_to_template_soup(self):
        pass


class TemplatePageNodeResourceWriter(TemplatePageContentWriter):
    """
    Writes information about node resources from text file to confluence page.

    Content of resource 'versions' is more static than the other resources. Therefore, each value is
    written into a predefined element. The other resources are just concatted.
    """

    def _add_content_to_template_soup(self):
        self.__add_versions_to_template_soup()
        self.__add_rscript_to_template_soup()
        self.__add_python_to_template_soup()
        self.__add_import_scripts_to_template_soup()

    def __add_versions_to_template_soup(self):
        versions = self.__load_node_resource_as_dict('versions')
        self._PAGE_TEMPLATE.find(class_='os').string.replace_with(self.__get_value_of_dict(versions, 'os'))
        self._PAGE_TEMPLATE.find(class_='kernel').string.replace_with(self.__get_value_of_dict(versions, 'kernel'))
        self._PAGE_TEMPLATE.find(class_='java').string.replace_with(self.__get_value_of_dict(versions, 'java'))
        self._PAGE_TEMPLATE.find(class_='j2ee-impl').string.replace_with(
            self.__get_value_of_dict(versions, 'j2ee-impl'))
        self._PAGE_TEMPLATE.find(class_='apache2').string.replace_with(self.__get_value_of_dict(versions, 'apache2'))
        self._PAGE_TEMPLATE.find(class_='postgres').string.replace_with(self.__get_value_of_dict(versions, 'postgres'))
        self._PAGE_TEMPLATE.find(class_='dwh-api').string.replace_with(self.__get_value_of_dict(versions, 'dwh-api'))
        self._PAGE_TEMPLATE.find(class_='dwh-j2ee').string.replace_with(self.__get_value_of_dict(versions, 'dwh-j2ee'))

    def __add_rscript_to_template_soup(self):
        dict_rscript = self.__load_node_resource_as_dict('rscript')
        rscript = self.__concat_dict_items_as_string(dict_rscript)
        self._PAGE_TEMPLATE.find(class_='rscript').string.replace_with(rscript)

    def __add_python_to_template_soup(self):
        dict_python = self.__load_node_resource_as_dict('python')
        python = self.__concat_dict_items_as_string(dict_python)
        self._PAGE_TEMPLATE.find(class_='python').string.replace_with(python)

    def __add_import_scripts_to_template_soup(self):
        dict_import_scripts = self.__load_node_resource_as_dict('import-scripts')
        import_scripts = self.__concat_dict_items_as_string(dict_import_scripts)
        self._PAGE_TEMPLATE.find(class_='import-scripts').string.replace_with(import_scripts)

    def __load_node_resource_as_dict(self, name_resource: str) -> dict:
        name_file = ''.join([self._ID_NODE, '_', name_resource, '.txt'])
        path_file = os.path.join(self._DIR_WORKING, name_file)
        if not os.path.exists(path_file):
            return {}
        with open(path_file) as file:
            resource = json.load(file)
        return resource

    @staticmethod
    def __get_value_of_dict(dictionary: dict, key: str) -> str:
        if key not in dictionary:
            return '-'
        value = dictionary[key]
        if not value:
            return '-'
        return value

    @staticmethod
    def __concat_dict_items_as_string(dictionary: dict) -> str:
        tmp_list = []
        if not dictionary:
            return '-'
        for key, value in dictionary.items():
            value = '-' if not value else value
            item = ''.join([key, ' ', '(', value, ')'])
            tmp_list.append(item)
        return ', '.join(tmp_list)


class TemplatePageJiraTableWriter(TemplatePageContentWriter):
    """
    Creates a jira query with values set in mapping json (see ConfluenceNodeMapper) for
    single broker node. Table for jira tickets is added as a custom confluence html element
    """

    def __init__(self):
        super().__init__()
        self.__MAPPER = ConfluenceNodeMapper()
        self.__ELEMENT_CREATOR = TemplatePageElementCreator()

    def _add_content_to_template_soup(self):
        labels_jira = self.__MAPPER.get_node_value_from_mapping_dict(self._ID_NODE, 'JIRA_LABELS')
        if labels_jira is not None and labels_jira:
            query = self.__generate_jira_query_from_labels(labels_jira)
        else:
            query = 'project=AKTIN AND Labels=\"empty\"'
        table = self.__generate_jira_table_with_query(query)
        self._PAGE_TEMPLATE.find(class_='table_jira').replace_with(table)

    @staticmethod
    def __generate_jira_query_from_labels(labels_jira: str) -> str:
        tmp_labels = []
        for label in labels_jira:
            tmp_labels.append(''.join(['Labels=\"', label, '\"']))
        query = ' OR '.join(tmp_labels)
        query = ' '.join(['project=AKTIN', 'AND', '(', query, ')'])
        return query

    def __generate_jira_table_with_query(self, query: str) -> Tag:
        param_server = self.__ELEMENT_CREATOR.create_ac_parameter_element('server', 'Jira IMI UK Aachen')
        param_id_columns = self.__ELEMENT_CREATOR.create_ac_parameter_element(
            'columnIds',
            'issuekey,summary,issuetype,created,updated,duedate,assignee,reporter,priority,status,resolution')
        param_columns = self.__ELEMENT_CREATOR.create_ac_parameter_element(
            'columns',
            'key,summary,type,created,updated,due,assignee,reporter,priority,status,resolution')
        param_max_issues = self.__ELEMENT_CREATOR.create_ac_parameter_element('maximumIssues', '25')
        param_query = self.__ELEMENT_CREATOR.create_ac_parameter_element('jqlQuery', query)
        frame = self.__ELEMENT_CREATOR.create_ac_macro_element('jira')
        frame.extend([param_server, param_id_columns, param_columns, param_max_issues, param_query])
        jira = self.__ELEMENT_CREATOR.create_html_element('p', {'class': 'table_jira'})
        jira.append(frame)
        return jira


class ConfluenceClinicContactGrabber(TemplatePageContentWriter):
    """
    Searches another confluence page form correspondants of broker node id
    Correspondants are written as a table into the template
    """
    __CONFLUENCE_EMAIL_LIST: str = 'E-Mail-Verteiler'

    def __init__(self):
        super().__init__()
        self.__DF_EMAIL = self.__grab_email_contacts_dataframe()
        self.__ELEMENT_CREATOR = TemplatePageElementCreator()

    def __grab_email_contacts_dataframe(self) -> pd.DataFrame:
        confluence = ConfluenceConnection()
        page_confluence_email = confluence.get_page_content(self.__CONFLUENCE_EMAIL_LIST)
        tables_email = pd.read_html(page_confluence_email)
        df = tables_email[0]
        df['Node ID'] = pd.to_numeric(df['Node ID'])
        df = df.fillna('')
        return df

    def _add_content_to_template_soup(self):
        table_it = self.__generate_contact_table_for_contact_type('IT')
        self._PAGE_TEMPLATE.find(class_='contact_it').replace_with(table_it)
        table_ed = self.__generate_contact_table_for_contact_type('Notaufnahme')
        self._PAGE_TEMPLATE.find(class_='contact_ed').replace_with(table_ed)

    def __generate_contact_table_for_contact_type(self, contact_type: str) -> Tag:
        contacts = self.__get_contacts_for_contact_type(contact_type)
        rows_contact = []
        for name, email in contacts.items():
            row_contact = self.__generate_contact_row(name, email)
            rows_contact.append(row_contact)
        table = self.__generate_contact_table_frame(contact_type)
        table.find('tbody').extend(rows_contact)
        return table

    def __get_contacts_for_contact_type(self, contact_type: str) -> dict:
        """
        contact_type must be either 'IT' or 'Notaufnahme'
        """
        list_idx = self.__DF_EMAIL.index[self.__DF_EMAIL['Node ID'] == int(self._ID_NODE)].tolist()
        df_node = self.__DF_EMAIL.iloc[list_idx]
        df_contacts = df_node[df_node['Ansprechpartner für'] == contact_type]
        contacts = {}
        for row in df_contacts.iterrows():
            row = row[1]
            name = ' '.join([row['Titel'], row['Vorname'], row['Nachname']])
            name = name.strip()
            contacts[name] = row['Kontakt']
        return contacts

    def __generate_contact_row(self, name: str, email: str) -> Tag:
        td_name = self.__ELEMENT_CREATOR.create_table_data_element(name, centered=True)
        link_mail = self.__ELEMENT_CREATOR.create_html_element('a', {'href': ':'.join(['mailto', email])})
        link_mail.append(email)
        td_mail = self.__ELEMENT_CREATOR.create_table_data_element(link_mail)
        tr = self.__ELEMENT_CREATOR.create_html_element('tr')
        tr.append(td_name)
        tr.append(td_mail)
        return tr

    def __generate_contact_table_frame(self, contact_type: str) -> bs4.BeautifulSoup:
        if contact_type == 'IT':
            classname = 'contact_it'
        elif contact_type == 'Notaufnahme':
            classname = 'contact_ed'
        else:
            raise SystemExit('invalid contact type')
        tbody = self.__ELEMENT_CREATOR.create_html_element('tbody')
        table = self.__ELEMENT_CREATOR.create_html_element('table', {'class': 'wrapped'})
        table.append(tbody)
        td = self.__ELEMENT_CREATOR.create_html_element('td', {'class': classname})
        td.append(table)
        return td


class TemplatePageClinicInfoWriter(TemplatePageContentWriter):
    """
    Writes (mostly) static information about the clinic to confluence page
    """

    def __init__(self):
        super().__init__()
        self.__MAPPER = ConfluenceNodeMapper()
        self.__ELEMENT_CREATOR = TemplatePageElementCreator()

    def _add_content_to_template_soup(self):
        self.__add_value_from_mapping_to_page_template('LONG_NAME', 'clinic_name')
        self.__add_value_from_mapping_to_page_template('HOSPITAL_INFORMATION_SYSTEM', 'information_system')
        self.__add_value_from_mapping_to_page_template('IMPORT_INTERFACE', 'interface_import')
        self.__add_clinic_ids('ROOT')
        self.__add_clinic_ids('FORMAT')

    def __add_value_from_mapping_to_page_template(self, key_mapping: str, key_page: str):
        value = self.__MAPPER.get_node_value_from_mapping_dict(self._ID_NODE, key_mapping)
        if not value or value is None:
            value = 'changeme'
        self._PAGE_TEMPLATE.find(class_=key_page).string.replace_with(value)

    def __add_clinic_ids(self, type_ids: str):
        dict_ids = self.__MAPPER.get_node_value_from_mapping_dict(self._ID_NODE, type_ids)
        for key in ['PATIENT', 'ENCOUNTER', 'BILLING']:
            value = 'changeme'
            if dict_ids is not None and key in dict_ids:
                value = dict_ids[key]
            self._PAGE_TEMPLATE.find(class_='_'.join([type_ids.lower(), key.lower()])).string.replace_with(value)


class TemplatePageMigrator:
    """
    Migrates information which is only set once in confluence page (like monitoring start date)
    to newer version of corresponding template
    """

    def __init__(self):
        self.__LOADER = TemplatePageLoader()
        self.__ELEMENT_CREATOR = TemplatePageElementCreator()

    def is_template_page_outdated(self, page_template: str) -> bool:
        current_page_template = self.__LOADER.get_template_page()
        template_new = self.__ELEMENT_CREATOR.convert_element_to_soup(current_page_template)
        version_new = template_new.find(class_='version_template').string
        template_old = bs4.BeautifulSoup(page_template, 'html.parser')
        version_old = template_old.find(class_='version_template').string
        return version.parse(version_new) > version.parse(version_old)

    def migrate_page_template_to_newer_version(self, page_template: str) -> str:
        current_page_template = self.__LOADER.get_template_page()
        template_new = self.__ELEMENT_CREATOR.convert_element_to_soup(current_page_template)
        template_old = bs4.BeautifulSoup(page_template, 'html.parser')
        template_new = self.__migrate_key_from_old_to_new_template('online_since', template_old, template_new)
        return str(template_new)

    @staticmethod
    def __migrate_key_from_old_to_new_template(key: str, soup_old: bs4.BeautifulSoup,
                                               soup_new: bs4.BeautifulSoup) -> bs4.BeautifulSoup:
        value = soup_old.find(class_=key)
        soup_new.find(class_=key).replace_with(value)
        return soup_new


class ConfluenceHandler(ABC, metaclass=SingletonABCMeta):
    _CONFLUENCE_ROOT_PAGE: str = 'Support'
    _CONFLUENCE_PARENT_PAGE: str = 'Support Log Broker-Monitor'

    def __init__(self):
        self._CONFLUENCE = ConfluenceConnection()
        self._MAPPER = ConfluenceNodeMapper()


class ConfluencePageHandler(ConfluenceHandler):
    """
    Creates new confluence page for single broker node. The name of a confluence
    page is its common name (from confluence node mapping json)
    """

    def __init__(self):
        super().__init__()
        self.__LOADER = TemplatePageLoader()
        self.__CLINIC_INFO_WRITER = TemplatePageClinicInfoWriter()
        self.__START_DATE_WRITER = TemplatePageMonitoringStartDateWriter()
        self.__MIGRATOR = TemplatePageMigrator()
        self.__CSV_INFO_WRITER = TemplatePageCSVInfoWriter()
        self.__CSV_ERROR_WRITER = TemplatePageCSVErrorWriter()
        self.__NODE_RESOURCE_WRITER = TemplatePageNodeResourceWriter()
        self.__JIRA_TABLE_WRITER = TemplatePageJiraTableWriter()
        self.__PAGE_STATUS_CHECKER = TemplatePageStatusChecker()
        self.__CONTACT_GRABBER = ConfluenceClinicContactGrabber()

    def upload_node_information_as_confluence_page(self, id_node: str):
        common_name = self._MAPPER.get_node_value_from_mapping_dict(id_node, 'COMMON_NAME')
        if not self._CONFLUENCE.does_page_exists(common_name):
            page = self.__LOADER.get_template_page()
            page = self.__START_DATE_WRITER.add_content_to_template_page(page, id_node)
            self._CONFLUENCE.create_confluence_page(common_name, self._CONFLUENCE_PARENT_PAGE, page)
        page = self._CONFLUENCE.get_page_content(common_name)
        if self.__MIGRATOR.is_template_page_outdated(page):
            page = self.__MIGRATOR.migrate_page_template_to_newer_version(page)
        page = self.__write_content_to_page_template(page, id_node)
        self._CONFLUENCE.update_confluence_page(common_name, page)

    def __write_content_to_page_template(self, template: str, id_node: str) -> str:
        template = self.__CLINIC_INFO_WRITER.add_content_to_template_page(template, id_node)
        template = self.__CONTACT_GRABBER.add_content_to_template_page(template, id_node)
        template = self.__CSV_INFO_WRITER.add_content_to_template_page(template, id_node)
        template = self.__CSV_ERROR_WRITER.add_content_to_template_page(template, id_node)
        template = self.__NODE_RESOURCE_WRITER.add_content_to_template_page(template, id_node)
        template = self.__JIRA_TABLE_WRITER.add_content_to_template_page(template, id_node)
        template = self.__PAGE_STATUS_CHECKER.add_content_to_template_page(template, id_node)
        return template


class FileBackupManager(ConfluenceHandler):
    """
    Backups all files of corresponding broker node id on its confluence page.
    Identical named attachements are overwritten when uploaded to confluence.
    """

    def __init__(self):
        super().__init__()
        self.__DIR_ROOT = os.environ['ROOT_DIR']

    def backup_files(self, id_node: str):
        self.__backup_files_with_line_ending(id_node, 'csv')
        self.__backup_files_with_line_ending(id_node, 'txt')
        self.__backup_files_with_line_ending(id_node, 'log')

    def __backup_files_with_line_ending(self, id_node: str, line_ending: str):
        dir_node = os.path.join(self.__DIR_ROOT, id_node)
        list_files = self.__get_all_files_in_directory_with_line_ending(dir_node, line_ending)
        name = self._MAPPER.get_node_value_from_mapping_dict(id_node, 'COMMON_NAME')
        for name_file in list_files:
            path_csv = os.path.join(dir_node, name_file)
            self._CONFLUENCE.upload_csv_as_attachement_to_page(name, path_csv)

    @staticmethod
    def __get_all_files_in_directory_with_line_ending(directory: str, line_ending: str) -> list:
        return [name_file for name_file in os.listdir(directory) if name_file.endswith(line_ending)]


class SummaryTableCreator:

    def __init__(self):
        self.__ELEMENT_CREATOR = TemplatePageElementCreator()

    def create_summary_table_frame(self) -> bs4.BeautifulSoup:
        colgroup = self.__ELEMENT_CREATOR.create_html_element('colgroup')
        col = self.__ELEMENT_CREATOR.create_html_element('col')
        colgroup.append(col)
        table = self.__ELEMENT_CREATOR.create_html_element('table')
        table.append(colgroup)
        return table

    def create_empty_summary_table(self) -> bs4.BeautifulSoup:
        header = self.__create_summary_table_header()
        table_summary = self.__ELEMENT_CREATOR.create_html_element('tbody', {'class': 'table_summary_body'})
        table_summary.append(header)
        table_summary = self.__ELEMENT_CREATOR.convert_element_to_soup(table_summary)
        return table_summary

    def __create_summary_table_header(self) -> Tag:
        node = self.__ELEMENT_CREATOR.create_table_header_element('Node')
        interface = self.__ELEMENT_CREATOR.create_table_header_element('Interface')
        status = self.__ELEMENT_CREATOR.create_table_header_element('Status')
        last_check = self.__ELEMENT_CREATOR.create_table_header_element('Letzter Check')
        error_rate_today = self.__ELEMENT_CREATOR.create_table_header_element('Fehlerrate heute')
        error_rate_last_week = self.__ELEMENT_CREATOR.create_table_header_element('Fehlerrate über 7 Tage')
        header = self.__ELEMENT_CREATOR.create_html_element('tr')
        header.extend([node, interface, status, last_check, error_rate_today, error_rate_last_week])
        return header

    def create_summary_table_row_from_confluence_page(self, name_common: str, page_confluence: str) -> Tag:
        template = self.__ELEMENT_CREATOR.convert_element_to_soup(page_confluence)
        link_node = self.__ELEMENT_CREATOR.create_ac_link_element(name_common)
        node = self.__ELEMENT_CREATOR.create_html_element('td', {'style': 'text-align: left;'})
        node.append(link_node)
        element_status = template.find(class_='status')
        interface = self.__create_table_data_from_page_template_key(template, 'interface_import')
        status = self.__ELEMENT_CREATOR.create_table_data_element(element_status.contents[0], centered=True)
        last_check = self.__create_table_data_from_page_template_key(template, 'last_check')
        error_rate_today = self.__create_table_data_from_page_template_key(template, 'daily_error_rate')
        error_rate_last_week = self.__create_table_data_from_page_template_key(template, 'error_rate')
        row = self.__ELEMENT_CREATOR.create_html_element('tr')
        row.extend([node, interface, status, last_check, error_rate_today, error_rate_last_week])
        return row

    def __create_table_data_from_page_template_key(self, template_page: bs4.BeautifulSoup, key: str) -> Tag:
        value = template_page.find(class_=key).string
        td = self.__ELEMENT_CREATOR.create_table_data_element(value, centered=True)
        return td


class ConfluencePageHandlerManager(ConfluenceHandler):
    """
    Initializes and executes ConfluencePageHandlers for each broker node. Creates a parent page
    with summarization of all connected nodes
    """

    def __init__(self):
        super().__init__()
        self.__DIR_ROOT = os.environ['ROOT_DIR']
        self.__HANDLER = ConfluencePageHandler()
        self.__SUMMARY = SummaryTableCreator()
        self.__BACKUP = FileBackupManager()
        self.__init_parent_page()

    def __init_parent_page(self):
        if not self._CONFLUENCE.does_page_exists(self._CONFLUENCE_PARENT_PAGE):
            self._CONFLUENCE.create_confluence_page(self._CONFLUENCE_PARENT_PAGE, self._CONFLUENCE_ROOT_PAGE, "")

    def upload_node_information_as_confluence_pages(self):
        id_nodes = self._MAPPER.get_all_keys()
        for id_node in id_nodes:
            dir_working = os.path.join(self.__DIR_ROOT, id_node)
            if os.path.isdir(dir_working):
                self.__HANDLER.upload_node_information_as_confluence_page(id_node)
                self.__BACKUP.backup_files(id_node)
            else:
                logging.info('Directory for id %s not found. Skipping...' % id_node)

    def upload_summary_for_confluence_pages(self):
        id_nodes = self._MAPPER.get_all_keys()
        tbody = self.__SUMMARY.create_empty_summary_table()
        for id_node in id_nodes:
            common_name = self._MAPPER.get_node_value_from_mapping_dict(id_node, 'COMMON_NAME')
            if self._CONFLUENCE.does_page_exists(common_name):
                page = self._CONFLUENCE.get_page_content(common_name)
                row = self.__SUMMARY.create_summary_table_row_from_confluence_page(common_name, page)
                tbody.find('tbody').append(row)
        table = self.__SUMMARY.create_summary_table_frame()
        table.append(tbody)
        self._CONFLUENCE.update_confluence_page(self._CONFLUENCE_PARENT_PAGE, str(table))


def main(path_config: str):
    logger = MyLogger()
    reader = PropertiesReader()
    try:
        logger.init_logger()
        reader.load_properties_as_env_vars(path_config)
        manager = ConfluencePageHandlerManager()
        manager.upload_node_information_as_confluence_pages()
        manager.upload_summary_for_confluence_pages()
    except Exception as e:
        logging.exception(e)
        notifier = MyErrorNotifier(os.path.basename(__file__))
        notifier.notify_me(str(e))
    finally:
        logger.stop_logger()


if __name__ == '__main__':
    if len(sys.argv) == 1:
        raise SystemExit('path to config file is missing')
    if len(sys.argv) > 2:
        raise SystemExit('invalid number of input arguments')
    main(sys.argv[1])
