import unittest

import bs4
import pandas as pd
from csv_to_confluence import TemplatePageStatusChecker
from csv_to_confluence import TemplateResourceLoader
from common import load_properties_file_as_environment


class TestTemplatePageStatusChecker(unittest.TestCase):
    __DEFAULT_NODE_ID: str = '10'
    __TEMPLATE: str = None

    @classmethod
    def setUpClass(cls):
        load_properties_file_as_environment('settings.json')
        cls.__CHECKER = TemplatePageStatusChecker(cls.__DEFAULT_NODE_ID)
        cls.__CURRENT_YMD_HMS = pd.Timestamp.now().tz_localize('Europe/Berlin').strftime('%Y-%m-%d %H:%M:%S')

    def setUp(self):
        loader = TemplateResourceLoader()
        page = loader.get_resource_as_string('template_page.html')
        html = bs4.BeautifulSoup(page, 'html.parser')
        html.find(class_='last_contact').string.replace_with(self.__CURRENT_YMD_HMS)
        html.find(class_='last_write').string.replace_with(self.__CURRENT_YMD_HMS)
        html.find(class_='daily_imported').string.replace_with('60')
        html.find(class_='daily_updated').string.replace_with('60')
        html.find(class_='daily_error_rate').string.replace_with('0.0')
        self.__TEMPLATE = str(html)

    def test_default_values_for_online(self):
        page = self.__CHECKER.add_content_to_template_page(self.__TEMPLATE)
        self.__check_title_and_color_of_status_element_on_page(page, 'ONLINE', 'Green')

    def test_last_contact_less_than_one_day_ago(self):
        date_yesterday = self.__get_current_date_moved_back_by_days_and_hours(0, 23)
        self.__set_value_in_template('last_contact', date_yesterday)
        page = self.__CHECKER.add_content_to_template_page(self.__TEMPLATE)
        self.__check_title_and_color_of_status_element_on_page(page, 'ONLINE', 'Green')

    def test_last_contact_more_than_one_day_ago(self):
        date_past = self.__get_current_date_moved_back_by_days_and_hours(1, 1)
        self.__set_value_in_template('last_contact', date_past)
        page = self.__CHECKER.add_content_to_template_page(self.__TEMPLATE)
        self.__check_title_and_color_of_status_element_on_page(page, 'OFFLINE', 'Red')

    def test_last_write_less_than_one_day_ago(self):
        date_yesterday = self.__get_current_date_moved_back_by_days_and_hours(0, 23)
        self.__set_value_in_template('last_write', date_yesterday)
        page = self.__CHECKER.add_content_to_template_page(self.__TEMPLATE)
        self.__check_title_and_color_of_status_element_on_page(page, 'ONLINE', 'Green')

    def test_last_write_more_than_one_day_ago(self):
        date_past = self.__get_current_date_moved_back_by_days_and_hours(1, 1)
        self.__set_value_in_template('last_write', date_past)
        page = self.__CHECKER.add_content_to_template_page(self.__TEMPLATE)
        self.__check_title_and_color_of_status_element_on_page(page, 'NO IMPORTS', 'Red')

    def test_empty_last_write(self):
        self.__set_value_in_template('last_write', '-')
        page = self.__CHECKER.add_content_to_template_page(self.__TEMPLATE)
        self.__check_title_and_color_of_status_element_on_page(page, 'NO IMPORTS', 'Red')

    def test_imports_below_lower_border(self):
        self.__set_value_in_template('daily_imported', '66')
        self.__set_value_in_template('daily_updated', '0')
        page = self.__CHECKER.add_content_to_template_page(self.__TEMPLATE)
        self.__check_title_and_color_of_status_element_on_page(page, 'DEVIATING IMPORTS', 'Yellow')

    def test_imports_on_lower_border(self):
        self.__set_value_in_template('daily_imported', '67')
        self.__set_value_in_template('daily_updated', '0')
        page = self.__CHECKER.add_content_to_template_page(self.__TEMPLATE)
        self.__check_title_and_color_of_status_element_on_page(page, 'ONLINE', 'Green')

    def test_imports_above_higher_broder(self):
        self.__set_value_in_template('daily_imported', '134')
        self.__set_value_in_template('daily_updated', '0')
        page = self.__CHECKER.add_content_to_template_page(self.__TEMPLATE)
        self.__check_title_and_color_of_status_element_on_page(page, 'DEVIATING IMPORTS', 'Yellow')

    def test_imports_on_higher_border(self):
        self.__set_value_in_template('daily_imported', '133')
        self.__set_value_in_template('daily_updated', '0')
        page = self.__CHECKER.add_content_to_template_page(self.__TEMPLATE)
        self.__check_title_and_color_of_status_element_on_page(page, 'ONLINE', 'Green')

    def test_no_daily_imports(self):
        self.__set_value_in_template('daily_imported', '-')
        self.__set_value_in_template('daily_updated', '-')
        page = self.__CHECKER.add_content_to_template_page(self.__TEMPLATE)
        self.__check_title_and_color_of_status_element_on_page(page, 'ONLINE', 'Green')

    def test_error_rate_below_threshold(self):
        self.__set_value_in_template('daily_error_rate', '0.99')
        page = self.__CHECKER.add_content_to_template_page(self.__TEMPLATE)
        self.__check_title_and_color_of_status_element_on_page(page, 'ONLINE', 'Green')

    def test_error_rate_above_threshold(self):
        self.__set_value_in_template('daily_error_rate', '1.01')
        page = self.__CHECKER.add_content_to_template_page(self.__TEMPLATE)
        self.__check_title_and_color_of_status_element_on_page(page, 'HIGH ERROR RATE', 'Yellow')

    def test_error_rate_below_threshold_low_imports(self):
        self.__set_value_in_template('daily_imported', '45')
        self.__set_value_in_template('daily_updated', '45')
        self.__set_value_in_template('daily_error_rate', '4.99')
        page = self.__CHECKER.add_content_to_template_page(self.__TEMPLATE)
        self.__check_title_and_color_of_status_element_on_page(page, 'ONLINE', 'Green')

    def test_error_rate_above_threshold_low_imports(self):
        self.__set_value_in_template('daily_imported', '45')
        self.__set_value_in_template('daily_updated', '45')
        self.__set_value_in_template('daily_error_rate', '5.01')
        page = self.__CHECKER.add_content_to_template_page(self.__TEMPLATE)
        self.__check_title_and_color_of_status_element_on_page(page, 'HIGH ERROR RATE', 'Yellow')

    def test_no_error_rate(self):
        self.__set_value_in_template('daily_error_rate', '-')
        page = self.__CHECKER.add_content_to_template_page(self.__TEMPLATE)
        self.__check_title_and_color_of_status_element_on_page(page, 'ONLINE', 'Green')

    def __set_value_in_template(self, key: str, value: str):
        html = bs4.BeautifulSoup(self.__TEMPLATE, 'html.parser')
        html.find(class_=key).string.replace_with(value)
        self.__TEMPLATE = str(html)

    def __get_current_date_moved_back_by_days_and_hours(self, days: int, hours: int) -> str:
        ts_current = pd.Timestamp(self.__CURRENT_YMD_HMS)
        ts_past = ts_current - pd.Timedelta(days=days, hours=hours)
        return ts_past.strftime('%Y-%m-%d %H:%M:%S')

    def __check_title_and_color_of_status_element_on_page(self, page: str, expected_title: str, expected_color: str):
        html = bs4.BeautifulSoup(page, 'html.parser')
        status = html.find(class_='status')
        param_title = status.findAll('ac:parameter', {'ac:name': 'title'})
        actual_title = param_title[0].string
        self.assertEqual(expected_title, actual_title)
        param_color = status.findAll('ac:parameter', {'ac:name': 'color'})
        actual_color = param_color[0].string
        self.assertEqual(expected_color, actual_color)


if __name__ == '__main__':
    unittest.main()
