import os
import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from shutil import rmtree

import bs4
import pandas as pd
import pytz

this_path = Path(os.path.realpath(__file__))
path_src = os.path.join(this_path.parents[2], 'src')
sys.path.insert(0, path_src)

from common import InfoCSVHandler, ConfigReader
from csv_to_confluence import TemplatePageStatusChecker, TemplatePageLoader, TemplatePageCSVInfoWriter


class TestTemplatePageStatusChecker(unittest.TestCase):
    """
    info csv columns:
    ['date', 'last_contact', 'last_start', 'last_write', 'last_reject',
    'imported', 'updated', 'invalid', 'failed', 'error_rate',
    'daily_imported', 'daily_updated', 'daily_invalid', 'daily_failed', 'daily_error_rate']
    """

    def setUp(self):
        path_settings = os.path.join(this_path.parents[1], 'resources', 'settings.toml')
        ConfigReader().load_config_as_env_vars(path_settings)
        self.__working_dir = os.environ['DIR.WORKING'] if os.environ['DIR.WORKING'] else os.getcwd()
        self.__builder = DataFrameBuilder(self.__working_dir)

    def tearDown(self):
        rmtree(self.__working_dir)

    def test_default_values_for_online(self):
        node_id = '10'
        self.__builder.for_node(node_id).with_row([self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   '-',
                                                   '-',
                                                   '-', '-', '-', '-', '-',
                                                   '-', '-', '-', '-', '-']).build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'ONLINE', 'Green')

    def test_last_contact_less_than_one_day_ago(self):
        node_id = '10'
        self.__builder.for_node(node_id).with_row([self.__create_timestamp(),
                                                   self.__create_timestamp(shift_hours=-23),
                                                   self.__create_timestamp(),
                                                   '-',
                                                   '-',
                                                   '-', '-', '-', '-', '-',
                                                   '-', '-', '-', '-', '-']).build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'ONLINE', 'Green')

    def test_last_contact_less_than_one_day_ago_updated_threshold(self):
        node_id = '3'
        self.__builder.for_node(node_id).with_row([self.__create_timestamp(),
                                                   self.__create_timestamp(shift_hours=-23),
                                                   self.__create_timestamp(),
                                                   '-',
                                                   '-',
                                                   '-', '-', '-', '-', '-',
                                                   '-', '-', '-', '-', '-']).build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'OFFLINE', 'Red')

    def test_last_contact_more_than_one_day_ago(self):
        node_id = '10'
        self.__builder.for_node(node_id).with_row([self.__create_timestamp(),
                                                   self.__create_timestamp(shift_days=-1, shift_hours=-1),
                                                   self.__create_timestamp(),
                                                   '-',
                                                   '-',
                                                   '-', '-', '-', '-', '-',
                                                   '-', '-', '-', '-', '-']).build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'OFFLINE', 'Red')

    def test_last_contact_more_than_one_day_ago_updated_threshold(self):
        node_id = '2'
        self.__builder.for_node(node_id).with_row([self.__create_timestamp(),
                                                   self.__create_timestamp(shift_days=1, shift_hours=1),
                                                   self.__create_timestamp(),
                                                   '-',
                                                   '-',
                                                   '-', '-', '-', '-', '-',
                                                   '-', '-', '-', '-', '-']).build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'ONLINE', 'Green')

    def test_last_write_less_than_one_day_ago(self):
        node_id = '10'
        self.__builder.for_node(node_id).with_row([self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   self.__create_timestamp(shift_hours=-23),
                                                   '-',
                                                   '-', '-', '-', '-', '-',
                                                   '-', '-', '-', '-', '-']).build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'ONLINE', 'Green')

    def test_last_write_less_than_one_day_ago_updated_threshold(self):
        node_id = '3'
        self.__builder.for_node(node_id).with_row([self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   self.__create_timestamp(shift_hours=-23),
                                                   '-',
                                                   '-', '-', '-', '-', '-',
                                                   '-', '-', '-', '-', '-']).build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'NO IMPORTS', 'Red')

    def test_last_write_more_than_one_day_ago(self):
        node_id = '10'
        self.__builder.for_node(node_id).with_row([self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   self.__create_timestamp(shift_days=-1, shift_hours=-1),
                                                   '-',
                                                   '-', '-', '-', '-', '-',
                                                   '-', '-', '-', '-', '-']).build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'NO IMPORTS', 'Red')

    def test_last_write_more_than_one_day_ago_updated_threshold(self):
        node_id = '2'
        self.__builder.for_node(node_id).with_row([self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   self.__create_timestamp(shift_days=-1, shift_hours=-1),
                                                   '-',
                                                   '-', '-', '-', '-', '-',
                                                   '-', '-', '-', '-', '-']).build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'ONLINE', 'Green')

    def test_empty_last_write_and_no_further_columns(self):
        node_id = '10'
        self.__builder.for_node(node_id).with_row([self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   '-',
                                                   '-',
                                                   '-', '-', '-', '-', '-',
                                                   '-', '-', '-', '-', '-']).build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'ONLINE', 'Green')

    def test_empty_last_write_and_csv_with_current_timetamps(self):
        node_id = '10'
        self.__builder.for_node(node_id) \
            .with_row([self.__create_timestamp(shift_days=-6), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '0', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-5), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '1', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-4), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '1', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-3), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '1', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-2), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '1', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-1), self.__create_timestamp(), self.__create_timestamp(), '-'
                          , '-', '-', '-', '-', '-', '-', '0', '-', '-', '-', '-']) \
            .build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'ONLINE', 'Green')

    def test_empty_last_write_and_csv_with_past_timetamps(self):
        node_id = '10'
        self.__builder.for_node(node_id) \
            .with_row([self.__create_timestamp(shift_days=-6), self.__create_timestamp(), self.__create_timestamp(), '-'
                          , '-', '-', '-', '-', '-', '-', '0', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-5), self.__create_timestamp(), self.__create_timestamp(), '-'
                          , '-', '-', '-', '-', '-', '-', '1', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-4), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp(shift_days=-4)
                          , '-', '-', '-', '-', '-', '-', '1', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-3), self.__create_timestamp(), self.__create_timestamp(), '-'
                          , '-', '-', '-', '-', '-', '-', '1', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-2), self.__create_timestamp(), self.__create_timestamp(), '-'
                          , '-', '-', '-', '-', '-', '-', '1', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-1), self.__create_timestamp(), self.__create_timestamp(), '-'
                          , '-', '-', '-', '-', '-', '-', '0', '-', '-', '-', '-']) \
            .build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'NO IMPORTS', 'Red')

    def test_error_rate_below_lower_threshold(self):
        node_id = '10'
        self.__builder.for_node(node_id).with_row([self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   '-',
                                                   '-',
                                                   '-', '-', '-', '-', '-',
                                                   '-', '-', '-', '-', '0.99']).build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'ONLINE', 'Green')

    def test_error_rate_on_lower_threshold(self):
        node_id = '10'
        self.__builder.for_node(node_id).with_row([self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   '-',
                                                   '-',
                                                   '-', '-', '-', '-', '-',
                                                   '-', '-', '-', '-', '1.0']).build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'LOW ERROR RATE', 'Yellow')

    def test_error_rate_above_lower_threshold(self):
        node_id = '10'
        self.__builder.for_node(node_id).with_row([self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   '-',
                                                   '-',
                                                   '-', '-', '-', '-', '-',
                                                   '-', '-', '-', '-', '1.01']).build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'LOW ERROR RATE', 'Yellow')

    def test_error_rate_below_higher_threshold(self):
        node_id = '10'
        self.__builder.for_node(node_id).with_row([self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   '-',
                                                   '-',
                                                   '-', '-', '-', '-', '-',
                                                   '-', '-', '-', '-', '4.99']).build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'LOW ERROR RATE', 'Yellow')

    def test_error_rate_on_higher_threshold(self):
        node_id = '10'
        self.__builder.for_node(node_id).with_row([self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   '-',
                                                   '-',
                                                   '-', '-', '-', '-', '-',
                                                   '-', '-', '-', '-', '5.0']).build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'HIGH ERROR RATE', 'Yellow')

    def test_error_rate_above_higher_threshold(self):
        node_id = '10'
        self.__builder.for_node(node_id).with_row([self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   '-',
                                                   '-',
                                                   '-', '-', '-', '-', '-',
                                                   '-', '-', '-', '-', '5.01']).build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'HIGH ERROR RATE', 'Yellow')

    def test_error_rate_below_extreme_threshold(self):
        node_id = '10'
        self.__builder.for_node(node_id).with_row([self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   '-',
                                                   '-',
                                                   '-', '-', '-', '-', '-',
                                                   '-', '-', '-', '-', '9.99']).build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'HIGH ERROR RATE', 'Yellow')

    def test_error_rate_on_extreme_threshold(self):
        node_id = '10'
        self.__builder.for_node(node_id).with_row([self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   '-',
                                                   '-',
                                                   '-', '-', '-', '-', '-',
                                                   '-', '-', '-', '-', '10.00']).build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'EXTREME ERROR RATE', 'Red')

    def test_error_rate_above_extreme_threshold(self):
        node_id = '10'
        self.__builder.for_node(node_id).with_row([self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   self.__create_timestamp(),
                                                   '-',
                                                   '-',
                                                   '-', '-', '-', '-', '-',
                                                   '-', '-', '-', '-', '10.01']).build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'EXTREME ERROR RATE', 'Red')

    def test_no_consecutive_imports(self):
        node_id = '10'
        self.__builder.for_node(node_id) \
            .with_row([self.__create_timestamp(shift_days=-6), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '0', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-5), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '1', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-4), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '0', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-3), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '1', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-2), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '1', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-1), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '0', '-', '-', '-', '-']) \
            .build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'TESTING', 'Blue')

    def test_consecutive_imports(self):
        node_id = '10'
        self.__builder.for_node(node_id) \
            .with_row([self.__create_timestamp(shift_days=-6), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '0', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-5), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '0', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-4), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '1', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-3), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '1', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-2), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '1', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-1), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '0', '-', '-', '-', '-']) \
            .build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'ONLINE', 'Green')

    def test_custom_value_set_for_consecutive_imports(self):
        node_id = '3'
        self.__builder.for_node(node_id) \
            .with_row([self.__create_timestamp(shift_days=-9), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '0', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-8), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '0', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-7), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '0', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-6), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '0', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-5), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '1', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-4), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '1', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-3), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '1', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-2), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '1', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-1), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '0', '-', '-', '-', '-']) \
            .build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'TESTING', 'Blue')

    def test_consecutive_imports_higher_than_csv_rows(self):
        node_id = '2'
        self.__builder.for_node(node_id) \
            .with_row([self.__create_timestamp(shift_days=-6), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '0', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-5), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '1', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-4), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '1', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-3), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '1', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-2), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '1', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-1), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '0', '-', '-', '-', '-']) \
            .build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'ONLINE', 'Green')

    def test_no_gap_in_monitoring(self):
        node_id = '10'
        self.__builder.for_node(node_id) \
            .with_row([self.__create_timestamp(shift_days=-6), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '1', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-5), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '2', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-4), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '3', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-3), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '4', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-2), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '5', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-1), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '6', '-', '-', '-', '-']) \
            .build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'ONLINE', 'Green')

    def test_gap_in_monitoring_today(self):
        node_id = '10'
        self.__builder.for_node(node_id) \
            .with_row([self.__create_timestamp(shift_days=-6), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '1', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-5), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '2', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-4), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '3', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-3), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '4', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-2), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '5', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-1, shift_hours=-1), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '6', '-', '-', '-', '-']) \
            .build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'GAP IN MONITORING', 'Red')

    def test_gap_in_monitoring_yesterday(self):
        node_id = '10'
        self.__builder.for_node(node_id) \
            .with_row([self.__create_timestamp(shift_days=-6), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '1', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-5), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '2', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-4), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '3', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-3), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '4', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-2, shift_hours=-1), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '5', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-1), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '6', '-', '-', '-', '-']) \
            .build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'GAP IN MONITORING', 'Red')

    def test_gap_in_monitoring_in_between(self):
        node_id = '10'
        self.__builder.for_node(node_id) \
            .with_row([self.__create_timestamp(shift_days=-6), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '1', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-5), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '2', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-4), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '3', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-99), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '4', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-2), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '5', '-', '-', '-', '-']) \
            .with_row([self.__create_timestamp(shift_days=-1), self.__create_timestamp(), self.__create_timestamp(), self.__create_timestamp()
                          , '-', '-', '-', '-', '-', '-', '6', '-', '-', '-', '-']) \
            .build()
        self.__load_content_and_check_title_and_color_of_status_element(node_id, 'ONLINE', 'Green')

    @staticmethod
    def __create_timestamp(shift_days=0, shift_hours=0):
        now = datetime.now(pytz.utc)
        shifted_time = now + timedelta(days=shift_days, hours=shift_hours)
        timestamp = shifted_time.strftime('%Y-%m-%d %H:%M:%S%z')
        return timestamp

    def __load_content_and_check_title_and_color_of_status_element(self, node_id: str, expected_title: str, expected_color: str):
        page = TemplatePageLoader().get_template_page()
        page = TemplatePageCSVInfoWriter().add_content_to_template_page(page, node_id)
        page = TemplatePageStatusChecker().add_content_to_template_page(page, node_id)
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


class DataFrameBuilder:
    def __init__(self, working_dir: str):
        self.__handler = InfoCSVHandler()
        self.__working_dir = working_dir
        self.__df = pd.DataFrame(columns=self.__handler.get_csv_columns())
        self.__node_id = None

    def for_node(self, node_id: str):
        self.__node_id = node_id
        dir_node = os.path.join(self.__working_dir, node_id)
        if not os.path.exists(dir_node):
            os.makedirs(dir_node)
        return self

    def with_row(self, row_values: list):
        if len(row_values) != len(self.__df.columns):
            raise ValueError(f"Number of values ({len(row_values)}) doesn't match number of columns ({len(self.__df.columns)})")
        self.__df.loc[len(self.__df)] = row_values
        return self

    def build(self):
        csv_name = self.__handler.generate_node_csv_name(self.__node_id)
        csv_path = os.path.join(self.__working_dir, self.__node_id, csv_name)
        self.__handler.write_data_to_file(self.__df, csv_path)
