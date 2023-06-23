import os
import sys
import unittest
from pathlib import Path

this_path = Path(os.path.realpath(__file__))
path_src = os.path.join(this_path.parents[2], 'src')
sys.path.insert(0, path_src)

from common import TimestampHandler


class TestSingletonMeta(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.__handler = TimestampHandler()

    def test_get_yesterdays_date(self):
        date_today = self.__handler.get_current_date()
        date_yesterday = self.__handler.get_yesterdays_date()
        timedelta = self.__handler.get_timedelta_in_absolute_hours(date_yesterday, date_today)
        self.assertEqual(24, timedelta)
        timedelta2 = self.__handler.get_timedelta_in_absolute_hours(date_today, date_yesterday)
        self.assertEqual(24, timedelta2)

    def test_get_year_from_date_string(self):
        self.assertEqual('2023', self.__handler.get_utc_year_from_date_string('2023-06-22T15:58:05Z'))

    def test_get_year_from_date_string_different_tz(self):
        self.assertEqual('2024', self.__handler.get_utc_year_from_date_string('2023-12-31T20:00:00-04:00'))

    def test_get_year_from_date_string_missing_hours(self):
        self.assertEqual('2021', self.__handler.get_utc_year_from_date_string('20220101'))

    def test_get_ymd_from_date_string(self):
        self.assertEqual('2023-06-22', self.__handler.get_utc_ymd_from_date_string('2023-06-22T15:58:05Z'))

    def test_get_ymd_from_date_string_different_tz(self):
        self.assertEqual('2024-01-01', self.__handler.get_utc_ymd_from_date_string('2023-12-31T20:00:00-04:00'))

    def test_get_ymd_from_date_string_invalid_input(self):
        with self.assertRaises(ValueError):
            _ = self.__handler.get_utc_ymd_from_date_string('2023062')

    def test_get_ymd_hms_from_date_string(self):
        self.assertEqual('2023-06-22 15:58:05', self.__handler.get_utc_ymd_hms_from_date_string('2023-06-22T15:58:05Z'))

    def test_get_ymd_hms_from_date_string_different_tz(self):
        self.assertEqual('2024-01-01 00:00:00', self.__handler.get_utc_ymd_hms_from_date_string('2023-12-31T20:00:00-04:00'))

    def test_get_timedelta_in_absolute_hours(self):
        self.assertEqual(24.0, self.__handler.get_timedelta_in_absolute_hours('2023-06-22T15:58:05Z', '2023-06-23T15:58:05Z'))

    def test_get_timedelta_in_absolute_hours_different_tz(self):
        self.assertEqual(0, self.__handler.get_timedelta_in_absolute_hours('2023-12-31T20:00:00-04:00', '2024-01-01T04:00:00+04:00'))

    def test_convert_to_berlin_time1(self):
        self.assertEqual('2023-06-22 02:00:00+02:00', self.__handler.convert_ts_to_berlin_time('2023-06-22 00:00:00'))

    def test_convert_to_berlin_time2(self):
        self.assertEqual('2023-12-22 01:00:00+01:00', self.__handler.convert_ts_to_berlin_time('2023-12-22 00:00:00'))

    def test_convert_to_berlin_time3(self):
        self.assertEqual('2023-12-22 01:00:00+01:00', self.__handler.convert_ts_to_berlin_time('2023-12-22'))

    def test_convert_to_berlin_time_with_tzinfo(self):
        self.assertEqual('2022-01-01 12:00:00+01:00', self.__handler.convert_ts_to_berlin_time('2022-01-01 12:00:00+01:00'))

    def test_convert_to_berlin_time_with_tzinfo2(self):
        self.assertEqual('2023-06-22 02:00:00+02:00', self.__handler.convert_ts_to_berlin_time('2023-06-22 00:00:00Z'))

    def test_convert_to_berlin_time_with_ms(self):
        self.assertEqual('2023-06-23 12:05:49.301685+02:00', self.__handler.convert_ts_to_berlin_time('2023-06-23 10:05:49.301685+00:00'))

if __name__ == '__main__':
    unittest.main()
