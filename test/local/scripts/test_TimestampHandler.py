import unittest

from src.common import TimestampHandler


class TestSingletonMeta(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.__TIMESTAMP_HANDLER = TimestampHandler()

    def test_get_yesterdays_date(self):
        date_today = self.__TIMESTAMP_HANDLER.get_current_date()
        date_yesterday = self.__TIMESTAMP_HANDLER.get_yesterdays_date()
        timedelta = self.__TIMESTAMP_HANDLER.get_timedelta_in_absolute_hours(date_yesterday, date_today)
        self.assertEqual(24, timedelta)
        timedelta2 = self.__TIMESTAMP_HANDLER.get_timedelta_in_absolute_hours(date_today, date_yesterday)
        self.assertEqual(24, timedelta2)

    def test_get_year_from_date_string(self):
        time = self.__TIMESTAMP_HANDLER.get_year_from_date_string('202201011230')
        self.assertEqual('2022', time)

    def test_get_year_from_date_string_missing_hours(self):
        time = self.__TIMESTAMP_HANDLER.get_year_from_date_string('20220101')
        self.assertEqual('2022', time)

    def test_get_YMD_from_date_string(self):
        time = self.__TIMESTAMP_HANDLER.get_YMD_from_date_string('202201111230')
        self.assertEqual('2022-01-11', time)

    def test_get_YMD_from_date_string_invalid_input(self):
        with self.assertRaises(ValueError):
            _ = self.__TIMESTAMP_HANDLER.get_YMD_from_date_string('2022011')

    def test_get_YMD_HMS_from_date_string(self):
        time = self.__TIMESTAMP_HANDLER.get_YMD_HMS_from_date_string('20220111123055')
        self.assertEqual('2022-01-11 12:30:55', time)

    def test_get_YMD_HMS_from_date_string_missing_hours(self):
        time = self.__TIMESTAMP_HANDLER.get_YMD_HMS_from_date_string('20220111')
        self.assertEqual('2022-01-11 00:00:00', time)


if __name__ == '__main__':
    unittest.main()
