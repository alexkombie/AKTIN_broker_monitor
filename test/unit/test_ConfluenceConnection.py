import os
import sys
import unittest
from pathlib import Path

this_path = Path(os.path.realpath(__file__))
path_src = os.path.join(this_path.parents[2], 'src')
sys.path.insert(0, path_src)

from common import ConfluenceConnection, ConfigReader


class TestConfluenceConnection(unittest.TestCase):
    """
    Only GET-Methods are tested for ConfluenceConnection as the other REST methods
    are too risky to test automatically on a running system. Therefore, they were
    tested manually
    """

    @classmethod
    def setUpClass(cls):
        path_settings = os.path.join(this_path.parents[1], 'resources', 'settings.toml')
        ConfigReader().load_config_as_env_vars(path_settings)
        cls.__CONFLUENCE_CONNECTION = ConfluenceConnection()

    def test_existing_page(self):
        self.assertTrue(self.__CONFLUENCE_CONNECTION.does_page_exists('Support Log Broker-Monitor'))

    def test_non_existing_page(self):
        self.assertFalse(self.__CONFLUENCE_CONNECTION.does_page_exists('Nonexisting'))


if __name__ == '__main__':
    unittest.main()
