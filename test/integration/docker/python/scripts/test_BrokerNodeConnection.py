import unittest
import os
import requests
import lxml
from common import BrokerNodeConnection
from common import load_properties_file_as_environment
from BrokerNodeDummy import BrokerNodeDummy
from BrokerNodeDummy import BrokerNodeImports
from BrokerNodeDummy import BrokerNodeError
from BrokerNodeDummy import BrokerNodeVersions
from BrokerNodeDummy import BrokerNodeRscript
from BrokerNodeDummy import BrokerNodePython
from BrokerNodeDummy import BrokerNodeImportScripts


class TestBrokerNodeConnection(unittest.TestCase):
    __DEFAULT_API_KEY = 'xxxApiKey123'
    __DEFAULT_NODE_ID = '0'

    @classmethod
    def setUpClass(cls):
        load_properties_file_as_environment('settings.json')
        cls.__DUMMY = BrokerNodeDummy(cls.__DEFAULT_API_KEY)
        cls.__BROKER_NODE_CONNECTION = BrokerNodeConnection()

    @staticmethod
    def __create_default_broker_import_stats():
        return BrokerNodeImports(
                '2020-01-01T00:00:00+01:00',
                '2020-01-02T12:00:00+01:00',
                '2020-01-02T12:00:00+01:00',
                '400',
                '300',
                '200',
                '100')

    @staticmethod
    def __create_default_broker_import_stats_with_missing_dates():
        return BrokerNodeImports(
                '2020-01-01T00:00:00+01:00',
                '',
                '',
                '400',
                '300',
                '200',
                '100')

    @staticmethod
    def __create_default_broker_error():
        return BrokerNodeError(
                '2020-01-01T00:00:00+01:00',
                '5',
                'TestError'
        )

    @staticmethod
    def __create_default_broker_error_with_missing_repeats():
        return BrokerNodeError(
                '2020-01-01T00:00:00+01:00',
                '',
                'TestError'
        )

    @staticmethod
    def __init_new_dummy_and_put_stats_on_node(api_key: str, payload):
        dummy = BrokerNodeDummy(api_key)
        dummy.put_import_info_on_broker(payload)

    def test_get_broker_nodes_list(self):
        stats = self.__create_default_broker_import_stats()
        self.__init_new_dummy_and_put_stats_on_node('xxxApiKey123', stats)
        self.__init_new_dummy_and_put_stats_on_node('xxxApiKey567', stats)
        self.__init_new_dummy_and_put_stats_on_node('xxxApiKey890', stats)
        list_nodes = self.__BROKER_NODE_CONNECTION.get_broker_nodes_list()
        self.assertEqual(3, len(list_nodes))

    def test_get_broker_node_stats(self):
        stats = self.__create_default_broker_import_stats()
        self.__DUMMY.put_import_info_on_broker(stats)
        stats2 = self.__BROKER_NODE_CONNECTION.get_broker_node_stats(self.__DEFAULT_NODE_ID)
        self.assertEqual('2020-01-01T00:00:00+01:00', stats2.dwh_start)
        self.assertEqual('2020-01-02T12:00:00+01:00', stats2.last_write)
        self.assertEqual('2020-01-02T12:00:00+01:00', stats2.last_reject)
        self.assertEqual('400', stats2.imported)
        self.assertEqual('300', stats2.updated)
        self.assertEqual('200', stats2.invalid)
        self.assertEqual('100', stats2.failed)

    def test_get_broker_node_stats_missing_dates(self):
        stats = self.__create_default_broker_import_stats_with_missing_dates()
        self.__DUMMY.put_import_info_on_broker(stats)
        stats2 = self.__BROKER_NODE_CONNECTION.get_broker_node_stats(self.__DEFAULT_NODE_ID)
        self.assertEqual('2020-01-01T00:00:00+01:00', stats2.dwh_start)
        self.assertFalse(stats2.last_write)
        self.assertFalse(stats2.last_reject)
        self.assertEqual('400', stats2.imported)
        self.assertEqual('300', stats2.updated)
        self.assertEqual('200', stats2.invalid)
        self.assertEqual('100', stats2.failed)

    def test_get_broker_nonexisting_node_stats(self):
        with self.assertRaises(requests.exceptions.HTTPError):
            _ = self.__BROKER_NODE_CONNECTION.get_broker_node_stats('nonexisting_id')

    def test_get_broker_node_errors(self):
        error = self.__create_default_broker_error()
        self.__DUMMY.put_import_info_on_broker(error)
        list_errors = self.__BROKER_NODE_CONNECTION.get_broker_node_errors(self.__DEFAULT_NODE_ID)
        self.assertEqual(1, len(list_errors))
        error2 = list_errors[0]
        self.assertEqual('2020-01-01T00:00:00+01:00', error2.timestamp)
        self.assertEqual('5', error2.repeats)
        self.assertEqual('TestError', error2.content)

    def test_get_broker_node_errors_missing_repeats(self):
        error1 = self.__create_default_broker_error_with_missing_repeats()
        self.__DUMMY.put_import_info_on_broker(error1)
        list_errors = self.__BROKER_NODE_CONNECTION.get_broker_node_errors(self.__DEFAULT_NODE_ID)
        self.assertEqual(1, len(list_errors))
        error2 = list_errors[0]
        self.assertEqual('2020-01-01T00:00:00+01:00', error2.timestamp)
        self.assertFalse(error2.repeats)
        self.assertEqual('TestError', error2.content)

    def test_get_broker_node_empty_errors(self):
        dummy = BrokerNodeDummy(self.__DEFAULT_API_KEY)
        dummy.reset_stats_on_broker()
        list_errors = self.__BROKER_NODE_CONNECTION.get_broker_node_errors(self.__DEFAULT_NODE_ID)
        self.assertEqual(0, len(list_errors))

    def test_get_broker_nonexisting_node_errors(self):
        with self.assertRaises(requests.exceptions.HTTPError):
            _ = self.__BROKER_NODE_CONNECTION.get_broker_node_errors('nonexisting_id')

    def test_get_broker_node_versions(self):
        versions = BrokerNodeVersions('Ubuntu/11.0.13', 'Ubuntu 20.04.1 LTS', '2.4.41-4ubuntu3.9', '12.9-0ubuntu0.20.04.1')
        self.__DUMMY.put_resource_on_broker(versions, 'versions')
        resource = self.__BROKER_NODE_CONNECTION.get_broker_node_resource(self.__DEFAULT_NODE_ID, 'versions')
        self.assertEqual(resource.get('java'), 'Ubuntu/11.0.13')
        self.assertEqual(resource.get('os'), 'Ubuntu 20.04.1 LTS')
        self.assertEqual(resource.get('apache2'), '2.4.41-4ubuntu3.9')
        self.assertEqual(resource.get('postgres'), '12.9-0ubuntu0.20.04.1')
        self.assertIsNone(resource.get('wildfly'))

    def test_get_broker_node_rscript(self):
        rscript = BrokerNodeRscript('3.6.3-2', '1.3.0-1', '0.20-40-1')
        self.__DUMMY.put_resource_on_broker(rscript, 'rscript')
        resource = self.__BROKER_NODE_CONNECTION.get_broker_node_resource(self.__DEFAULT_NODE_ID, 'rscript')
        self.assertEqual(resource.get('r-base-core'), '3.6.3-2')
        self.assertEqual(resource.get('r-cran-tidyverse'), '1.3.0-1')
        self.assertEqual(resource.get('r-cran-lattice'), '0.20-40-1')
        self.assertIsNone(resource.get('r-core'))

    def test_get_broker_node_python(self):
        python = BrokerNodePython('3.8.2-0ubuntu2', '1:1.17.4-5ubuntu3', '')
        self.__DUMMY.put_resource_on_broker(python, 'python')
        resource = self.__BROKER_NODE_CONNECTION.get_broker_node_resource(self.__DEFAULT_NODE_ID, 'python')
        self.assertEqual(resource.get('python3'), '3.8.2-0ubuntu2')
        self.assertEqual(resource.get('python3-numpy'), '1:1.17.4-5ubuntu3')
        self.assertIsNone(resource.get('python3-pandas'))

    def test_get_broker_node_import_scripts(self):
        scripts = BrokerNodeImportScripts('1.5')
        self.__DUMMY.put_resource_on_broker(scripts, 'import-scripts')
        resource = self.__BROKER_NODE_CONNECTION.get_broker_node_resource(self.__DEFAULT_NODE_ID, 'import-scripts')
        self.assertEqual(resource.get('p21'), '1.5')
        self.assertIsNone(resource.get('p20'))

    def test_get_broker_node_empty_resource(self):
        self.__DUMMY.put_empty_resource_on_broker('empty')
        with self.assertRaises(lxml.etree.XMLSyntaxError):
            _ = self.__BROKER_NODE_CONNECTION.get_broker_node_resource(self.__DEFAULT_NODE_ID, 'empty')

    def test_get_broker_node_nonexisting_resource(self):
        resource = self.__BROKER_NODE_CONNECTION.get_broker_node_resource(self.__DEFAULT_NODE_ID, 'nonexisting')
        self.assertFalse(resource)


if __name__ == '__main__':
    unittest.main()
