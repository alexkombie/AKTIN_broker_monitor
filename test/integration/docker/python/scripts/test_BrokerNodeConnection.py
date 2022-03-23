import unittest
import requests
import lxml
from common import BrokerNodeConnection
from common import load_properties_file_as_environment
from BrokerNodeDummy import BrokerNodeDummy
from BrokerNodeDummy import BrokerImportStats
from BrokerNodeDummy import BrokerNodeError
from BrokerNodeDummy import BrokerNodeVersions
from BrokerNodeDummy import BrokerNodeRscript
from BrokerNodeDummy import BrokerNodePython
from BrokerNodeDummy import BrokerNodeImportScripts


class TestBrokerNodeConnection(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        load_properties_file_as_environment('settings.json')
        stats = cls.__create_default_broker_import_stats()
        cls.__init_new_dummy_and_put_payload_on_node('xxxApiKey123', stats)
        cls.__init_new_dummy_and_put_payload_on_node('xxxApiKey567', stats)
        cls.__init_new_dummy_and_put_payload_on_node('xxxApiKey890', stats)
        cls.__BROKER_NODE_CONNECTION = BrokerNodeConnection()
        cls.__DEFAULT_API_KEY = 'xxxApiKey123'
        cls.__DEFAULT_NODE_ID = '0'

    def tearDown(self):
        stats = self.__create_default_broker_import_stats()
        self.__init_new_dummy_and_put_payload_on_node(self.__DEFAULT_API_KEY, stats)

    def test_for_singleton(self):
        broker = BrokerNodeConnection()
        self.assertEqual(id(self.__BROKER_NODE_CONNECTION), id(broker))

    def test_get_broker_nodes_list(self):
        list_nodes = self.__BROKER_NODE_CONNECTION.get_broker_nodes_list()
        self.assertEqual(3, len(list_nodes))

    def test_get_broker_node_stats(self):
        stats = self.__BROKER_NODE_CONNECTION.get_broker_node_stats(self.__DEFAULT_NODE_ID)
        self.assertEqual('2020-01-01T00:00:00+01:00', stats.dwh_start)
        self.assertEqual('2020-01-02T12:00:00+01:00', stats.last_write)
        self.assertEqual('2020-01-02T12:00:00+01:00', stats.last_reject)
        self.assertEqual('400', stats.imported)
        self.assertEqual('300', stats.updated)
        self.assertEqual('200', stats.invalid)
        self.assertEqual('100', stats.failed)

    def test_get_broker_node_stats_missing_dates(self):
        stats1 = self.__create_default_broker_import_stats_with_missing_dates()
        self.__init_new_dummy_and_put_payload_on_node(self.__DEFAULT_API_KEY, stats1)
        stats2 = self.__BROKER_NODE_CONNECTION.get_broker_node_stats(self.__DEFAULT_NODE_ID)
        self.assertEqual('2020-01-01T00:00:00+01:00', stats2.dwh_start)
        self.assertFalse(stats2.last_write)
        self.assertFalse(stats2.last_reject)
        self.assertEqual('400', stats2.imported)
        self.assertEqual('300', stats2.updated)
        self.assertEqual('200', stats2.invalid)
        self.assertEqual('100', stats2.failed)

    def test_get_broker_node_errors(self):
        error1 = self.__create_default_broker_error()
        self.__init_new_dummy_and_put_payload_on_node(self.__DEFAULT_API_KEY, error1)
        list_errors = self.__BROKER_NODE_CONNECTION.get_broker_node_errors(self.__DEFAULT_NODE_ID)
        self.assertEqual(1, len(list_errors))
        error2 = list_errors[0]
        self.assertEqual('2020-01-01T00:00:00+01:00', error2.timestamp)
        self.assertEqual('5', error2.repeats)
        self.assertEqual('TestError', error2.content)

    def test_get_broker_node_errors_missing_repeats(self):
        error1 = self.__create_default_broker_error_with_missing_repeats()
        self.__init_new_dummy_and_put_payload_on_node(self.__DEFAULT_API_KEY, error1)
        list_errors = self.__BROKER_NODE_CONNECTION.get_broker_node_errors(self.__DEFAULT_NODE_ID)
        self.assertEqual(1, len(list_errors))
        error2 = list_errors[0]
        self.assertEqual('2020-01-01T00:00:00+01:00', error2.timestamp)
        self.assertFalse(error2.repeats)
        self.assertEqual('TestError', error2.content)

    def test_get_broker_node_errors_empty(self):
        dummy = BrokerNodeDummy(self.__DEFAULT_API_KEY)
        dummy.reset_stats_on_broker()
        list_errors = self.__BROKER_NODE_CONNECTION.get_broker_node_errors(self.__DEFAULT_NODE_ID)
        self.assertEqual(0, len(list_errors))

    def test_get_broker_node_nonexisting_id(self):
        with self.assertRaises(requests.exceptions.HTTPError):
            self.__BROKER_NODE_CONNECTION.get_broker_node_stats('99')


    @staticmethod
    def __create_default_broker_import_stats():
        return BrokerImportStats(
                '2020-01-01T00:00:00+01:00',
                '2020-01-02T12:00:00+01:00',
                '2020-01-02T12:00:00+01:00',
                '400',
                '300',
                '200',
                '100')

    @staticmethod
    def __create_default_broker_import_stats_with_missing_dates():
        return BrokerImportStats(
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
    def __init_new_dummy_and_put_payload_on_node(api_key: str, payload):
        dummy = BrokerNodeDummy(api_key)
        dummy.put_stats_object_on_broker(payload)

    def test_get_broker_node_versions(self):
        dummy = BrokerNodeDummy(self.__DEFAULT_API_KEY)
        versions = BrokerNodeVersions('Ubuntu/11.0.13', 'Ubuntu 20.04.1 LTS', '2.4.41-4ubuntu3.9', '12.9-0ubuntu0.20.04.1')
        dummy.put_resource_object_on_broker(versions, 'versions')
        resource = self.__BROKER_NODE_CONNECTION.get_broker_node_resource(self.__DEFAULT_NODE_ID, 'versions')
        self.assertEqual(resource.get('java'), 'Ubuntu/11.0.13')
        self.assertEqual(resource.get('os'), 'Ubuntu 20.04.1 LTS')
        self.assertEqual(resource.get('apache2'), '2.4.41-4ubuntu3.9')
        self.assertEqual(resource.get('postgres'), '12.9-0ubuntu0.20.04.1')
        self.assertIsNone(resource.get('wildfly'))

    def test_get_broker_node_rscript(self):
        dummy = BrokerNodeDummy(self.__DEFAULT_API_KEY)
        rscript = BrokerNodeRscript('3.6.3-2', '1.3.0-1', '0.20-40-1')
        dummy.put_resource_object_on_broker(rscript, 'rscript')
        resource = self.__BROKER_NODE_CONNECTION.get_broker_node_resource(self.__DEFAULT_NODE_ID, 'rscript')
        self.assertEqual(resource.get('r-base-core'), '3.6.3-2')
        self.assertEqual(resource.get('r-cran-tidyverse'), '1.3.0-1')
        self.assertEqual(resource.get('r-cran-lattice'), '0.20-40-1')
        self.assertIsNone(resource.get('r-core'))

    def test_get_broker_node_python(self):
        dummy = BrokerNodeDummy(self.__DEFAULT_API_KEY)
        python = BrokerNodePython('3.8.2-0ubuntu2', '1:1.17.4-5ubuntu3', '')
        dummy.put_resource_object_on_broker(python, 'python')
        resource = self.__BROKER_NODE_CONNECTION.get_broker_node_resource(self.__DEFAULT_NODE_ID, 'python')
        self.assertEqual(resource.get('python3'), '3.8.2-0ubuntu2')
        self.assertEqual(resource.get('python3-numpy'), '1:1.17.4-5ubuntu3')
        self.assertIsNone(resource.get('python3-pandas'))

    def test_get_broker_node_import_scripts(self):
        dummy = BrokerNodeDummy(self.__DEFAULT_API_KEY)
        scripts = BrokerNodeImportScripts('1.5')
        dummy.put_resource_object_on_broker(scripts, 'import-scripts')
        resource = self.__BROKER_NODE_CONNECTION.get_broker_node_resource(self.__DEFAULT_NODE_ID, 'import-scripts')
        self.assertEqual(resource.get('p21'), '1.5')
        self.assertIsNone(resource.get('p20'))

    def test_get_broker_empty_resource(self):
        dummy = BrokerNodeDummy(self.__DEFAULT_API_KEY)
        dummy.put_empty_resource_on_broker('empty')
        with self.assertRaises(lxml.etree.XMLSyntaxError):
            self.__BROKER_NODE_CONNECTION.get_broker_node_resource(self.__DEFAULT_NODE_ID, 'empty')


if __name__ == '__main__':
    unittest.main()
