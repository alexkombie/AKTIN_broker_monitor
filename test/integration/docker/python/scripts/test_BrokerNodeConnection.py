import unittest
from node_to_csv import BrokerNodeConnection
from node_to_csv import load_properties_file_as_environment
from test.integration.docker.python.scripts.BrokerNodeDummy import BrokerNodeDummy
from test.integration.docker.python.scripts.BrokerNodeDummy import BrokerImportStats
from test.integration.docker.python.scripts.BrokerNodeDummy import BrokerNodeError


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
        cls.__DEFAULT_NODE_ID = '2'

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

    def test_get_broker_node_stats_missing(self):
        stats1 = self.__create_default_broker_import_stats_with_missing()
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
        error1 = BrokerNodeError('2020-01-01T00:00:00+01:00', '5', 'TestError')
        self.__init_new_dummy_and_put_payload_on_node(self.__DEFAULT_API_KEY, error1)
        list_errors = self.__BROKER_NODE_CONNECTION.get_broker_node_errors(self.__DEFAULT_NODE_ID)
        self.assertEqual(1, len(list_errors))
        error2 = list_errors[0]
        self.assertEqual('2020-01-01T00:00:00+01:00', error2.timestamp)
        self.assertEqual('5', error2.repeats)
        self.assertEqual('TestError', error2.content)

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
    def __create_default_broker_import_stats_with_missing():
        return BrokerImportStats(
                '2020-01-01T00:00:00+01:00',
                '',
                '',
                '400',
                '300',
                '200',
                '100')

    @staticmethod
    def __init_new_dummy_and_put_payload_on_node(api_key: str, payload):
        dummy = BrokerNodeDummy(api_key)
        dummy.put_stats_object_on_broker(payload)


if __name__ == '__main__':
    unittest.main()
