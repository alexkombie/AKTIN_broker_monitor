import unittest
from node_to_csv import BrokerNodeConnection
from node_to_csv import load_properties_file_as_environment
from BrokerNodeDummy import BrokerNodeDummy
from BrokerNodeDummy import BrokerImportStats
from BrokerNodeDummy import BrokerNodeError


class TestBrokerNodeConnection(unittest.TestCase):
    __BROKER_NODE_CONNECTION: BrokerNodeConnection

    def setUp(self):
        load_properties_file_as_environment('settings.json')
        stats = self.__create_default_broker_import_stats()
        self.__init_new_dummy_and_put_payload_on_node('xxxApiKey123', stats)
        self.__init_new_dummy_and_put_payload_on_node('xxxApiKey567', stats)
        self.__init_new_dummy_and_put_payload_on_node('xxxApiKey890', stats)
        self.__BROKER_NODE_CONNECTION = BrokerNodeConnection()

    def test_for_singleton(self):
        broker = BrokerNodeConnection()
        self.assertEqual(id(self.__BROKER_NODE_CONNECTION), id(broker))

    def test_get_broker_nodes_list(self):
        list_nodes = self.__BROKER_NODE_CONNECTION.get_broker_nodes_list()
        self.assertEqual(3, len(list_nodes))

    def test_get_broker_node_stats(self):
        stats = self.__BROKER_NODE_CONNECTION.get_broker_node_stats('1')
        self.assertEqual('2020-01-01T00:00:00+01:00', stats.dwh_start)
        self.assertEqual('2020-01-02T12:00:00+01:00', stats.last_write)
        self.assertEqual('2020-01-02T12:00:00+01:00', stats.last_reject)
        self.assertEqual('400', stats.imported)
        self.assertEqual('300', stats.updated)
        self.assertEqual('200', stats.invalid)
        self.assertEqual('100', stats.failed)

    def test_get_broker_node_stats_missing(self):
        stats1 = self.__create_default_broker_import_stats_with_missing()
        self.__init_new_dummy_and_put_payload_on_node('xxxApiKey123', stats1)
        stats2 = self.__BROKER_NODE_CONNECTION.get_broker_node_stats('2')
        self.assertEqual('2020-01-01T00:00:00+01:00', stats2.dwh_start)
        self.assertFalse(stats2.last_write)
        self.assertFalse(stats2.last_reject)
        self.assertEqual('400', stats2.imported)
        self.assertEqual('300', stats2.updated)
        self.assertEqual('200', stats2.invalid)
        self.assertEqual('100', stats2.failed)
        stats3 = self.__create_default_broker_import_stats()
        self.__init_new_dummy_and_put_payload_on_node('xxxApiKey123', stats3)

    def test_get_broker_node_errors(self):
        error1 = BrokerNodeError('2020-01-01T00:00:00+01:00', '5', 'TestError')
        self.__init_new_dummy_and_put_payload_on_node('xxxApiKey123', error1)
        list_errors = self.__BROKER_NODE_CONNECTION.get_broker_node_errors('2')
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
