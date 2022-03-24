import unittest
from common import load_properties_file_as_environment
from common import ConfluenceNodeMapper


class TestConfluenceNodeMapper(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        load_properties_file_as_environment('settings.json')
        cls.__CONFLUENCE_NODE_MAPPER = ConfluenceNodeMapper()

    def test_get_mapping_dict(self):
        dict_mapping = self.__CONFLUENCE_NODE_MAPPER.get_mapping_dict()
        self.assertEqual(4, len(dict_mapping.keys()))
        self.assertEqual('Broker Node 1', dict_mapping['1']['COMMON'])
        self.assertEqual('Broker Node 2', dict_mapping['2']['COMMON'])
        self.assertEqual('Broker Node 3', dict_mapping['3']['COMMON'])
        with self.assertRaises(KeyError):
            _ = dict_mapping['4']['COMMON']

    def test_get_node(self):
        node = self.__CONFLUENCE_NODE_MAPPER.get_node_from_mapping_dict('2')
        self.assertEqual('Broker Node 2', node['COMMON'])
        self.assertEqual(['label1'], node['JIRA_LABELS'])
        self.assertEqual('120', node['DAILY_IMPORT_THRESHOLD'])

    def test_get_nonexisting_node(self):
        node = self.__CONFLUENCE_NODE_MAPPER.get_node_from_mapping_dict('99')
        self.assertIsNone(node)

    def test_get_node_value(self):
        value = self.__CONFLUENCE_NODE_MAPPER.get_node_value_from_mapping_dict('2', 'DAILY_IMPORT_THRESHOLD')
        self.assertEqual('120', value)

    def test_get_nonexisting_node_value(self):
        value = self.__CONFLUENCE_NODE_MAPPER.get_node_value_from_mapping_dict('2', 'NONEXISTING')
        self.assertIsNone(value)


if __name__ == '__main__':
    unittest.main()
