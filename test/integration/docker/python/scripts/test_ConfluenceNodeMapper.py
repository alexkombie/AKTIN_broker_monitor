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
        self.assertEqual('[1] Clinic1', dict_mapping['1']['COMMON'])
        self.assertEqual('[2] Clinic2', dict_mapping['2']['COMMON'])
        self.assertEqual('[3] ÄÜÖ äüö', dict_mapping['3']['COMMON'])
        self.assertEqual('[10] Clinic10', dict_mapping['10']['COMMON'])
        with self.assertRaises(KeyError):
            _ = dict_mapping['4']['COMMON']

    def test_get_node(self):
        node1 = self.__CONFLUENCE_NODE_MAPPER.get_node_from_mapping_dict('1')
        self.assertEqual('[1] Clinic1', node1['COMMON'])
        self.assertEqual(['label1','label2'], node1['JIRA_LABELS'])
        node10 = self.__CONFLUENCE_NODE_MAPPER.get_node_from_mapping_dict('10')
        self.assertEqual('[10] Clinic10', node10['COMMON'])
        self.assertEqual('100', node10['DAILY_IMPORT_THRESHOLD'])

    def test_get_nonexisting_node(self):
        node = self.__CONFLUENCE_NODE_MAPPER.get_node_from_mapping_dict('99')
        self.assertIsNone(node)

    def test_get_node_value(self):
        value = self.__CONFLUENCE_NODE_MAPPER.get_node_value_from_mapping_dict('3', 'COMMON')
        self.assertEqual('[3] ÄÜÖ äüö', value)

    def test_get_nonexisting_node_value(self):
        value = self.__CONFLUENCE_NODE_MAPPER.get_node_value_from_mapping_dict('2', 'NONEXISTING')
        self.assertIsNone(value)


if __name__ == '__main__':
    unittest.main()
