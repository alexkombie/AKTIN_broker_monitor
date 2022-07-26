import unittest

from common import ConfluenceNodeMapper, PropertiesReader


class TestConfluenceNodeMapper(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        PropertiesReader().load_properties_as_env_vars('settings.json')
        cls.__CONFLUENCE_NODE_MAPPER = ConfluenceNodeMapper()

    def test_get_mapping_dict(self):
        list_keys = self.__CONFLUENCE_NODE_MAPPER.get_all_keys()
        self.assertEqual(4, len(list_keys))

    def test_get_node(self):
        node1 = self.__CONFLUENCE_NODE_MAPPER.get_node_from_mapping_dict('1')
        self.assertEqual('[1] Clinic1', node1['COMMON'])
        self.assertEqual(['label1', 'label2'], node1['JIRA_LABELS'])
        node10 = self.__CONFLUENCE_NODE_MAPPER.get_node_from_mapping_dict('10')
        self.assertEqual('[10] Clinic10', node10['COMMON'])
        with self.assertRaises(KeyError):
            _ = node10['NONEXISTING']

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
