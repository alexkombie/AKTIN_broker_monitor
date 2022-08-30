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
        self.assertEqual('[1] Clinic1', node1['COMMON_NAME'])
        self.assertEqual(['label1', 'label2'], node1['JIRA_LABELS'])
        node10 = self.__CONFLUENCE_NODE_MAPPER.get_node_from_mapping_dict('10')
        self.assertEqual('[10] Clinic10', node10['COMMON_NAME'])
        with self.assertRaises(KeyError):
            _ = node10['NONEXISTING']

    def test_get_node_int_values(self):
        hours1 = self.__CONFLUENCE_NODE_MAPPER.get_node_value_from_mapping_dict('2', 'THRESHOLD_HOURS_FAILURE')
        self.assertEqual(48, hours1)
        self.assertEqual(int, type(hours1))
        hours2 = self.__CONFLUENCE_NODE_MAPPER.get_node_value_from_mapping_dict('3', 'THRESHOLD_HOURS_FAILURE')
        self.assertEqual(12, hours2)
        self.assertEqual(int, type(hours2))

    def test_get_nonexisting_node(self):
        node = self.__CONFLUENCE_NODE_MAPPER.get_node_from_mapping_dict('99')
        self.assertIsNone(node)

    def test_get_node_value(self):
        value = self.__CONFLUENCE_NODE_MAPPER.get_node_value_from_mapping_dict('3', 'COMMON_NAME')
        self.assertEqual('[3] ÄÜÖ äüö', value)

    def test_get_nonexisting_node_value(self):
        value = self.__CONFLUENCE_NODE_MAPPER.get_node_value_from_mapping_dict('2', 'NONEXISTING')
        self.assertIsNone(value)

    def test_get_root_dict(self):
        dict_root = self.__CONFLUENCE_NODE_MAPPER.get_node_value_from_mapping_dict('10', 'ROOT')
        dict_expected = {'PATIENT': '1.2.2'}
        self.assertTrue(dict_root == dict_expected)

    def test_get_format_dict(self):
        dict_root = self.__CONFLUENCE_NODE_MAPPER.get_node_value_from_mapping_dict('3', 'FORMAT')
        dict_expected = {'PATIENT': '1111', 'ENCOUNTER': '2222', 'BILLING': '3333'}
        self.assertTrue(dict_root == dict_expected)


if __name__ == '__main__':
    unittest.main()
