import unittest

from common import PropertiesReader, ResourceLoader


class TestResourceLoader(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        PropertiesReader().load_properties_as_env_vars('settings.json')
        cls.__RESOURCE_LOADER = ResourceLoader()

    def test_get_resource_as_string(self):
        resource = self.__RESOURCE_LOADER.get_resource_as_string('template_page.html')
        self.assertEqual(str, type(resource))


if __name__ == '__main__':
    unittest.main()
