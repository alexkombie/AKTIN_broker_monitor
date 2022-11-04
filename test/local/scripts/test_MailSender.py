import os
import sys
import unittest
from email.mime.text import MIMEText
from pathlib import Path

this_path = Path(os.path.realpath(__file__))
path_src = os.path.join(this_path.parents[3], 'src')
sys.path.insert(0, path_src)

from common import MailSender, PropertiesReader


class TestMailSender(unittest.TestCase):
    __MAIL_RECEIVER: str = 'CHANGEME'

    @classmethod
    def setUpClass(cls):
        path_settings = os.path.join(this_path.parents[1], 'settings.json')
        PropertiesReader().load_properties_as_env_vars(path_settings)
        cls.__MAIL_SENDER = MailSender()

    def test_mailing(self):
        mail = MIMEText('test', 'html', 'iso-8859-1')
        self.__MAIL_SENDER.send_mail([self.__MAIL_RECEIVER], mail)


if __name__ == '__main__':
    unittest.main()
