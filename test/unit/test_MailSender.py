import os
import sys
import unittest
from email.mime.text import MIMEText
from pathlib import Path

this_path = Path(os.path.realpath(__file__))
path_src = os.path.join(this_path.parents[2], 'src')
sys.path.insert(0, path_src)

from common import MailSender, ConfigReader


class TestMailSender(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        path_settings = os.path.join(this_path.parents[1], 'resources', 'settings.toml')
        ConfigReader().load_config_as_env_vars(path_settings)
        cls.__MAIL_SENDER = MailSender()

    def test_mailing(self):
        """
        Sent mail to configured static recipients
        """
        mail = MIMEText('test', 'html', 'utf-8')
        self.__MAIL_SENDER.send_mail([], mail)


if __name__ == '__main__':
    unittest.main()
