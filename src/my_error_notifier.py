from email.mime.text import MIMEText

from common import MailSender


class MyErrorNotifier:
    __TEXT_SUBTYPE: str = 'html'
    __PARSER: str = 'html.parser'
    __ENCODING: str = 'iso-8859-1'

    __NOTIFIER_EMAIL_ADRESS: str = 'CHANGEME'

    def __init__(self, name_script: str):
        self.__NAME_SCRIPT = name_script

    def notify_me(self, exception: str):
        mail = MIMEText(exception, self.__TEXT_SUBTYPE, self.__ENCODING)
        mail['Subject'] = 'ERROR in script {0}'.format(self.__NAME_SCRIPT)
        MailSender().send_mail([self.__NOTIFIER_EMAIL_ADRESS], mail)
