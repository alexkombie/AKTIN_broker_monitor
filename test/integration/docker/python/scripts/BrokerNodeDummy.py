import os
from dataclasses import dataclass

import requests


class BrokerNodeDummy:

    def __init__(self, api_key: str):
        self.__BROKER_CLIENT_URL = self.__append_to_broker_url('broker', 'my', 'node', 'stats')
        self.__API_KEY = api_key

    def __create_basic_header(self) -> dict:
        return {'Authorization': ' '.join(['Bearer', self.__API_KEY]), 'Connection': 'keep-alive', 'Content-Type': 'application/xml', 'Accept-Charset': 'UTF-8'}

    @staticmethod
    def __append_to_broker_url(*items: str) -> str:
        url = os.environ['BROKER_URL']
        for item in items:
            url = '{}/{}'.format(url, item)
        return url

    def put_stats_object_on_broker(self, payload):
        """
        Payload must be either BrokerImportStats or BrokerImportError
        """
        response = requests.put(self.__BROKER_CLIENT_URL, data=payload.to_xml_string(), headers=self.__create_basic_header())
        response.raise_for_status()


@dataclass()
class BrokerImportStats:
    __DWH_START: str
    __LAST_WRITE: str
    __LAST_REJECT: str
    __IMPORTED: str
    __UPDATED: str
    __INVALID: str
    __FAILED: str

    @property
    def dwh_start(self) -> str:
        return self.__DWH_START

    @property
    def last_write(self) -> str:
        return self.__LAST_WRITE

    @property
    def last_reject(self) -> str:
        return self.__LAST_REJECT

    @property
    def imported(self) -> str:
        return self.__IMPORTED

    @property
    def updated(self) -> str:
        return self.__UPDATED

    @property
    def invalid(self) -> str:
        return self.__INVALID

    @property
    def failed(self) -> str:
        return self.__FAILED

    def to_xml_string(self) -> str:
        last_write = "<last-write>" + self.__LAST_WRITE + "</last-write>" if self.__LAST_WRITE else ""
        last_reject = "<last-reject>" + self.__LAST_REJECT + "</last-reject>" if self.__LAST_REJECT else ""
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><import-statistics>" + \
               "<start>" + self.__DWH_START + "</start>" + \
               last_write + \
               last_reject + \
               "<imported>" + self.__IMPORTED + "</imported>" + \
               "<updated>" + self.__UPDATED + "</updated>" + \
               "<invalid>" + self.__INVALID + "</invalid>" + \
               "<failed>" + self.__FAILED + "</failed>" + \
               "<last-errors></last-errors></import-statistics>"


@dataclass()
class BrokerNodeError:
    __TIMESTAMP: str
    __REPEATS: str
    __CONTENT: str

    @property
    def repeats(self) -> str:
        return self.__REPEATS

    @property
    def timestamp(self) -> str:
        return self.__TIMESTAMP

    @property
    def content(self) -> str:
        return self.__CONTENT

    def to_xml_string(self) -> str:
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><import-statistics>" + \
               "<last-errors><error timestamp=\"" + self.__TIMESTAMP + "\" repeats=\"" + self.__REPEATS + "\">" + self.__CONTENT + "</error></last-errors>" + \
               "</import-statistics>"
