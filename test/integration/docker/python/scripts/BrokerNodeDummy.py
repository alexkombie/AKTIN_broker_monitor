import os
from abc import ABC, abstractmethod
from dataclasses import dataclass

import requests


class Payload(ABC):

    @abstractmethod
    def to_xml_string(self) -> str:
        pass


@dataclass()
class BrokerNodeImports(Payload):
    __DWH_START: str
    __LAST_WRITE: str
    __LAST_REJECT: str
    __IMPORTED: str
    __UPDATED: str
    __INVALID: str
    __FAILED: str

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
class BrokerNodeError(Payload):
    __TIMESTAMP: str
    __REPEATS: str
    __CONTENT: str

    def to_xml_string(self) -> str:
        if self.__REPEATS:
            return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><import-statistics>" + \
                "<last-errors><error timestamp=\"" + self.__TIMESTAMP + "\" repeats=\"" + self.__REPEATS + "\">" + self.__CONTENT + "</error></last-errors>" + \
                "</import-statistics>"
        else:
            return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><import-statistics>" + \
                "<last-errors><error timestamp=\"" + self.__TIMESTAMP + "\">" + self.__CONTENT + "</error></last-errors>" + \
                "</import-statistics>"


@dataclass()
class BrokerNodeVersions(Payload):
    __JAVA: str
    __OS: str

    def to_xml_string(self) -> str:
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?><!DOCTYPE properties SYSTEM \"http://java.sun.com/dtd/properties.dtd\"><properties><comment>versions</comment>" + \
            "<entry key=\"java\">" + self.__JAVA + "</entry>" + \
            "<entry key=\"os\">" + self.__OS + "</entry>" + \
            "</properties>"


@dataclass()
class BrokerNodeVersions2(Payload):
    __JAVA: str
    __OS: str
    __APACHE2: str
    __POSTGRES: str

    def to_xml_string(self) -> str:
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?><!DOCTYPE properties SYSTEM \"http://java.sun.com/dtd/properties.dtd\"><properties><comment>versions</comment>" + \
            "<entry key=\"java\">" + self.__JAVA + "</entry>" + \
            "<entry key=\"os\">" + self.__OS + "</entry>" + \
            "<entry key=\"apache2\">" + self.__APACHE2 + "</entry>" + \
            "<entry key=\"postgres\">" + self.__POSTGRES + "</entry>" + \
            "</properties>"


@dataclass()
class BrokerNodeRscript(Payload):
    __CORE: str
    __TIDYVERSE: str
    __LATTICE: str

    def to_xml_string(self) -> str:
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?><!DOCTYPE properties SYSTEM \"http://java.sun.com/dtd/properties.dtd\"><properties><comment>rscript</comment>" + \
            "<entry key=\"r-base-core\">" + self.__CORE + "</entry>" + \
            "<entry key=\"r-cran-tidyverse\">" + self.__TIDYVERSE + "</entry>" + \
            "<entry key=\"r-cran-lattice\">" + self.__LATTICE + "</entry>" + \
            "</properties>"


@dataclass()
class BrokerNodePython(Payload):
    __CORE: str
    __NUMPY: str
    __PANDAS: str

    def to_xml_string(self) -> str:
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?><!DOCTYPE properties SYSTEM \"http://java.sun.com/dtd/properties.dtd\"><properties><comment>python</comment>" + \
            "<entry key=\"python3\">" + self.__CORE + "</entry>" + \
            "<entry key=\"python3-numpy\">" + self.__NUMPY + "</entry>" + \
            "<entry key=\"python3-pandas\">" + self.__PANDAS + "</entry>" + \
            "</properties>"


@dataclass()
class BrokerNodeImportScripts(Payload):
    __P21: str

    def to_xml_string(self) -> str:
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?><!DOCTYPE properties SYSTEM \"http://java.sun.com/dtd/properties.dtd\"><properties><comment>import-scripts</comment>" + \
            "<entry key=\"p21\">" + self.__P21 + "</entry>" + \
            "</properties>"


class BrokerNodeDummy:

    def __init__(self, api_key: str):
        self.__API_KEY = api_key

    def __create_basic_header(self) -> dict:
        return {'Authorization': ' '.join(['Bearer', self.__API_KEY]), 'Connection': 'keep-alive', 'Content-Type': 'application/xml', 'Accept-Charset': 'UTF-8'}

    @staticmethod
    def __append_to_broker_url(*items: str) -> str:
        url = os.environ['BROKER.URL']
        for item in items:
            url = '{}/{}'.format(url, item)
        return url

    def put_import_info_on_broker(self, payload: Payload):
        """
        Payload must be either BrokerImportStats or BrokerImportError
        """
        url = self.__append_to_broker_url('broker', 'my', 'node', 'stats')
        response = requests.put(url, data=payload.to_xml_string(), headers=self.__create_basic_header())
        response.raise_for_status()

    def reset_stats_on_broker(self):
        url = self.__append_to_broker_url('broker', 'my', 'node', 'stats')
        payload = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><import-statistics>" + \
                  "<start>2020-01-01T00:00:00+01:00</start><imported>0</imported><updated>0</updated>" + \
                  "<invalid>0</invalid><failed>0</failed><last-errors></last-errors></import-statistics>"
        response = requests.put(url, data=payload, headers=self.__create_basic_header())
        response.raise_for_status()

    def put_resource_on_broker(self, payload: Payload, type_resource: str):
        """
        Payload must be BrokerNodeVersions, BrokerNodeRscript, BrokerNodePyhton or BrokerNodeImportScripts
        """
        url = self.__append_to_broker_url('broker', 'my', 'node', type_resource)
        response = requests.put(url, data=payload.to_xml_string(), headers=self.__create_basic_header())
        response.raise_for_status()

    def put_empty_resource_on_broker(self, type_resource: str):
        url = self.__append_to_broker_url('broker', 'my', 'node', type_resource)
        response = requests.put(url, data=None, headers=self.__create_basic_header())
        response.raise_for_status()
