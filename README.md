# broker-monitor ![Python 3.8.10](https://img.shields.io/badge/python-3.8.10-blue)

Simple scripts that monitor connected node activity of the [AKTIN Broker](https://github.com/aktin/broker). The whole process is divided into several scripts. All reusable components are stored in `common.py`:

* `node_to_csv.py` retrieves information from broker connected nodes and stores them locally. Import statistics and import errors are stored in CSV files. Resource information is written into text files.
* `csv_to_confluence.py` takes the generated files and fills a HTML template of a Confluence page with their content. The template is then uploaded to Confluence. Existing pages on Confluence are updated in the same way.
* `email_service.py` checks the pages generated in Confluence for a specific node status and informs the node's correspondents via e-mail if there is an interruption in connection or data import.

## Usage

A JSON configuration file with the following key-value pairs is required to run the scripts (see also the example in the integration tests):

| Parameter               |  Description                                                                                                              | Example                  |
|-------------------------|---------------------------------------------------------------------------------------------------------------------------|--------------------------|
| BROKER_URL              | URL to your broker server                                                                                                 | http://localhost:8080    |
| ADMIN_API_KEY           | API key of your broker server administrator                                                                               | xxxAdmin1234             |
| ROOT_DIR                | Working directory of the script. Directories for each connected node to store the retrieved information are created here. | /opt                     |
| RESOURCES_DIR           | Path to the directory with HTML templates and other resources                                                             | /opt/resources           |
| CONFLUENCE_URL          | URL to your confluence server                                                                                             | http://my-confluence.com |
| CONFLUENCE_SPACE        | Your Confluence space where the pages with node information should be created                                             | MY_SPACE                 |
| CONFLUENCE_TOKEN        | Your token for authentication in Confluence                                                                               | jAzMjQ4Omy               |
| CONFLUENCE_MAPPING_JSON | Path to the confluence json mapping file                                                                                  | /opt/mapping.json        |
| EMAIL_HOST              | URL to your mailing server                                                                                                | http://localhost:8888    |
| EMAIL_USER              | Your user of your mailing server                                                                                          | myuser@myserver.net      |
| EMAIL_PASSWORD          | The password to your mailing server user                                                                                  | Hc5sGhdr2577             |

The configuration file must be passed to the scripts as an input argument. The script `common.py` must be located in the same folder as the executed script:

```
python3 node_to_csv.py {PATH_TO_SETTINGS_FILE}
```

Additionally, the script `csv_to_confluence.py` needs a mapping table (`CONFLUENCE_MAPPING_JSON`) to map the ID of the broker nodes to static node-reladed information:

```
"99": {
    "COMMON_NAME": "[99] Default Clinic",
    "LONG_NAME": "Institute of Ninety-Nine",
    "JIRA_LABELS": [
      "label1",
      "label2",
    ],
    "HOSPITAL_INFORMATION_SYSTEM" : "HyperHIS",
    "IMPORT_INTERFACE": "SuperImporter V3.3",
    "THRESHOLD_HOURS_FAILURE" : 48,
    "ROOT": {
        "PATIENT": "1.2.2",
        "ENCOUNTER": "1.2.45",
        "BILLING": "1.2.47"
    },
    "FORMAT": {
        "PATIENT": "1111",
        "ENCOUNTER": "2222",
        "BILLING": "3333"
    }
}
```

| Parameter                   | Description                                                                                                                                                                                                       | Example                                                                  |
|-----------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------|
| NODE_ID                     | Id of the corresponding node. Used to map the import statistics with other clinic-related information                                                                                                             | 99                                                                       |
| COMMON_NAME                 | Short qualified name of the node. Is used as the name of the created Confluence page. This is the only required key in the dictionary.                                                                            | [99] Default Clinic                                                      |
| LONG_NAME                   | Official name of the node/the institution. If this key is empty, the value "changeme" is used.                                                                                                                    | Institute of Ninety-Nine                                                 |
| JIRA_LABELS                 | List of labels to define a JIRA query and pass to a table for JIRA tickets inside the Confluence page. If this key is empty, an empty JIRA table is created in the Confluence page.                               | ["label1", "label2"]                                                     |
| THRESHOLD_HOURS_FAILURE     | Integer, after how many hours of no imports/no broker contact the status of the node is changed. If this key is empty, a default value of 24 is used.                                                             | 48                                                                       |
| HOSPITAL_INFORMATION_SYSTEM | The hospital information system used by the node. If this key is empty, the value "changeme" is used.                                                                                                             | HyperHIS                                                                 |
| IMPORT_INTERFACE            | The AKTIN import interface used by the node. If this key is empty, the value "changeme" is used.                                                                                                                  | SuperImporter V3.3                                                       |
| ROOT                        | The root ids used in the CDAs of the node. "PATIENT", "ENCOUNTER" and "BILLING" are the only possible keys. Other keys are ignored. If a key is missing, the value "changeme" is used instead.                    | {"PATIENT": "1.2.2",<br/>"ENCOUNTER": "1.2.45",<br/>"BILLING": "1.2.47"} |
| FORMAT                      | The format of the extension ids used in the CDAs of the node. "PATIENT", "ENCOUNTER" and "BILLING" are the only possible keys. Other keys are ignored. If a key is missing, the value "changeme" is used instead. | {"PATIENT": "1111",<br/>"ENCOUNTER": "2222",<br/>"BILLING": "3333"}      |

## Testing

To test the script, **integration-test.bat** and **integration-test.sh** are attached. To run an integration test, a running instance of [Docker](https://www.docker.com/) is required. The script will create a container to
simulate the [AKTIN Broker Server](https://github.com/aktin/broker/tree/master/broker-server) and a second container to run the scripts on. Every class of the scripts, which does not need a connection to Confluence or the
E-Mail-Server, is tested within the integration tests.
