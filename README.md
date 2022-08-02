# broker-monitor [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=aktin_AKTIN_broker_monitor&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=aktin_AKTIN_broker_monitor) ![Python 3.8.10](https://img.shields.io/badge/python-3.8.10-blue)

Simple scripts that monitor connected node activity of the [AKTIN Broker](https://github.com/aktin/broker). The whole process is divided into several scripts. All reusable components are stored in `common.py`:

* `node_to_csv.py` retrieves information from broker connected nodes and stores them locally. Import statistics and import errors are stored in CSV files. Resource information is written into text files.
* `csv_to_confluence.py` takes the generated files and fills a HTML template of a Confluence page with their content. The template is then uploaded to Confluence. Existing pages on Confluence are updated in the same way.
* `email_service.py` checks the pages generated in Confluence for a specific node status and informs the node's correspondents via e-mail if there is an interruption in connection or data import.

## Usage

A JSON configuration file with the following key-value pairs is required to run the scripts (see also the example in the integration tests):

| Parameter               | Description                                                                                                               | Example                  |
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

Additionally, the script `csv_to_confluence.py` needs a mapping table to map the ID of the broker nodes to a human-readable name (`COMMON`). This name also acts as the name of the created Confluence page. Inside the mapping table, the
optional keys `JIRA_LABELS` and `LONG` can be set. From the list of `JIRA_LABELS` a JIRA query is defined and passed to a table for JIRA tickets inside the Confluence page. The key `LONG` is used by the script as the "
official" name of the node (see the key `clinic_name` in `resources/template_page.html`).

```
"99": {
    "COMMON": "[99] Default Clinic",
    "JIRA_LABELS": [
      "label1",
      "label2",
    ],
    "LONG": "Institute of Ninety-Nine"
```

## Testing

To test the script, **integration-test.bat** and **integration-test.sh** are attached. To run an integration test, a running instance of [Docker](https://www.docker.com/) is required. The script will create a container to
simulate the [AKTIN Broker Server](https://github.com/aktin/broker/tree/master/broker-server) and a second container to run the scripts on. Every class of the scripts, which does not need a connection to Confluence or the E-Mail-Server, is tested
within the integration tests.
