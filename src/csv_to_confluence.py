import json
import logging
import os
import sys
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import singledispatchmethod

import lxml.etree as ET
import pandas as pd
import requests

from bs4 import BeautifulSoup as bs
import os
import re

import pprint

from atlassian import Confluence

SPACE = 'AKTIN'
URL_CONFLUENCE = 'https://confluence-imi.ukaachen.de'
TOKEN = 'CHANGEME'

confluence = Confluence(
        url=URL_CONFLUENCE,
        token=TOKEN)
#confluence.page_exists(SPACE, 'Dummy Broker-Monitor')


def get_page():
    page_id = confluence.get_page_id(SPACE, 'Dummy Broker-Monitor')
    page = confluence.get_page_by_id(page_id, expand='body.storage')
    content = page['body']['storage']['value']
    print(content)


def update_page():
    page_id = confluence.get_page_id(SPACE, 'Dummy Broker-Monitor')
    page = confluence.get_page_by_id(page_id, expand='body.storage')
    content = page['body']['storage']['value']
    html = bs(content, 'html.parser')
    html.find(class_='clinic_since').string.replace_with('ABCDEF')
    confluence.update_page(page_id, page['title'], str(html))

update_page()

# html = bs(content, 'html.parser')
# _ = html.find(id='it-contact').string.replace_with('12345')

# broker connection for version/rscript/python/import-scripts

# needs mapping settings (node number to common name)

# for each node folder in workdir:
# check if in aktin confluence exists page:
# else create page:
# grap content:
# extract info
# update own temp html with content
# update confluence page

# !!!!!!!!!!!!!!!!!! send csv as attachements


"""
('<h1 class="auto-cursor-target">Statistiken</h1><ac:structured-macro '
 'ac:name="section" ac:schema-version="1" '
 'ac:macro-id="d55b509f-357c-44ba-93c2-1ab37a02b527"><ac:rich-text-body><p '
 'class="auto-cursor-target"><br /></p><ac:structured-macro ac:name="column" '
 'ac:schema-version="1" '
 'ac:macro-id="613fc405-b37e-4567-9fd3-0fb2f64e000a"><ac:rich-text-body><p '
 'class="auto-cursor-target"><br /></p><table '
 'class="fixed-table"><thead><tr><th>Klinik</th><td>Common '
 'Name</td></tr><tr><th colspan="1">Klinik seit</th><td '
 'colspan="1">changeme</td></tr></thead><colgroup><col style="width: 119.0px;" '
 '/><col style="width: 298.0px;" '
 '/></colgroup><tbody><tr><th>KIS</th><td>changeme</td></tr><tr><th '
 'colspan="1">Schnittstelle</th><td '
 'colspan="1">changeme</td></tr><tr><th>NA-Leiter*in</th><td>changeme</td></tr><tr><th>IT-Kontakt</th><td><p>changeme</p></td></tr></tbody></table><p '
 'class="auto-cursor-target"><br /></p><table '
 'class="fixed-table"><thead><tr><th style="text-align: center;" '
 'rowspan="2">patient</th><td style="text-align: '
 'center;"><strong>root</strong></td><td style="text-align: center;" '
 'colspan="1">changeme</td></tr><tr><td style="text-align: center;" '
 'colspan="1"><strong>format</strong></td><td style="text-align: center;" '
 'colspan="1">changeme</td></tr></thead><colgroup><col style="width: 119.0px;" '
 '/><col style="width: 101.0px;" /><col style="width: 197.0px;" '
 '/></colgroup><tbody><tr><th style="text-align: center;" '
 'rowspan="2">encounter</th><td style="text-align: center;" '
 'colspan="1"><strong>root</strong></td><td style="text-align: center;" '
 'colspan="1">changeme</td></tr><tr><td style="text-align: center;" '
 'colspan="1"><strong>format</strong></td><td style="text-align: center;" '
 'colspan="1">changeme</td></tr><tr><th style="text-align: center;" '
 'rowspan="2">billing</th><td style="text-align: center;" '
 'colspan="1"><strong>root</strong></td><td style="text-align: center;" '
 'colspan="1">changeme</td></tr><tr><td style="text-align: center;" '
 'colspan="1"><strong>format</strong></td><td style="text-align: center;" '
 'colspan="1">changeme</td></tr></tbody></table><p '
 'class="auto-cursor-target"><br '
 '/></p></ac:rich-text-body></ac:structured-macro><p '
 'class="auto-cursor-target"><br /></p><ac:structured-macro ac:name="column" '
 'ac:schema-version="1" '
 'ac:macro-id="8a573483-64d4-414a-ba5d-db6240f3719d"><ac:rich-text-body><p '
 'class="auto-cursor-target"><br /></p><table '
 'class="fixed-table"><thead><tr><th style="text-align: left;">Status</th><td '
 'style="text-align: center;"><p><ac:structured-macro ac:name="status" '
 'ac:schema-version="1" '
 'ac:macro-id="b9464e8d-d261-4a4a-b8dd-603b783ac68a"><ac:parameter '
 'ac:name="colour">Green</ac:parameter><ac:parameter '
 'ac:name="title">ONLINE</ac:parameter></ac:structured-macro></p><p><ac:structured-macro '
 'ac:name="status" ac:schema-version="1" '
 'ac:macro-id="499188cd-e24a-446f-b5ec-16f89fe38573"><ac:parameter '
 'ac:name="colour">Yellow</ac:parameter><ac:parameter ac:name="title">HIGH '
 'ERROR RATE</ac:parameter></ac:structured-macro></p><p><ac:structured-macro '
 'ac:name="status" ac:schema-version="1" '
 'ac:macro-id="4c7e708a-2797-46d6-a8c1-fa63aa46c8bd"><ac:parameter '
 'ac:name="colour">Yellow</ac:parameter><ac:parameter '
 'ac:name="title">DEVIATING '
 'IMPORTS</ac:parameter></ac:structured-macro></p><p><ac:structured-macro '
 'ac:name="status" ac:schema-version="1" '
 'ac:macro-id="8f806429-2e8d-4c17-af8b-939ea6d142fa"><ac:parameter '
 'ac:name="colour">Red</ac:parameter><ac:parameter ac:name="title">NO '
 'IMPORTS</ac:parameter></ac:structured-macro></p><p><ac:structured-macro '
 'ac:name="status" ac:schema-version="1" '
 'ac:macro-id="7c375a6e-95f3-40d0-88c9-1d4265413522"><ac:parameter '
 'ac:name="colour">Red</ac:parameter><ac:parameter '
 'ac:name="title">OFFLINE</ac:parameter></ac:structured-macro></p></td></tr><tr><th '
 'colspan="1">last-check</th><td style="text-align: center;" '
 'colspan="1"><pre>2022-02-07</pre></td></tr></thead><colgroup><col '
 'style="width: 110.0px;" /><col style="width: 205.0px;" '
 '/></colgroup><tbody><tr><th style="text-align: left;" '
 'colspan="1">last-contact</th><td style="text-align: center;" '
 'colspan="1"><span style="color: rgb(255,0,0);"><strong>2022-02-07 '
 '13:49:32</strong></span></td></tr><tr><th style="text-align: left;" '
 'colspan="1">last-start</th><td style="text-align: center;" '
 'colspan="1">2022-01-21 16:34:09</td></tr><tr><th style="text-align: left;" '
 'colspan="1">last-write</th><td style="text-align: center;" colspan="1"><span '
 'style="color: rgb(255,102,0);"><strong>2022-02-07 '
 '13:45:01</strong></span></td></tr><tr><th style="text-align: left;" '
 'colspan="1">last-reject</th><td style="text-align: center;" '
 'colspan="1">2022-02-07 11:00:03</td></tr></tbody></table><p '
 'class="auto-cursor-target"><br /></p><table '
 'class="fixed-table"><thead><tr><th style="text-align: center;" '
 'colspan="2">Global</th></tr><tr><th style="text-align: '
 'left;">imported</th><td style="text-align: '
 'center;">1544</td></tr></thead><colgroup><col style="width: 113.0px;" /><col '
 'style="width: 200.0px;" /></colgroup><tbody><tr><th style="text-align: '
 'left;" colspan="1">updated</th><td style="text-align: center;" '
 'colspan="1">2981</td></tr><tr><th style="text-align: left;" '
 'colspan="1">invalid</th><td style="text-align: center;" '
 'colspan="1">36</td></tr><tr><th style="text-align: left;" '
 'colspan="1">failed</th><td style="text-align: center;" '
 'colspan="1">0</td></tr><tr><th style="text-align: left;" colspan="1">error '
 'rate [%]</th><td style="text-align: center;" '
 'colspan="1">0.8</td></tr></tbody></table><p class="auto-cursor-target"><br '
 '/></p><table class="fixed-table"><thead><tr><th style="text-align: center;" '
 'colspan="2">Daily</th></tr><tr><th style="text-align: '
 'left;">imported</th><td style="text-align: '
 'center;">106</td></tr></thead><colgroup><col style="width: 115.0px;" /><col '
 'style="width: 199.0px;" /></colgroup><tbody><tr><th style="text-align: '
 'left;" colspan="1">updated</th><td style="text-align: center;" '
 'colspan="1">0</td></tr><tr><th style="text-align: left;" '
 'colspan="1">invalid</th><td style="text-align: center;" '
 'colspan="1">1</td></tr><tr><th style="text-align: left;" '
 'colspan="1">failed</th><td style="text-align: center;" '
 'colspan="1">0</td></tr><tr><th style="text-align: left;" colspan="1">error '
 'rate [%]</th><td style="text-align: center;" '
 'colspan="1">0.94</td></tr></tbody></table><p class="auto-cursor-target"><br '
 '/></p></ac:rich-text-body></ac:structured-macro><p '
 'class="auto-cursor-target"><br /></p><ac:structured-macro ac:name="column" '
 'ac:schema-version="1" '
 'ac:macro-id="a4306f72-a480-4498-98e7-f55689967cdb"><ac:rich-text-body><p '
 'class="auto-cursor-target"><br /></p><table '
 'class="fixed-table"><thead><tr><th style="text-align: left;">OS</th><td '
 'style="text-align: center;"><p><span style="color: '
 'rgb(0,0,0);">Ubuntu&nbsp;20.04.1&nbsp;LTS</span></p></td></tr></thead><colgroup><col '
 'style="width: 130.0px;" /><col style="width: 207.0px;" '
 '/></colgroup><tbody><tr><th style="text-align: left;" '
 'colspan="1">Kernel</th><td style="text-align: center;" colspan="1"><p><span '
 'style="color: rgb(0,0,0);">5.4.0-88-generic</span></p></td></tr><tr><th '
 'style="text-align: left;" colspan="1">Java</th><td style="text-align: '
 'center;" colspan="1"><p><span style="color: '
 'rgb(0,0,0);">Ubuntu/11.0.13</span></p></td></tr><tr><th style="text-align: '
 'left;" colspan="1">wildfly</th><td style="text-align: center;" '
 'colspan="1"><p><span style="color: '
 'rgb(0,0,0);">18.0.0.Final</span></p></td></tr><tr><th style="text-align: '
 'left;" colspan="1">apache2</th><td style="text-align: center;" '
 'colspan="1"><p><span style="color: '
 'rgb(0,0,0);">2.4.41-4ubuntu3.9</span></p></td></tr><tr><th '
 'style="text-align: left;" colspan="1">postgresql</th><td style="text-align: '
 'center;" colspan="1"><p><span style="color: '
 'rgb(0,0,0);">12.9-0ubuntu0.20.04.1</span></p></td></tr><tr><th '
 'style="text-align: left;" colspan="1">dwh-api</th><td style="text-align: '
 'center;" colspan="1"><p><span style="color: '
 'rgb(0,0,0);">0.10</span></p></td></tr><tr><th style="text-align: left;" '
 'colspan="1">dwh-j2ee</th><td style="text-align: center;" '
 'colspan="1"><p><span style="color: '
 'rgb(0,0,0);">dwh-j2ee-1.5.1rc1</span></p></td></tr></tbody></table><p '
 'class="auto-cursor-target"><br /></p><table '
 'class="fixed-table"><thead><tr><th style="text-align: left;">rscript</th><td '
 'style="text-align: left;"><p>r-cran-xml (3.99-0.3-2), r-cran-tidyverse '
 '(1.3.0-1), r-cran-lattice (0.20-40-1), libxml2-dev '
 '(2.9.10+dfsg-5ubuntu0.20.04.1), r-base-core (3.6.3-2), libcurl4-openssl-dev '
 '(7.68.0-1ubuntu2.7), libssl-dev '
 '(1.1.1f-1ubuntu2.8)</p></td></tr></thead><colgroup><col style="width: '
 '130.0px;" /><col style="width: 544.0px;" /></colgroup><tbody><tr><th '
 'style="text-align: left;" colspan="1">python</th><td style="text-align: '
 'left;" colspan="1"><p>python3-numpy (1:1.17.4-5ubuntu3), python3-psycopg2 '
 '(2.8.4-2), python3-zipp (1.0.0-1), python3 (3.8.2-0ubuntu2), python3-plotly '
 '(4.4.1+dfsg-1), python3-pandas (0.25.3+dfsg-7), python3-unicodecsv '
 '(0.14.1-2build1), python3-postgresql (1.2.1+git20180803.ef7b9a9-2build1), '
 'python3-sqlalchemy (1.3.12+ds1-1ubuntu2), python3-gunicorn (20.0.4-3), '
 'python3-requests (2.22.0-2ubuntu1)</p></td></tr><tr><th style="text-align: '
 'left;" colspan="1">import-scripts</th><td style="text-align: left;" '
 'colspan="1">p21 (1.5)</td></tr></tbody></table><p '
 'class="auto-cursor-target"><br '
 '/></p></ac:rich-text-body></ac:structured-macro><p '
 'class="auto-cursor-target"><br '
 '/></p></ac:rich-text-body></ac:structured-macro><p '
 'class="auto-cursor-target"><br /></p><hr /><h1 '
 'class="auto-cursor-target">Fehermeldungen #nur die letzten 20</h1><table '
 'class="fixed-table"><colgroup><col style="width: 150.0px;" /><col '
 'style="width: 76.0px;" /><col style="width: 1088.0px;" '
 '/></colgroup><tbody><tr><th style="text-align: center;">timestamp</th><th '
 'style="text-align: center;">repeats</th><th style="text-align: '
 'center;">content</th></tr><tr><td style="text-align: center;">2022-01-28 '
 '11:30:09</td><td style="text-align: center;">1</td><td>information: '
 'dtr1-13-II: Lower case UUID '
 '&quot;&quot;65a6948b-72c6-11e7-962b-0242ac120002&quot;&quot; found in @root. '
 'UUIDs SHALL, under official HL7 V3 Datatypes Release 1 (and 2) rules, have '
 'upper case hexadecimal digits A-F. RFC 4122 and HL7 FHIR state lower case '
 'UUID display.<br />error: dtr1-7-IVL_TS: low/@value must be before '
 'high/@value<br />error: dtr1-7-IVL_TS: low/@value must be before '
 'high/@value<br />information: dtr1-13-II: Lower case UUID '
 '&quot;&quot;45f99818-637b-4be7-bc22-a7041c1cf813&quot;&quot; found in @root. '
 'UUIDs SHALL, under official HL7 V3 Datatypes Release 1 (and 2) rules, have '
 'upper case hexadecimal digits A-F. RFC 4122 and HL7 FHIR state lower case '
 'UUID display.<br />fatal: XML validation not passed</td></tr><tr><td '
 'style="text-align: center;">2022-01-31 23:35:10</td><td style="text-align: '
 'center;">1</td><td>information: dtr1-13-II: Lower case UUID '
 '&quot;&quot;65a6948b-72c6-11e7-962b-0242ac120002&quot;&quot; found in @root. '
 'UUIDs SHALL, under official HL7 V3 Datatypes Release 1 (and 2) rules, have '
 'upper case hexadecimal digits A-F. RFC 4122 and HL7 FHIR state lower case '
 'UUID display.<br />information: dtr1-13-II: Lower case UUID '
 '&quot;&quot;bbfb672e-a620-4aa4-9117-bdd8b2505aaa&quot;&quot; found in @root. '
 'UUIDs SHALL, under official HL7 V3 Datatypes Release 1 (and 2) rules, have '
 'upper case hexadecimal digits A-F. RFC 4122 and HL7 FHIR state lower case '
 'UUID display.<br />information: dtr1-13-II: Lower case UUID '
 '&quot;&quot;45f99818-637b-4be7-bc22-a7041c1cf813&quot;&quot; found in @root. '
 'UUIDs SHALL, under official HL7 V3 Datatypes Release 1 (and 2) rules, have '
 'upper case hexadecimal digits A-F. RFC 4122 and HL7 FHIR state lower case '
 'UUID display.<br />error: Insert failed</td></tr><tr><td style="text-align: '
 'center;">2022-02-01 00:45:09</td><td style="text-align: '
 'center;">2</td><td>information: dtr1-13-II: Lower case UUID '
 '&quot;&quot;65a6948b-72c6-11e7-962b-0242ac120002&quot;&quot; found in @root. '
 'UUIDs SHALL, under official HL7 V3 Datatypes Release 1 (and 2) rules, have '
 'upper case hexadecimal digits A-F. RFC 4122 and HL7 FHIR state lower case '
 'UUID display.<br />information: dtr1-13-II: Lower case UUID '
 '&quot;&quot;45f99818-637b-4be7-bc22-a7041c1cf813&quot;&quot; found in @root. '
 'UUIDs SHALL, under official HL7 V3 Datatypes Release 1 (and 2) rules, have '
 'upper case hexadecimal digits A-F. RFC 4122 and HL7 FHIR state lower case '
 'UUID display.<br />error: Insert failed</td></tr></tbody></table><hr '
 '/><h1>&Uuml;bersicht Jira</h1><p><ac:structured-macro ac:name="jira" '
 'ac:schema-version="1" '
 'ac:macro-id="fe2df788-b650-4e89-8c5f-aa602629ef63"><ac:parameter '
 'ac:name="server">Jira IMI UK Aachen</ac:parameter><ac:parameter '
 'ac:name="columnIds">issuekey,summary,issuetype,created,updated,duedate,assignee,reporter,priority,status,resolution</ac:parameter><ac:parameter '
 'ac:name="columns">key,summary,type,created,updated,due,assignee,reporter,priority,status,resolution</ac:parameter><ac:parameter '
 'ac:name="maximumIssues">1000</ac:parameter><ac:parameter '
 'ac:name="jqlQuery">project=AKTIN AND Labels = &quot;klinikum-fuerth&quot;  '
 'OR Labels = &quot;fuerth&quot; OR Labels = '
 '&quot;f&uuml;rth&quot;                  </ac:parameter><ac:parameter '
 'ac:name="serverId">7b29f2e3-6219-30b0-acfd-687495fead3e</ac:parameter></ac:structured-macro></p><p '
 'class="auto-cursor-target"><br /></p>')
 
 <h1 class="auto-cursor-target">Statistiken</h1><ac:structured-macro ac:macro-id="d55b509f-357c-44ba-93c2-1ab37a02b527" ac:name="section" ac:schema-version="1"><ac:rich-text-body><p class="auto-cursor-target"><br/></p><ac:structured-macro ac:macro-id="613fc405-b37e-4567-9fd3-0fb2f64e000a" ac:name="column" ac:schema-version="1"><ac:rich-text-body><p class="auto-cursor-target"><br/></p><table class="fixed-table"><thead><tr><th>Klinik</th><td>Common Name</td></tr><tr><th colspan="1">Klinik seit</th><td colspan="1" class="clinic_since">changeme</td></tr></thead><colgroup><col style="width: 119.0px;"/><col style="width: 298.0px;"/></colgroup><tbody><tr><th>KIS</th><td>changeme</td></tr><tr><th colspan="1">Schnittstelle</th><td colspan="1">changeme</td></tr><tr><th>NA-Leiter*in</th><td>changeme</td></tr><tr><th>IT-Kontakt</th><td><p>changeme</p></td></tr></tbody></table><p class="auto-cursor-target"><br/></p><table class="fixed-table"><thead><tr><th rowspan="2" style="text-align: center;">patient</th><td style="text-align: center;"><strong>root</strong></td><td colspan="1" style="text-align: center;">changeme</td></tr><tr><td colspan="1" style="text-align: center;"><strong>format</strong></td><td colspan="1" style="text-align: center;">changeme</td></tr></thead><colgroup><col style="width: 119.0px;"/><col style="width: 101.0px;"/><col style="width: 197.0px;"/></colgroup><tbody><tr><th rowspan="2" style="text-align: center;">encounter</th><td colspan="1" style="text-align: center;"><strong>root</strong></td><td colspan="1" style="text-align: center;">changeme</td></tr><tr><td colspan="1" style="text-align: center;"><strong>format</strong></td><td colspan="1" style="text-align: center;">changeme</td></tr><tr><th rowspan="2" style="text-align: center;">billing</th><td colspan="1" style="text-align: center;"><strong>root</strong></td><td colspan="1" style="text-align: center;">changeme</td></tr><tr><td colspan="1" style="text-align: center;"><strong>format</strong></td><td colspan="1" style="text-align: center;">changeme</td></tr></tbody></table><p class="auto-cursor-target"><br/></p></ac:rich-text-body></ac:structured-macro><p class="auto-cursor-target"><br/></p><ac:structured-macro ac:macro-id="8a573483-64d4-414a-ba5d-db6240f3719d" ac:name="column" ac:schema-version="1"><ac:rich-text-body><p class="auto-cursor-target"><br/></p><table class="fixed-table"><thead><tr><th style="text-align: left;">Status</th><td style="text-align: center;"><p><ac:structured-macro ac:macro-id="b9464e8d-d261-4a4a-b8dd-603b783ac68a" ac:name="status" ac:schema-version="1"><ac:parameter ac:name="colour">Green</ac:parameter><ac:parameter ac:name="title">ONLINE</ac:parameter></ac:structured-macro></p><p><ac:structured-macro ac:macro-id="499188cd-e24a-446f-b5ec-16f89fe38573" ac:name="status" ac:schema-version="1"><ac:parameter ac:name="colour">Yellow</ac:parameter><ac:parameter ac:name="title">HIGH ERROR RATE</ac:parameter></ac:structured-macro></p><p><ac:structured-macro ac:macro-id="4c7e708a-2797-46d6-a8c1-fa63aa46c8bd" ac:name="status" ac:schema-version="1"><ac:parameter ac:name="colour">Yellow</ac:parameter><ac:parameter ac:name="title">DEVIATING IMPORTS</ac:parameter></ac:structured-macro></p><p><ac:structured-macro ac:macro-id="8f806429-2e8d-4c17-af8b-939ea6d142fa" ac:name="status" ac:schema-version="1"><ac:parameter ac:name="colour">Red</ac:parameter><ac:parameter ac:name="title">NO IMPORTS</ac:parameter></ac:structured-macro></p><p><ac:structured-macro ac:macro-id="7c375a6e-95f3-40d0-88c9-1d4265413522" ac:name="status" ac:schema-version="1"><ac:parameter ac:name="colour">Red</ac:parameter><ac:parameter ac:name="title">OFFLINE</ac:parameter></ac:structured-macro></p></td></tr><tr><th colspan="1">last-check</th><td colspan="1" style="text-align: center;"><pre>2022-02-07</pre></td></tr></thead><colgroup><col style="width: 110.0px;"/><col style="width: 205.0px;"/></colgroup><tbody><tr><th colspan="1" style="text-align: left;">last-contact</th><td colspan="1" style="text-align: center;"><span style="color: rgb(255,0,0);"><strong>2022-02-07 13:49:32</strong></span></td></tr><tr><th colspan="1" style="text-align: left;">last-start</th><td colspan="1" style="text-align: center;">2022-01-21 16:34:09</td></tr><tr><th colspan="1" style="text-align: left;">last-write</th><td colspan="1" style="text-align: center;"><span style="color: rgb(255,102,0);"><strong>2022-02-07 13:45:01</strong></span></td></tr><tr><th colspan="1" style="text-align: left;">last-reject</th><td colspan="1" style="text-align: center;">2022-02-07 11:00:03</td></tr></tbody></table><p class="auto-cursor-target"><br/></p><table class="fixed-table"><thead><tr><th colspan="2" style="text-align: center;">Global</th></tr><tr><th style="text-align: left;">imported</th><td style="text-align: center;">1544</td></tr></thead><colgroup><col style="width: 113.0px;"/><col style="width: 200.0px;"/></colgroup><tbody><tr><th colspan="1" style="text-align: left;">updated</th><td colspan="1" style="text-align: center;">2981</td></tr><tr><th colspan="1" style="text-align: left;">invalid</th><td colspan="1" style="text-align: center;">36</td></tr><tr><th colspan="1" style="text-align: left;">failed</th><td colspan="1" style="text-align: center;">0</td></tr><tr><th colspan="1" style="text-align: left;">error rate [%]</th><td colspan="1" style="text-align: center;">0.8</td></tr></tbody></table><p class="auto-cursor-target"><br/></p><table class="fixed-table"><thead><tr><th colspan="2" style="text-align: center;">Daily</th></tr><tr><th style="text-align: left;">imported</th><td style="text-align: center;">106</td></tr></thead><colgroup><col style="width: 115.0px;"/><col style="width: 199.0px;"/></colgroup><tbody><tr><th colspan="1" style="text-align: left;">updated</th><td colspan="1" style="text-align: center;">0</td></tr><tr><th colspan="1" style="text-align: left;">invalid</th><td colspan="1" style="text-align: center;">1</td></tr><tr><th colspan="1" style="text-align: left;">failed</th><td colspan="1" style="text-align: center;">0</td></tr><tr><th colspan="1" style="text-align: left;">error rate [%]</th><td colspan="1" style="text-align: center;">0.94</td></tr></tbody></table><p class="auto-cursor-target"><br/></p></ac:rich-text-body></ac:structured-macro><p class="auto-cursor-target"><br/></p><ac:structured-macro ac:macro-id="a4306f72-a480-4498-98e7-f55689967cdb" ac:name="column" ac:schema-version="1"><ac:rich-text-body><p class="auto-cursor-target"><br/></p><table class="fixed-table"><thead><tr><th style="text-align: left;">OS</th><td style="text-align: center;"><p><span style="color: rgb(0,0,0);">Ubuntu 20.04.1 LTS</span></p></td></tr></thead><colgroup><col style="width: 130.0px;"/><col style="width: 207.0px;"/></colgroup><tbody><tr><th colspan="1" style="text-align: left;">Kernel</th><td colspan="1" style="text-align: center;"><p><span style="color: rgb(0,0,0);">5.4.0-88-generic</span></p></td></tr><tr><th colspan="1" style="text-align: left;">Java</th><td colspan="1" style="text-align: center;"><p><span style="color: rgb(0,0,0);">Ubuntu/11.0.13</span></p></td></tr><tr><th colspan="1" style="text-align: left;">wildfly</th><td colspan="1" style="text-align: center;"><p><span style="color: rgb(0,0,0);">18.0.0.Final</span></p></td></tr><tr><th colspan="1" style="text-align: left;">apache2</th><td colspan="1" style="text-align: center;"><p><span style="color: rgb(0,0,0);">2.4.41-4ubuntu3.9</span></p></td></tr><tr><th colspan="1" style="text-align: left;">postgresql</th><td colspan="1" style="text-align: center;"><p><span style="color: rgb(0,0,0);">12.9-0ubuntu0.20.04.1</span></p></td></tr><tr><th colspan="1" style="text-align: left;">dwh-api</th><td colspan="1" style="text-align: center;"><p><span style="color: rgb(0,0,0);">0.10</span></p></td></tr><tr><th colspan="1" style="text-align: left;">dwh-j2ee</th><td colspan="1" style="text-align: center;"><p><span style="color: rgb(0,0,0);">dwh-j2ee-1.5.1rc1</span></p></td></tr></tbody></table><p class="auto-cursor-target"><br/></p><table class="fixed-table"><thead><tr><th style="text-align: left;">rscript</th><td style="text-align: left;"><p>r-cran-xml (3.99-0.3-2), r-cran-tidyverse (1.3.0-1), r-cran-lattice (0.20-40-1), libxml2-dev (2.9.10+dfsg-5ubuntu0.20.04.1), r-base-core (3.6.3-2), libcurl4-openssl-dev (7.68.0-1ubuntu2.7), libssl-dev (1.1.1f-1ubuntu2.8)</p></td></tr></thead><colgroup><col style="width: 130.0px;"/><col style="width: 544.0px;"/></colgroup><tbody><tr><th colspan="1" style="text-align: left;">python</th><td colspan="1" style="text-align: left;"><p>python3-numpy (1:1.17.4-5ubuntu3), python3-psycopg2 (2.8.4-2), python3-zipp (1.0.0-1), python3 (3.8.2-0ubuntu2), python3-plotly (4.4.1+dfsg-1), python3-pandas (0.25.3+dfsg-7), python3-unicodecsv (0.14.1-2build1), python3-postgresql (1.2.1+git20180803.ef7b9a9-2build1), python3-sqlalchemy (1.3.12+ds1-1ubuntu2), python3-gunicorn (20.0.4-3), python3-requests (2.22.0-2ubuntu1)</p></td></tr><tr><th colspan="1" style="text-align: left;">import-scripts</th><td colspan="1" style="text-align: left;">p21 (1.5)</td></tr></tbody></table><p class="auto-cursor-target"><br/></p></ac:rich-text-body></ac:structured-macro><p class="auto-cursor-target"><br/></p></ac:rich-text-body></ac:structured-macro><p class="auto-cursor-target"><br/></p><hr/><h1 class="auto-cursor-target">Fehermeldungen #nur die letzten 20</h1><table class="fixed-table"><colgroup><col style="width: 150.0px;"/><col style="width: 76.0px;"/><col style="width: 1088.0px;"/></colgroup><tbody><tr><th style="text-align: center;">timestamp</th><th style="text-align: center;">repeats</th><th style="text-align: center;">content</th></tr><tr><td style="text-align: center;">2022-01-28 11:30:09</td><td style="text-align: center;">1</td><td>information: dtr1-13-II: Lower case UUID ""65a6948b-72c6-11e7-962b-0242ac120002"" found in @root. UUIDs SHALL, under official HL7 V3 Datatypes Release 1 (and 2) rules, have upper case hexadecimal digits A-F. RFC 4122 and HL7 FHIR state lower case UUID display.<br/>error: dtr1-7-IVL_TS: low/@value must be before high/@value<br/>error: dtr1-7-IVL_TS: low/@value must be before high/@value<br/>information: dtr1-13-II: Lower case UUID ""45f99818-637b-4be7-bc22-a7041c1cf813"" found in @root. UUIDs SHALL, under official HL7 V3 Datatypes Release 1 (and 2) rules, have upper case hexadecimal digits A-F. RFC 4122 and HL7 FHIR state lower case UUID display.<br/>fatal: XML validation not passed</td></tr><tr><td style="text-align: center;">2022-01-31 23:35:10</td><td style="text-align: center;">1</td><td>information: dtr1-13-II: Lower case UUID ""65a6948b-72c6-11e7-962b-0242ac120002"" found in @root. UUIDs SHALL, under official HL7 V3 Datatypes Release 1 (and 2) rules, have upper case hexadecimal digits A-F. RFC 4122 and HL7 FHIR state lower case UUID display.<br/>information: dtr1-13-II: Lower case UUID ""bbfb672e-a620-4aa4-9117-bdd8b2505aaa"" found in @root. UUIDs SHALL, under official HL7 V3 Datatypes Release 1 (and 2) rules, have upper case hexadecimal digits A-F. RFC 4122 and HL7 FHIR state lower case UUID display.<br/>information: dtr1-13-II: Lower case UUID ""45f99818-637b-4be7-bc22-a7041c1cf813"" found in @root. UUIDs SHALL, under official HL7 V3 Datatypes Release 1 (and 2) rules, have upper case hexadecimal digits A-F. RFC 4122 and HL7 FHIR state lower case UUID display.<br/>error: Insert failed</td></tr><tr><td style="text-align: center;">2022-02-01 00:45:09</td><td style="text-align: center;">2</td><td>information: dtr1-13-II: Lower case UUID ""65a6948b-72c6-11e7-962b-0242ac120002"" found in @root. UUIDs SHALL, under official HL7 V3 Datatypes Release 1 (and 2) rules, have upper case hexadecimal digits A-F. RFC 4122 and HL7 FHIR state lower case UUID display.<br/>information: dtr1-13-II: Lower case UUID ""45f99818-637b-4be7-bc22-a7041c1cf813"" found in @root. UUIDs SHALL, under official HL7 V3 Datatypes Release 1 (and 2) rules, have upper case hexadecimal digits A-F. RFC 4122 and HL7 FHIR state lower case UUID display.<br/>error: Insert failed</td></tr></tbody></table><hr/><h1>Übersicht Jira</h1><p><ac:structured-macro ac:macro-id="fe2df788-b650-4e89-8c5f-aa602629ef63" ac:name="jira" ac:schema-version="1"><ac:parameter ac:name="server">Jira IMI UK Aachen</ac:parameter><ac:parameter ac:name="columnIds">issuekey,summary,issuetype,created,updated,duedate,assignee,reporter,priority,status,resolution</ac:parameter><ac:parameter ac:name="columns">key,summary,type,created,updated,due,assignee,reporter,priority,status,resolution</ac:parameter><ac:parameter ac:name="maximumIssues">1000</ac:parameter><ac:parameter ac:name="jqlQuery">project=AKTIN AND Labels = "klinikum-fuerth"  OR Labels = "fuerth" OR Labels = "fürth"                  </ac:parameter><ac:parameter ac:name="serverId">7b29f2e3-6219-30b0-acfd-687495fead3e</ac:parameter></ac:structured-macro></p><p class="auto-cursor-target"><br/></p>

 """
