# jeedom_elastic
Store jeedom metrics into elasticsearch database

# Requirements
* requests
* elasticsearch

# Parameters
Change the folowing parameters at the begining of get_jeedom_info.py
* jeedom_url : The URL of jeedom API, it should end with /core/api/jeeApi.php
* jeedom_key : The API Key define in jeedom
* elastic_url : your elasticsearch url or IP
