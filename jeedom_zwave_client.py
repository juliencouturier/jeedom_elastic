#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time, logging, datetime, traceback
import os
import argparse
import requests
import jmespath
try:
    import urllib.parse
except:
    import urllib    
from datetime import datetime
import json
try:
  import yaml
except:
  print('YAML is not available')

logger = logging.getLogger(__name__)


class OpenZwaveClient(object):
    def __init__(self, url, key):
        self.base_url = url
        self.key = key
        self.sesssion = requests.session()

    def __get__(self, *args, **kwargs ):
        params = {'apikey' : self.key}
        params.update(kwargs)
        try:
            get_args = urllib.parse.urlencode(params)
        except:
            get_args = urllib.urlencode(params)
        url = '/'.join([self.base_url]+list(args)) + '?' + get_args
        logger.debug('GET : %s' % url)
        r = self.sesssion.get(url)
        if r.ok:
            return r.json()
        else:
            raise IOError(r.content)

    def get_network_health(self):
        return self.__get__('network', type='info', info='getHealth')

    def get_network_status(self):
        return self.__get__('network', type='info', info='getStatus')              

    def get_network_neighbours(self):
        return self.__get__('network', type='info', info='getNeighbours')                

    def get_network_config(self):
        return self.__get__('network', type='info', info='getZWConfig')                        

    def get_node_list(self):
        return self.__get__('network', type='info', info='getNodesList')        

    def get_node_info(self, node_id):
        return self.__get__('node', type='info', info='all', node_id=node_id)        

    def get_node_info(self, node_id):
        return self.__get__('node', type='info', info='all', node_id=node_id)        

    def get_node_stats(self, node_id):
        return self.__get__('node', type='info', info='getNodeStatistics', node_id=node_id)                    

    def get_node_health(self, node_id):
        return self.__get__('node', type='info', info='getHealth', node_id=node_id)       

    def test_node(self, node_id):
        return self.__get__('node', type='action', action='testNode', node_id=node_id)               

    def test_failed_node(self):
        node_list = self.get_node_list()
        if node_list['state'] == 'ok':
            for node_id in jmespath.search('result.devices', node_list).keys():
                health = self.get_node_health(node_id)
                if jmespath.search('result.data.isFailed.value', health):
                    logger.info('Test failed node %s' % node_id)
                    self.test_node(node_id)


def main(*args, **kwargs):    
    base_url = kwargs.get('openzwave_url', 'http://127.0.0.1:8083')
    api_key = kwargs.get('api_key')
    client = OpenZwaveClient(base_url, api_key)
    client.test_failed_node()
        


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Index jeedom informations into elastic')
    parser.add_argument('-o', '--openzwave_url', type=str, default='http://127.0.0.1:8083',
                        help='Jeedom openzwave URL like : http://127.0.0.1:8083')
    parser.add_argument('-k', '--api_key', type=str, 
                        help='openzwave API Key')
    parser.add_argument('-c', '--config', type=argparse.FileType('r'),
                        help='JSON or YAML config File with those parameters')                                            
    args = parser.parse_args()
    if args.config is not None:
        try:
            config_dict = json.load(args.config)
        except:
            config_dict = yaml.parse(args.config)        
        main(**config_dict)
    else:
        main(**args.__dict__)        
