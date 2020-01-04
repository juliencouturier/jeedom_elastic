#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time, logging, datetime, traceback, pytz
import os
import argparse
import requests
import elasticsearch
from elasticsearch.helpers import bulk
from datetime import datetime
from json import JSONEncoder
import json
try:
  import yaml
except:
  print('YAML is not available')

from hashlib import md5

# jeedom_url = 'http://127.0.0.1/core/api/jeeApi.php'
# jeedom_key = 'YourJeedomKey'
# elastic_url = 'Your elasticsearch URL'

# create logger
logger = logging.getLogger('jeedom')
logger.setLevel(logging.INFO)


jeedom_mapping = {
                "jeedom_metric": {
                        "properties": {
                                "commande": {
                                        "type": "text",
                                        "fields": {
                                                "keyword": {
                                                        "type": "keyword",
                                                        "ignore_above": 256
                                                }
                                        }
                                },
                                "equipement": {
                                        "type": "text",
                                        "fields": {
                                                "keyword": {
                                                        "type": "keyword",
                                                        "ignore_above": 256
                                                }
                                        }
                                },
                                "objet": {
                                        "type": "text",
                                        "fields": {
                                                "keyword": {
                                                        "type": "keyword",
                                                        "ignore_above": 256
                                                }
                                        }
                                },
                                "timestamp": {
                                        "type": "date"
                                },
                                "lastCommunication": {
                                        "type": "date"
                                },
                                "value_numeric": {
                                        "type": "float"
                                },
                                "value_text": {
                                        "type": "text",
                                        "fields": {
                                                "keyword": {
                                                        "type": "keyword",
                                                        "ignore_above": 256
                                                }
                                        }
                                }
                        }
                }
        }

class JSONDateTimeEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%dT%H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime('%Y-%m-%d')
        # Let the base class default method raise the TypeError
        return JSONEncoder.default(self, obj)

class ElasticIndexer(object):
    def __init__(self, elastic_url, index_name = 'jeedom', batch_size=100):
        """ Initialize ES connection and set the mapping for jeedom objects"""
        self.batch_size=batch_size
        self.batch = []
        self.error_cnt = 0
        self.cnter = 0
        try:
            self.ES = elasticsearch.Elasticsearch(elastic_url.split(','))
            my_tz = pytz.timezone("Europe/Paris")
            self.metrics_date = my_tz.localize(datetime.now())
            self.index_template = index_name
            self.index_name = self.metrics_date.strftime(index_name)
            if not self.ES.indices.exists(self.index_name):
                self.ES.indices.create(self.index_name)
            try:
                mapping = self.ES.indices.get_mapping(index=self.index_name, doc_type='jeedom_metric')
            except:
                mapping = []
            if len(mapping) == 0:
                try:
                    self.ES.indices.put_mapping(index=self.index_name, doc_type='jeedom_metric', body = jeedom_mapping)
                except:
                    logger.exception(u'Cannot set elastic mapping')
        except:
            logger.exception(u'Cannot connect to elastic')
            self.ES = None
    
    def index(self, my_info, my_id):
        if self.ES is not None:
            try:
                self.ES.index(self.index_name, 'jeedom_metric', my_info, my_id)
                return True
            except:
                logger.exception(u'Cannot index document')
        self.error_cnt += 1
        return False

    def push(self, my_info, my_id, save=True, index_name=None):
        self.batch.append({
            '_index': index_name or self.index_name,
            '_type': 'jeedom_metric',
            '_id': my_id,
            '_source': my_info
        })
        if len(self.batch) > self.batch_size:
            self.flush(save)
    
    def flush(self, save=True):
        if self.ES is not None:
            try:
                bulk(self.ES, self.batch)
                self.batch = []
                return True
            except:
                logger.exception(u'Cannot index documents')
                self.batch = []
        self.error_cnt += 1
        if save:
            save_items([(es_bulk['_source'], es_bulk['_id']) for es_bulk in self.batch])
        return False
    
    @property
    def ok(self):
        return self.error_cnt == 0



def get_info(ES, jeedom_key, jeedom_url = 'http://127.0.0.1/core/api/jeeApi.php', index_name='jeedom'):
    r = requests.get('%s?apikey=%s&type=fullData' % (jeedom_url, jeedom_key))
    my_tz = pytz.timezone("Europe/Paris")
    metrics_date = pytz.utc.localize(datetime.now())
    cnt = 0
    if r.ok:
        for anobject in r.json():
            for aneq in anobject['eqLogics']:
                for acmd in aneq['cmds']:
                    if 'state' in acmd:
                        my_val = acmd['state']
                        try:
                            cmd_date = datetime.strptime(aneq['status']['lastCommunication'], '%Y-%m-%d %H:%M:%S')
                            cmd_date = my_tz.localize(cmd_date)
                            my_info = {'objet' : anobject['name'], 'equipement' : aneq['name'], 'commande' : acmd['name'], 'lastCommunication' : cmd_date, 'timestamp' : metrics_date}
                            id_calc = md5()
                            for akey, aval in my_info.items():
                                id_calc.update(akey.encode('latin-1'))
                                if isinstance(aval, unicode):
                                    id_calc.update(aval.encode('latin-1'))
                                elif isinstance(aval, datetime):
                                    id_calc.update(aval.strftime('%Y-%m-%d %H:%M:%S'))
                                else:
                                    id_calc.update(str(aval))
                            if isinstance(my_val, unicode):
                                if len(my_val) == 0:
                                    continue
                                my_info['value_text'] = acmd['state']
                            elif isinstance(my_val, float) or isinstance(my_val, int):
                                my_info['value_numeric'] = acmd['state']
                            else:
                                continue
                            my_id = id_calc.digest().encode('base64')
                            ES.push(my_info, my_id)
                            cnt += 1
                        except:
                            logger.exception('Can not index %s (%s)' % (acmd, aneq['status']))
        logger.info('%s document indexed' % cnt)
    else:
        logger.error('%s : %s' % (r.status_code, r.content))
    ES.flush()
    if ES.ok:
        return cnt
    else:
        return 0


def save_items(items_to_save, filename='jeedom_metrics.json'):
    """Save items into a file as json list"""
    with open(filename,'a') as myfile:
        for my_info, my_id  in items_to_save:
            item_as_string = json.dumps({'id' : my_id, 'document': my_info}, cls=JSONDateTimeEncoder)
            myfile.write(item_as_string+'\n')

def load_items(filename='jeedom_metrics.json'):
    """Load items from a file"""
    if os.path.isfile(filename):
        with open(filename,'r') as myfile:
            for line in myfile:
                try:
                    item_as_dict = json.loads(line)
                    yield (item_as_dict['document'], item_as_dict['id'])
                except:
                    logger.exception(u'Cannot parse line %s' % line)
                    continue


def main(*args, **kwargs):
    index_name=kwargs.get('elastic_index', 'jeedom')
    # ES = init_es_connection(kwargs.get('elastic_url'), index_name=index_name)
    ES = ElasticIndexer(kwargs.get('elastic_url'), index_name=index_name)
    if get_info(ES, jeedom_url=kwargs.get('jeedom_url', 'http://127.0.0.1/core/api/jeeApi.php'), jeedom_key=kwargs.get('jeedom_key'), index_name=index_name) > 0:
        for my_info, my_id in load_items():
            ES.push(my_info, my_id, save=False, index_name=my_info['timestamp'].strftime(ES.index_template))
        ES.flush()
        if ES.ok and os.path.isfile('jeedom_metrics.json'):
            os.remove('jeedom_metrics.json')



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Index jeedom informations into elastic')
    parser.add_argument('-j', '--jeedom_url', type=str, default='http://127.0.0.1/core/api/jeeApi.php',
                        help='Jeedom API URL like : http://127.0.0.1/core/api/jeeApi.php')
    parser.add_argument('-k', '--jeedom_key', type=str,
                        help='Jeedom API Key')
    parser.add_argument('-e', '--elastic_url', type=str, default='http://127.0.0.1:9200',
                        help='Elastic URL')
    parser.add_argument('-i', '--elastic_index', type=str, default='jeedom',
                        help='Elastic index to store data')
    parser.add_argument('-c', '--config', type=argparse.FileType('r'),
                        help='JSON or YAML config File with those parameters')
    args = parser.parse_args()
    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)

    if args.config is not None:
        try:
            config_dict = json.load(args.config)
        except:
            config_dict = yaml.parse(args.config)
        main(**config_dict)
    else:
        main(**args.__dict__)
