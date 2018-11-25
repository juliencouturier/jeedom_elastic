#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time, logging, datetime, traceback, pytz
import os
import argparse
import requests
import elasticsearch
from datetime import datetime
import json
try:
  import yaml
except:
  print('YAML is not available')

from hashlib import md5

# jeedom_url = 'http://127.0.0.1/core/api/jeeApi.php'
# jeedom_key = 'YourJeedomKey'
# elastic_url = 'Your elasticsearch URL'

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

def get_info(ES, jeedom_key, jeedom_url = 'http://127.0.0.1/core/api/jeeApi.php', index_name='jeedom'):
    r = requests.get('%s?apikey=%s&type=fullData' % (jeedom_url, jeedom_key))
    my_tz = pytz.timezone("Europe/Paris")
    metrics_date = my_tz.localize(datetime.now())
    backup_items = []
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
                            #print u"%s : %s" % (my_id, my_info)
                            try:
                                ES.index(metrics_date.strftime(index_name), 'jeedom_metric', my_info, my_id)
                                cnt += 1
                            except:
                                logging.exception(u'Cannot index document')
                                backup_items.append((my_info, my_id))
                        except:
                            logging.error(traceback.format_exc())
    else:
        logging.error('%s : %s' % (r.status_code, r.content))
    if len(backup_items) > 0:
        save_items(backup_items)
    return cnt

def init_es_connection(elastic_url, index_name = 'jeedom'):
    """ Initialize ES connection and set the mapping for jeedom objects"""
    try:
        ES = elasticsearch.Elasticsearch([elastic_url])
        my_tz = pytz.timezone("Europe/Paris")
        metrics_date = my_tz.localize(datetime.now())        
        if not ES.indices.exists(metrics_date.strftime(index_name)):
            ES.indices.create(metrics_date.strftime(index_name))
        try:
            mapping = ES.indices.get_mapping(index=metrics_date.strftime(index_name), doc_type='jeedom_metric')
        except:
            mapping = []
        if len(mapping) == 0:
            try:
                ES.indices.put_mapping(index=metrics_date.strftime(index_name), doc_type='jeedom_metric', body = jeedom_mapping)
            except:
                logging.exception(u'Cannot set elastic mapping')    
    except:
        logging.exception(u'Cannot connect to elastic')                    
        ES = None
    return ES


def save_items(items_to_save, filename='jeedom_metrics.json'):
    """Save items into a file as json list"""
    with open(filename,'a') as myfile:
        for my_info, my_id  in items_to_save:
            item_as_string = json.dumps({'id' : my_id, 'document': my_info})
            myfile.write(item_as_string+'\n')

def load_items(filename='jeedom_metrics.json'):
    """Load items from a file"""
    result_list = []
    line = ''    
    if os.path.isfile(filename):
        with open(filename,'r') as myfile:
            while line:
                line = myfile.readline()
                try:
                    item_as_dict = json.loads(line)
                    result_list.append((item_as_dict['document'], item_as_dict['id']))
                except:
                    logging.exception(u'Cannot parse line %s' % line)
                    continue
    return result_list
    

def main(*args, **kwargs):
    index_name=kwargs.get('elastic_index', 'jeedom')
    ES = init_es_connection(kwargs.get('elastic_url'), index_name=index_name)
    if get_info(ES, jeedom_url=kwargs.get('jeedom_url', 'http://127.0.0.1/core/api/jeeApi.php'), jeedom_key=kwargs.get('jeedom_key'), index_name=index_name) > 0:
        backup = load_items()
        while len(backup) > 0:        
            my_info, my_id = backup.pop()
            try:
                ES.index(my_info['timestamp'].strftime(index_name), 'jeedom_metric', my_info, my_id)
            except:
                logging.exception(u'Cannot index document')
                break  
        if len(backup) == 0 and os.path.isfile('jeedom_metrics.json'):
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
    if args.config is not None:
        try:
            config_dict = json.load(args.config)
        except:
            config_dict = yaml.parse(args.config)        
        main(**config_dict)
    else:
        main(**args.__dict__)        