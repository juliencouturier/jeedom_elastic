#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time, logging, datetime, traceback, pytz
import os
import argparse
import requests
import elasticsearch
from elasticsearch.helpers import bulk
from datetime import datetime, timedelta
from json import JSONEncoder
import json
import glob
import six
import zipfile
try:
  import yaml
except:
  print('YAML is not available')

from hashlib import md5
from tzlocal import get_localzone

SYSTEM_TZ = get_localzone()


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
                                    "type": "keyword",
                                    "ignore_above": 256
                                },
                                "equipement": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                },
                                "objet": {
                                    "type": "keyword",
                                    "ignore_above": 256
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
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                        }
                }
        }

freediskspace = None

def get_free_disk_space():
    import psutil
    global freediskspace
    disk_usage = psutil.disk_usage('.')
    freediskspace = disk_usage.free
    return freediskspace

class JSONDateTimeEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%dT%H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime('%Y-%m-%d')
        # Let the base class default method raise the TypeError
        return JSONEncoder.default(self, obj)

class ElasticIndexer(object):
    def __init__(self, elastic_url, index_name = 'jeedom', batch_size=500, filename='jeedom_metrics.zip'):
        """ Initialize ES connection and set the mapping for jeedom objects"""
        self.batch_size=batch_size
        self.batch = []
        self.error_cnt = 0
        split_filename = filename.split('.')
        split_filename[-2] += '_%Y%m%d%H%M%S'
        self.filename = '.'.join(split_filename)
        self.filename = datetime.now().strftime(self.filename)
        self.freediskspace = None
        self.zipfile = None
        self.zipjsonfile = None
        self.zip_idx = 1

        try:
            self.ES = elasticsearch.Elasticsearch(elastic_url.split(','))
            self.metrics_date = SYSTEM_TZ.localize(datetime.now())
            self.index_template = index_name
            self.index_name = self.metrics_date.strftime(index_name)
            # if not self.ES.indices.exists(self.index_name):
            #     self.ES.indices.create(self.index_name)
            # try:
            #     mapping = self.ES.indices.get_mapping(index=self.index_name, doc_type='jeedom_metric')
            # except:
            #     mapping = []
            # if len(mapping) == 0:
            #     try:
            #         self.ES.indices.put_mapping(index=self.index_name, doc_type='jeedom_metric', body = jeedom_mapping)
            #     except:
            #         logger.exception(u'Cannot set elastic mapping')
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
        return False

    def push(self, my_info, my_id, save=True, index_name=None):
        self.batch.append({
            '_index': index_name or self.index_name,
            # '_type': 'jeedom_metric',
            '_id': my_id,
            '_source': my_info
        })
        if len(self.batch) > self.batch_size:
            return self.flush(save)

    def flush(self, save=True):
        if self.ES is not None:
            try:
                bulk(self.ES, self.batch)
                self.batch = []
                return True
            except:
                logger.exception(u'Cannot index documents')                
        self.error_cnt += 1
        if save and len(self.batch)>0:
            self.save_items([(es_bulk['_source'], es_bulk['_id']) for es_bulk in self.batch])
        self.batch = []
        return False

    @property
    def ok(self):
        return self.error_cnt == 0



    def save_items(self, items_to_save):
        """Save items into a file as json list"""
        if self.freediskspace is None:
            self.freediskspace = get_free_disk_space()
        # S'il reste moins de 1Go, on ne sauvegarde pas
        if self.freediskspace < 1024 * 1024 * 1024:
            logger.warning('Espace disponible insuffisant pour sauvegarder les données (%% Mb)' % freediskspace/1024/1024)
            return
        if self.zipfile is None:
            self.zipfile =  zipfile.ZipFile(self.filename, 'a', zipfile.ZIP_DEFLATED)
            try:
                self.zipjsonfile = self.zipfile.open('jeedom_metrics.json', 'w')
            except:
                logger.warning('Impossible d\'ouvrir en écriture un fichier "zippé"')
        if self.zipjsonfile is None:
            self.zipfile.writestr('jeedom_metrics_%s.json'% self.zip_idx, '\n'.join(format_items(items_to_save)))
            self.zip_idx += 1 
        else:
            for line in format_items(items_to_save):
                self.zipjsonfile.write(line+'\n')

    def close(self):
        if self.zipjsonfile is not None:
            self.zipjsonfile.close()
        if self.zipfile is not None:
            self.zipfile.close()

def format_items(items_to_save):
    for my_info, my_id  in items_to_save:
        yield json.dumps({'id' : my_id, 'document': my_info}, cls=JSONDateTimeEncoder)


def get_info(ES, jeedom_key, jeedom_url = 'http://127.0.0.1/core/api/jeeApi.php', index_name='jeedom'):
    r = requests.get('%s?apikey=%s&type=fullData' % (jeedom_url, jeedom_key))
    my_tz = pytz.timezone("Europe/Paris")
    metrics_date = SYSTEM_TZ.localize(datetime.now())
    cnt = 0
    if r.ok:
        for anobject in r.json():
            for aneq in anobject['eqLogics']:
                for acmd in aneq['cmds']:
                    if 'state' in acmd:
                        my_val = acmd['state']
                        try:
                            if isinstance(aneq['status'], list):
                                continue
                            cmd_date = datetime.strptime(aneq['status']['lastCommunication'], '%Y-%m-%d %H:%M:%S')
                            cmd_date = my_tz.localize(cmd_date)
                            my_info = {'objet' : anobject['name'], 'equipement' : aneq['name'], 'commande' : acmd['name'], 'lastCommunication' : cmd_date, 'timestamp' : metrics_date}
                            id_calc = md5()
                            for akey, aval in my_info.items():
                                id_calc.update(akey.encode('latin-1'))
                                if isinstance(aval, six.text_type):
                                    id_calc.update(aval.encode('latin-1'))
                                elif isinstance(aval, datetime):
                                    id_calc.update(aval.strftime('%Y-%m-%d %H:%M:%S'))
                                else:
                                    id_calc.update(str(aval))
                            if isinstance(my_val, six.text_type):
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
    return cnt


# def save_items(items_to_save, filename='jeedom_metrics.zip'):
#     """Save items into a file as json list"""
#     global freediskspace
#     if freediskspace is None:
#         get_free_disk_space()
#     # S'il reste moins de 1Go, on ne sauvegarde pas
#     if freediskspace < 1024 * 1024 * 1024:
#         logger.warning('Espace disponible insuffisant pour sauvegarder les données (% Mb)' % freediskspace/1024/1024)
#         return
#     with zipfile.ZipFile(filename, 'a', zipfile.ZIP_DEFLATED) as my_zip:
#         try:
#             with myzip.open('jeedom_metrics.json', 'w') as myfile:
#                 for my_info, my_id  in items_to_save:
#                     item_as_string = json.dumps({'id' : my_id, 'document': my_info}, cls=JSONDateTimeEncoder)
#                     myfile.write(item_as_string+'\n')
#     # with open(filename,'a') as myfile:


def get_files(filemask='jeedom_metrics*.zip'):
    """List files"""
    for afile in glob.glob(filemask):
        if os.path.isfile(afile):
            yield afile
        

def load_items(filename):
    """Load items from a file"""
    logger.info('Integration de %s' % filename)
    with zipfile.ZipFile(filename, 'r') as myzip:
        for afileinfo in myzip.infolist():
            with myzip.open(afileinfo.filename,'r') as myfile:
                for line in myfile:
                    try:
                        item_as_dict = json.loads(line)
                        item_as_dict['document']['timestamp'] = datetime.strptime(item_as_dict['document']['timestamp'],'%Y-%m-%dT%H:%M:%S')
                        yield (item_as_dict['document'], item_as_dict['id'])
                    except:
                        logger.exception(u'Cannot parse line %s' % line)
                        continue
            

class JeedomAction:
    @staticmethod
    def restore(*args, **kwargs):
        index_name=kwargs.get('elastic_index', 'jeedom')
        # ES = init_es_connection(kwargs.get('elastic_url'), index_name=index_name)
        ES = ElasticIndexer(kwargs.get('elastic_url'), index_name=index_name)
        for afile in get_files():
            fileresult = True
            for my_info, my_id in load_items(afile):
                if ES.push(my_info, my_id, index_name=my_info['timestamp'].strftime(ES.index_template), save=False) == False:
                    fileresult = False
            if fileresult:
                os.remove(afile)
        ES.flush(save=False)
        ES.close()


    @staticmethod
    def get_from_jeedom(*args, **kwargs):
        index_name=kwargs.get('elastic_index', 'jeedom')
        # ES = init_es_connection(kwargs.get('elastic_url'), index_name=index_name)
        ES = ElasticIndexer(kwargs.get('elastic_url'), index_name=index_name)
        get_info(ES, jeedom_url=kwargs.get('jeedom_url', 'http://127.0.0.1/core/api/jeeApi.php'), jeedom_key=kwargs.get('jeedom_key'), index_name=index_name)
        ES.flush()
    #        for my_info, my_id in load_items():
    #            ES.push(my_info, my_id, save=False, index_name=my_info['timestamp'].strftime(ES.index_template))
    #        ES.flush()
    #        if ES.ok and os.path.isfile('jeedom_metrics.json'):
    #            os.remove('jeedom_metrics.json')
    #        backup = load_items()
    #        while len(backup) > 0:
    #            my_info, my_id = backup.pop()
    #            ES.push(my_info, my_id)
    #        if ES.flush() and os.path.isfile('jeedom_metrics.json'):
    #            os.remove('jeedom_metrics.json')
        ES.close()



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
    parser.add_argument('-s', '--service', type=bool,
                        help='Make collect continuous')
    parser.add_argument('action', nargs='?', type=str, choices=['get_from_jeedom', 'restore'], default='get_from_jeedom')
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

    method = getattr(JeedomAction, args.action)

    if args.config is not None:
        try:
            config_dict = json.load(args.config)
        except:
            config_dict = yaml.parse(args.config)
    else:
        config_dict = args.__dict__

    if args.service is True:
        last_call = datetime.now()
        while True:            
            method(**config_dict)
            sleep_time = timedelta(minutes=1) - (datetime.now() - last_call)
            last_call = datetime.now()
            if sleep_time.total_seconds() > 0:
                time.sleep(sleep_time.total_seconds())            
    else:
        method(**config_dict)

