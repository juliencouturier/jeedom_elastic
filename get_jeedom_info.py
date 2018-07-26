import time, logging, datetime, traceback, pytz
import argparse
import requests, elasticsearch
from hashlib import md5

jeedom_url = 'http://127.0.0.1/core/api/jeeApi.php'
jeedom_key = 'YourJeedomKey'
elastic_url = 'Your elasticsearch URL'

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

def get_info():
    r = requests.get('%s?apikey=%s&type=fullData' % (jeedom_url, jeedom_key))
    my_tz = pytz.timezone("Europe/Paris")
    if r.ok:
        ES = elasticsearch.Elasticsearch([elastic_url])
        if not ES.indices.exists('jeedom'):
            ES.indices.create('jeedom')
        try:
            mapping = ES.indices.get_mapping(index='jeedom', doc_type='jeedom_metric')
        except:
            mapping = []
        if len(mapping) == 0:
            ES.indices.put_mapping(index='jeedom', doc_type='jeedom_metric', body = jeedom_mapping)
        for anobject in r.json():
            for aneq in anobject['eqLogics']:
                for acmd in aneq['cmds']:
                    if 'state' in acmd:
                        my_val = acmd['state']
                        try:
                            cmd_date = datetime.datetime.strptime(aneq['status']['lastCommunication'], '%Y-%m-%d %H:%M:%S')
                            cmd_date = my_tz.localize(cmd_date)
                            my_info = {'objet' : anobject['name'], 'equipement' : aneq['name'], 'commande' : acmd['name'], 'timestamp' : cmd_date}
                            id_calc = md5()
                            for akey, aval in my_info.iteritems():
                                id_calc.update(akey.encode('latin-1'))
                                if isinstance(aval, unicode):
                                  id_calc.update(aval.encode('latin-1'))
                                elif isinstance(aval, datetime.datetime):
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
                            ES.index('jeedom', 'jeedom_metric', my_info, my_id)
                        except:
                            logging.error(traceback.format_exc())
    else:
        logging.error('%s : %s' % (r.status_code, r.content))

def main():
    get_info()

if __name__ == '__main__':
    main()