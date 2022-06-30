import elasticsearch
from elasticsearch.helpers import bulk
import logging
from datetime import datetime, timedelta
from hashlib import md5
import base64

logger = logging.getLogger('Reindexer')
logging.getLogger('urllib3').setLevel(logging.CRITICAL)
logging.getLogger('elasticsearch').setLevel(logging.CRITICAL)

INDEX_AGG_PATERN = 'jeedom_agg.%Y'
INDEX_DETAIL_PATERN = 'jeedom.%Y.%m'

ES = elasticsearch.Elasticsearch(['http://192.168.10.61:9200','http://192.168.10.62:9200'])

def downsampler_numeric(start_date, end_date):
    query = {
            "bool": {
            "must": [
                {"range": {
                "timestamp": {
                    "gte": start_date,
                    "lte": end_date,
                }
                }},
                {"exists": {"field": "value_numeric"}}
            ]
            }
        }
    aggs = {
        "terms": {
        "multi_terms": {
            "terms": [{
            "field": "objet"
            }, {
            "field": "equipement"
            }, {
            "field": "commande"
            }],
            "size": "5000",
            "order" : { "_count": "asc" }
        },
        "aggs": {
            "timestamp": {
            "date_histogram": {
                "field": "timestamp",
                "fixed_interval": "1h"
            },
                "aggs": {
                "value_stats" : { "extended_stats": { "field": "value_numeric" } }
                }            
            }
        }
        }
    }
    index_data = []
    result = ES.search(query=query, index=start_date.strftime(INDEX_DETAIL_PATERN), size=0 , aggs=aggs)
    for term_bucket in result['aggregations']['terms']['buckets']:
        objet, equipement, commande = term_bucket['key'][0], term_bucket['key'][1], term_bucket['key'][2]
        for date_bucket in term_bucket['timestamp']['buckets']:
            timestamp = datetime.fromtimestamp(date_bucket['key']/1000)
            id_calc = md5()
            for aval in [objet, equipement, commande, timestamp]:
                if isinstance(aval, str):
                    id_calc.update(aval.encode('latin-1'))
                elif isinstance(aval, datetime):
                    id_calc.update(aval.strftime('%Y-%m-%d %H:%M:%S').encode('utf-8'))
                else:
                    id_calc.update(str(aval).encode('utf-8'))
            index_data.append({
                '_index': timestamp.strftime(INDEX_AGG_PATERN),
                '_id': id_calc.digest().hex(),
                '_source': {
                    'timestamp' : timestamp,
                    'objet' : objet,
                    'equipement' : equipement,
                    'commande' : commande,
                    'value_stats' : date_bucket['value_stats']
                }
            })
    bulk(ES, index_data)

def downsampler_text(start_date, end_date):
    query = {
            "bool": {
            "must": [
                {"range": {
                "timestamp": {
                    "gte": start_date,
                    "lte": end_date,
                }
                }},
                {"exists": {"field": "value_text"}}
            ]
            }
        }
    aggs = {
        "terms": {
        "multi_terms": {
            "terms": [{
            "field": "objet"
            }, {
            "field": "equipement"
            }, {
            "field": "commande"
            }, {
            "field": "value_text"
            }],
            "size": "5000",
            "order" : { "_count": "asc" }
        },
        "aggs": {
            "timestamp": {
            "date_histogram": {
                "field": "timestamp",
                "fixed_interval": "1h"
                }
            }
        }
      }
    }
    index_data = []
    result = ES.search(query=query, index=start_date.strftime(INDEX_DETAIL_PATERN), size=0 , aggs=aggs)
    for term_bucket in result['aggregations']['terms']['buckets']:
        objet, equipement, commande, value_text = term_bucket['key'][0], term_bucket['key'][1], term_bucket['key'][2], term_bucket['key'][3]
        for date_bucket in term_bucket['timestamp']['buckets']:
            timestamp = datetime.fromtimestamp(date_bucket['key']/1000)
            id_calc = md5()
            for aval in [objet, equipement, commande, timestamp]:
                if isinstance(aval, str):
                    id_calc.update(aval.encode('latin-1'))
                elif isinstance(aval, datetime):
                    id_calc.update(aval.strftime('%Y-%m-%d %H:%M:%S').encode('utf-8'))
                else:
                    id_calc.update(str(aval).encode('utf-8'))
            index_data.append({
                '_index': timestamp.strftime(INDEX_AGG_PATERN),
                '_id': id_calc.digest().hex(),
                '_source': {
                    'timestamp' : timestamp,
                    'objet' : objet,
                    'equipement' : equipement,
                    'commande' : commande,
                    'value_text': value_text,
                    'count' : date_bucket['doc_count']
                }
            })
    bulk(ES, index_data)
            
if __name__ == '__main__':
    my_date = datetime(2020,1,1)
    while my_date < datetime(2021,1,1):
        try:
            downsampler_numeric(my_date,  my_date + timedelta(days=1))
        except elasticsearch.helpers.BulkIndexError:
            pass
        try:
            downsampler_text(my_date,  my_date + timedelta(days=1))
        except elasticsearch.helpers.BulkIndexError:
            pass
        my_date += timedelta(days=1)