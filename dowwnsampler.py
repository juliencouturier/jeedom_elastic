import elasticsearch
from elasticsearch.helpers import bulk
import logging
from datetime import datetime, timedelta
from hashlib import md5
import argparse

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
    result = ES.search(query=query, index=start_date.strftime(INDEX_DETAIL_PATERN), size=0 , aggs=aggs, request_timeout=60)
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
            yield {
                '_index': timestamp.strftime(INDEX_AGG_PATERN),
                '_id': id_calc.digest().hex(),
                '_source': {
                    'timestamp' : timestamp,
                    'objet' : objet,
                    'equipement' : equipement,
                    'commande' : commande,
                    'value_stats' : date_bucket['value_stats']
                }
            }

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
            yield {
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
            }
    

def main(args):
    my_date = args.startdate
    while my_date < args.enddate:
        try:
            my_gen = downsampler_numeric(my_date,  my_date + timedelta(days=1))
            bulk(ES, my_gen, max_retries=10, chunk_size=500, request_timeout=60*3)
        except elasticsearch.helpers.BulkIndexError:
            logger.exception('Erreur à l\'indexation numeric')
        try:
            my_gen = downsampler_text(my_date,  my_date + timedelta(days=1))
            bulk(ES, my_gen, max_retries=10, chunk_size=500, request_timeout=60*3)
        except elasticsearch.helpers.BulkIndexError:
            logger.exception('Erreur à l\'indexation text')
        my_date += timedelta(days=1)

        toto = datetime.now()
        toto.replace()

if __name__ == '__main__':
    parser = argparse.ArgumentParser('Downsampler', description="Permet de réindexer les données jeedom après les avoir aggrégées")
    parser.add_argument('-s', "--startdate",
        help="The Start Date - format YYYY-MM-DD",
        required=True,
        type=datetime.fromisoformat,
        default=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1))
    parser.add_argument('-e', "--enddate",
        help="The End Date format YYYY-MM-DD (Exclusive)",
        required=True,
        type=datetime.fromisoformat,
        default=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0))
    args = parser.parse_args()
    main(args)
