import elasticsearch
import logging
import re
import time

logger = logging.getLogger('Reindexer')
logging.getLogger('urllib3').setLevel(logging.CRITICAL)
logging.getLogger('elasticsearch').setLevel(logging.CRITICAL)

index_list = [
'jeedom.bk.2020.03',
'jeedom.bk.2019.04',
'jeedom.bk.2019.05',
'jeedom.bk.2020.01',
'jeedom.bk.2019.06',
'jeedom.bk.2020.02',
'jeedom.bk.2019.10',
'jeedom.bk.2019.11',
'jeedom.bk.2019.12',
'jeedom.bk.2020.07',
'jeedom.2020.06',
'jeedom.2020.05',
'jeedom.2020.04',
'jeedom.bk.2019.07',
'jeedom.bk.2019.08',
'jeedom.bk.2019.09',
'jeedom.2020.08',
]
index_name_pattern = re.compile(r'^(.+)\.([\d]{4}(\.[\d]{2})?(\.[\d]{2})?)$')
suffix = 'opt'


ES = elasticsearch.Elasticsearch(['192.168.10.61','192.168.10.62'])

def wait_for_reindex(index, new_index, nb_doc_awaited):
    loop_count = last_doc_count = loop_doc_count  = 0    
    try:
        task_info = ES.tasks.list(actions="*reindex", detailed=True)
        task_id = list(list(task_info['nodes'].values())[0]['tasks'].keys())[0]
    except:
        logger.error('No task found')
    while True:
        try:      
            if task_id is not None:
                task_info = ES.tasks.get(task_id)
                if not task_info['completed']:
                    print("Nombre de documents indexés : %s/%s" % (task_info['task']['status']['created'], task_info['task']['status']['total']))
                    time.sleep(10)
                    continue
            nb_doc = ES.count(index=new_index)['count']
            if nb_doc >= nb_doc_awaited:
                return True
            if last_doc_count == nb_doc:
                loop_doc_count += 1
                if loop_doc_count > 60:
                    return False
                elif loop_doc_count == 30:
                    try:
                        ES.reindex({
                                "source": {
                                    "index": index
                                },
                                "dest": {
                                    "index": new_index_name
                                }            
                            })
                    except elasticsearch.exceptions.ConnectionTimeout:
                        pass
            else:
                loop_doc_count = 0
                last_doc_count = nb_doc
            print("Nombre de documents indexés : %s/%s" % (nb_doc, nb_doc_awaited))
            time.sleep(10)
        except:
            pass


logging.basicConfig(level=logging.INFO)
for index in index_list:
    if not ES.indices.exists(index):
        logger.warning('Index %s does not exist'% (index,))
        continue
    macthing_re = index_name_pattern.match(index)
    if not macthing_re:
        logger.warning('Index %s does not match pattern'% (index,))
        continue     
    if macthing_re.groups()[0] == "jeedom.bk":
        new_index_name = "jeedom." + macthing_re.groups()[1]
        alias_creation = False
    else:
        new_index_name = macthing_re.groups()[0] + "." + suffix + "." + macthing_re.groups()[1]
        alias_creation = True    
    ES.indices.delete_alias(index, new_index_name)    
    if ES.indices.exists(new_index_name):
        try:
            ES.indices.delete_alias(index, new_index_name)
        except:
            logger.warning('Index %s already exists'% (new_index_name,))
            continue    
    nb_doc = ES.count(index=index)['count']
    logger.info('Reindex %s to %s (%s docs)' % (index, new_index_name, nb_doc))
    try:
        reindex_result = ES.reindex({
            "source": {
                "index": index
            },
            "dest": {
                "index": new_index_name
            }            
        })
        print(reindex_result)
        result = True
    except elasticsearch.exceptions.ConnectionTimeout:
        result = wait_for_reindex(index, new_index_name, nb_doc)
    if result:
        logger.info('Reindexation de %s réussie avec %s documents' % (index, nb_doc))
        try:
            ES.indices.delete(index)
        except elasticsearch.exceptions.ConnectionTimeout:
            for i in range(100):
                if not ES.indices.exists(index):
                    break
                elif i > 30:
                    logger.error('La suppression de l\'index %s a échoué' % (index))
                    raise elasticsearch.exceptions.ConnectionTimeout()
                time.sleep(10)
        if ES.indices.exists(new_index_name) and alias_creation:
            try:
                ES.indices.put_alias(new_index_name, index)
            except elasticsearch.exceptions.ConnectionTimeout:
                for i in range(100):
                    if not ES.indices.exists(index):
                        break
                    elif i > 30:
                        logger.error('La création de l\'alias %s a échoué' % (index))
                        raise elasticsearch.exceptions.ConnectionTimeout()
                    time.sleep(10)
            logger.info('Création de l\'alias de %s vers %s réussie' % (new_index_name, index))