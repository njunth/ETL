from elasticsearch import Elasticsearch
from redis import StrictRedis
import redis.exceptions
import elasticsearch.exceptions
import urllib3.exceptions
from http.client import RemoteDisconnected
import pymysql
import time
import datetime
import pytz
import threading
import os,re
import json
import jieba
import falconn
import SIF_embedding
import numpy as np

ES_HOST = os.getenv('ES_HOST', '114.212.189.147')
ES_PORT = int(os.getenv('ES_PORT', 10142))
REDIS_HOST = os.getenv('REDIS_HOST', '114.212.189.147')
REDIS_PORT = int(os.getenv('REDIS_PORT', 10104))
MYSQL_HOST = os.getenv('MYSQL_HOST', '114.212.189.147')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 10136))
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWD = os.getenv('MYSQL_PASSWD', 'crawl_nju903')
MYSQL_DB = os.getenv('MYSQL_DB', 'woodpecker')

number_of_tables = 50

tz = pytz.timezone('Asia/Shanghai')
SITE_TYPES =['微博','门户网站','论坛','培训机构','商务资讯','行业动态']
SITE_TYPES_EN = ['weibo','portal','forum','agency','business','industry']
SITE_TYPE_DICT = {}
mysqldb = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, passwd=MYSQL_PASSWD, db=MYSQL_DB, charset='utf8')
sqlcur = mysqldb.cursor()

data = []
sqlcur.execute('SELECT * FROM msgNegative')
res = sqlcur.fetchall()
for r in res:
    row = ' '.join(jieba.cut(re.sub('<[^>]+>', '', r[0].lower().strip())))
    data.append(row)
print('neg msg count:',len(data))
sif_mod = SIF_embedding.SIF('webdict_with_freq.txt', 'sgns.weibo.bigram-char.txt')
data_vec = sif_mod.get_sif_embedding(data)
cen = np.mean(data_vec, axis=0)
data_vec.astype(np.float32)
data_vec -= cen
params_cp = falconn.LSHConstructionParameters()
params_cp.dimension = len(data_vec[0])
params_cp.lsh_family = falconn.LSHFamily.CrossPolytope
params_cp.distance_function = falconn.DistanceFunction.EuclideanSquared
params_cp.l = number_of_tables
params_cp.num_rotations = 1
# params_cp.seed = 5721840
params_cp.num_setup_threads = 0
params_cp.storage_hash_table = falconn.StorageHashTable.BitPackedFlatHashTable
falconn.compute_number_of_hash_functions(18, params_cp)

table = falconn.LSHIndex(params_cp)
table.setup(data_vec)
query_object = table.construct_query_object()
# number_of_probes = number_of_tables
number_of_probes = 6000
query_object.set_num_probes(number_of_probes)
K_NEAR = 5
threshold = 0.48
lock = threading.Lock()

for i in range(6):
    #print(i)
    sql = "SELECT tableName FROM site_t WHERE type = '"+SITE_TYPES[i]+"'"
    sqlcur.execute(sql)
    sitenames = sqlcur.fetchall()
    for site in sitenames:
        SITE_TYPE_DICT[site[0]] = SITE_TYPES_EN[i]
sqlcur.close()
mysqldb.close()
print(SITE_TYPE_DICT)

def etl_process(keyword):#,es_host,es_port,redis_host,redis_port):

    r = StrictRedis(host=REDIS_HOST, port=REDIS_PORT)
    #print(keyword[0])
    d = datetime.datetime.now(tz)
    delta = datetime.timedelta(days=20)#,days=5)
    d=d-delta
    timestr = d.strftime('%Y_%m_%d_%H_%M_%S')
    timedouble = float(d.strftime('%Y%m%d%H%M%S'))
    print(keyword,'time',timestr)
    for i in range(5):
        try:
            r.zremrangebyscore(keyword+'_cache',0,timedouble)
            print(keyword + '_cache', r.zcard(keyword + '_cache'))
            break
        # except (ConnectionError,OSError) as e:
        except Exception as e:
            print(i, 'try redis rem:', e)
    body = {
        'query': {
            'bool': {
                'must': {
                    'match_phrase': {
                        'content': keyword
                    }
                },
                'filter': {
                    'range': {
                        'create_time.keyword': {
                            'gte': timestr
                        }
                    }
                }
            }
        },
        'size': 1000
    }

    for i in range(5):
        try:
            es = Elasticsearch([{'host': ES_HOST, 'port': ES_PORT}])
            result = es.search(index='crawler', body=body, request_timeout=30)
            print(keyword, result['took'], result['hits']['total'])
            for item in result['hits']['hits']:
                # print(item)
                # msg = item['_source']
                # content = dataprocess.delhtmltag(msg['content'])
                # sentiment = dataanalysis.sentiment(content)
                # msg['sentiment'] = sentiment
                timescore = float(
                    time.strftime('%Y%m%d%H%M%S', time.strptime(item['_source']['create_time'], '%Y_%m_%d_%H_%M_%S')))
                jsonobj = {'content': item['_source'], 'type': SITE_TYPE_DICT[item['_type']]}
                jsonobj['content']['_id'] = item['_id']
                jsonstr = json.dumps(jsonobj)
                query_sentences = []
                query_row = ' '.join(jieba.cut(re.sub('<[^>]+>', '', jsonobj['content']['content'].lower().strip())))
                query_sentences.append(query_row)
                # query_vec = sif_mod.get_sif_embedding(query_sentences)[0] - cen
                with lock:
                    query_vec = sif_mod.get_new_sif_embedding(query_sentences)[0] - cen
                    query_res = query_object.find_k_nearest_neighbors(query_vec, K_NEAR)
                res_vec = data_vec[query_res]
                res_score = np.dot(res_vec, query_vec) / np.sqrt((res_vec * res_vec).sum(axis=1)) / np.sqrt(
                    (query_vec * query_vec).sum())
                if np.mean(res_score) > threshold:
                    jsonobj['content']['sentiment'] = 1
                else:
                    jsonobj['content']['sentiment'] = 3
                print('zadd', jsonobj['content']['_id'], jsonobj['content']['time'], jsonobj['content']['create_time'],
                      jsonobj['type'])
                for i in range(5):
                    try:
                        r.zadd(keyword + '_cache', timescore, jsonstr)
                        es.update(index='crawler', doc_type=jsonobj['type'], id=jsonobj['content']['_id'], body={
                            'doc': {
                                'sentiment': jsonobj['content']['sentiment']
                            }
                        })
                        break
                    # except (ConnectionError, redis.exceptions.ConnectionError, OSError) as e:
                    except Exception as e:
                        print(i, 'try redis add:', e)
            break
        # except (urllib3.exceptions.ProtocolError, RemoteDisconnected,ConnectionResetError,ConnectionError,urllib3.exceptions.NewConnectionError,elasticsearch.exceptions.ConnectionError,OSError,Exception as e:
        except Exception as e:
            print(i, 'try elasticsearch:', e)

    #dataprocess = dataproana.DataPreprocess()
    #dataanalysis = dataproana.DataAnalysis()
    #print(keyword,'s',time.time())
    #p = r.pipeline()

        # print(r.zcard(keyword[0]+'_cache'))
    #p.execute()
    #print(keyword,'e',time.time())



while True :
    print('Connecting MySQL')
    sqldb = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, passwd=MYSQL_PASSWD, db=MYSQL_DB, charset='utf8')
    print('Connected')
    cursor = sqldb.cursor()
    sql = "SELECT DISTINCT name FROM keyword_t"
    cursor.execute(sql)
    keywords = cursor.fetchall()
    cursor.close()
    sqldb.close()
    threadList = []
    for keyword in keywords:
        t = threading.Thread(target=etl_process,args=(keyword[0],))
        threadList.append(t)
    for t in threadList:
        t.start()
    for t in threadList:
        t.join()
    print('sleep 10s',flush=True)
    time.sleep(10)