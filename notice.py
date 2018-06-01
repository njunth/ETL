from elasticsearch import Elasticsearch
from redis import StrictRedis
from redis.exceptions import ConnectionError
import pymysql
import time
import datetime
import pytz
import threading
import os
import json

ES_HOST = os.getenv('ES_HOST', '114.212.189.147')
ES_PORT = int(os.getenv('ES_PORT', 10142))
REDIS_HOST = os.getenv('REDIS_HOST', '114.212.189.147')
REDIS_PORT = int(os.getenv('REDIS_PORT', 10104))
MYSQL_HOST = os.getenv('MYSQL_HOST', '114.212.189.147')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 10136))
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWD = os.getenv('MYSQL_PASSWD', 'crawl_nju903')
MYSQL_DB = os.getenv('MYSQL_DB', 'woodpecker')

tz = pytz.timezone('Asia/Shanghai')
SITE_TYPES =['微博','门户网站','论坛','培训机构']
SITE_TYPES_EN = ['weibo','portal','forum','agency']
SITE_TYPE_DICT = {}
mysqldb = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, passwd=MYSQL_PASSWD, db=MYSQL_DB, charset='utf8')
sqlcur = mysqldb.cursor()
for i in range(4):
    #print(i)
    sql = "SELECT tableName FROM site_t WHERE type = '"+SITE_TYPES[i]+"'"
    sqlcur.execute(sql)
    sitenames = sqlcur.fetchall()
    for site in sitenames:
        SITE_TYPE_DICT[site[0]] = SITE_TYPES_EN[i]
sqlcur.close()
mysqldb.close()
#print(SITE_TYPE_DICT)

def etl_process(keyword):#,es_host,es_port,redis_host,redis_port):
    es = Elasticsearch([{'host': ES_HOST, 'port': ES_PORT}])
    r = StrictRedis(host=REDIS_HOST, port=REDIS_PORT)
    #print(keyword[0])
    d = datetime.datetime.now(tz)
    delta = datetime.timedelta(seconds=20)#,days=5)
    d=d-delta
    timestr = d.strftime('%Y_%m_%d_%H_%M_%S')
    timedouble = float(d.strftime('%Y%m%d%H%M%S'))
    for i in range(5):
        try:
            r.zremrangebyscore(keyword+'_cache',0,timedouble)
            print(keyword + '_cache', r.zcard(keyword + '_cache'))
            break
        except (ConnectionError,OSError) as e:
            print(i,e)
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
    result = es.search(index='crawler', body=body, request_timeout=30)
    print(keyword,result['took'],result['hits']['total'])
    #dataprocess = dataproana.DataPreprocess()
    #dataanalysis = dataproana.DataAnalysis()
    #print(keyword,'s',time.time())
    #p = r.pipeline()
    for item in result['hits']['hits']:
        # print(item)
        #msg = item['_source']
        #content = dataprocess.delhtmltag(msg['content'])
        #sentiment = dataanalysis.sentiment(content)
        #msg['sentiment'] = sentiment
        timescore = float(time.strftime('%Y%m%d%H%M%S',time.strptime(item['_source']['create_time'],'%Y_%m_%d_%H_%M_%S')))
        jsonobj = {'content':item['_source'],'type':SITE_TYPE_DICT[item['_type']]}
        jsonobj['content']['_id'] = item['_id']
        jsonstr = json.dumps(jsonobj)
        print('zadd',jsonobj['content']['_id'],jsonobj['content']['time'],jsonobj['content']['create_time'],jsonobj['type'])
        for i in range(5):
            try:
                r.zadd(keyword + '_cache', timescore, jsonstr)
                break
            except (ConnectionError,OSError) as e:
                print(i, e)
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
