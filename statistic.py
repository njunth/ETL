import pymysql
from elasticsearch import Elasticsearch
import os
import datetime
import pytz
import json

tz = pytz.timezone('Asia/Shanghai')

ES_HOST = os.getenv('ES_HOST', '114.212.189.147')
ES_PORT = int(os.getenv('ES_PORT', 10142))
MYSQL_HOST = os.getenv('MYSQL_HOST', '114.212.189.147')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 10136))
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWD = os.getenv('MYSQL_PASSWD', 'crawl_nju903')
MYSQL_DB = os.getenv('MYSQL_DB', 'woodpecker')

SITE_TYPES =['微博','门户网站','论坛','培训机构']
SITES ={'微博':[],'门户网站':[],'论坛':[],'培训机构':[]}
TABLE_NAMES = ['statistic_t','sentiment_t','distribution_t','map_t']
TABLE_SQL = ["CREATE TABLE statistic_t(keyword VARCHAR(45), date CHAR(19), source VARCHAR(45), count BIGINT, PRIMARY KEY(keyword, date, source))",
             "CREATE TABLE sentiment_t(keyword VARCHAR(45), date CHAR(19), sentiment INT, count BIGINT, PRIMARY KEY(keyword, date, sentiment))",
             "CREATE TABLE distribution_t(keyword VARCHAR(45), source VARCHAR(45), count BIGINT, PRIMARY KEY(keyword, source))",
             "CREATE TABLE map_t(keyword VARCHAR(45), start_time CHAR(19),end_time CHAR(19), json VARCHAR(1024), PRIMARY KEY(keyword))"]
print('Connecting MySQL')
sqldb = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, passwd=MYSQL_PASSWD, db=MYSQL_DB, charset='utf8')
print('Connected')
cursor = sqldb.cursor()
for i in range(4):
    #print(i)
    tableq = "SELECT DISTINCT t.table_name, n.SCHEMA_NAME FROM information_schema.TABLES t, information_schema.SCHEMATA n WHERE t.table_name = '"+TABLE_NAMES[i]+"' AND n.SCHEMA_NAME = 'woodpecker'"
    cursor.execute(tableq)
    if len(cursor.fetchall())==0:
        cursor.execute(TABLE_SQL[i])
        print(TABLE_SQL[i])
for i in range(4):
    #print(i)
    sql = "SELECT tableName FROM site_t WHERE type = '"+SITE_TYPES[i]+"'"
    cursor.execute(sql)
    sitenames = cursor.fetchall()
    print(sitenames)
    for site in sitenames:
        #print(site[0])
        SITES[SITE_TYPES[i]].append(site[0])
sql = "SELECT tableName, addr FROM site_t"
# sql = "desc site_t"
addr_dict = {}
cursor.execute(sql)
result = cursor.fetchall()
for row in result:
    addr_dict[row[0]] = row[1]

sql = "SELECT DISTINCT name FROM keyword_t"
cursor.execute(sql)
keywords = cursor.fetchall()

for row in keywords:
    keyword = row[0]
    print(keyword)
    for try_i in range(10):
        print('try',try_i)
        try:
            es = Elasticsearch([{'host': ES_HOST, 'port': ES_PORT}])
            for i in range(4):
                d = datetime.datetime.now(tz)
                delta = datetime.timedelta(days=1)
                timestr = d.strftime('%Y_%m_%d_%H_%M_%S')
                # print(timestr)
                timestr2 = d.strftime('%Y_%m_%d') + '_00_00_00'
                for j in range(10):
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
                                        'time.keyword': {
                                            'gte': timestr2,
                                            'lt': timestr
                                        }
                                    }
                                }
                            }
                        },
                        'size': 0
                    }
                    result = es.search(index='crawler', body=body, doc_type=SITES[SITE_TYPES[i]], request_timeout=30)
                    # print(result)
                    insert_sql = "REPLACE INTO statistic_t VALUES('" + keyword + "','" + timestr2 + "','" + SITE_TYPES[
                        i] + "'," + str(result['hits']['total']) + ")"
                    cursor.execute(insert_sql)
                    print(insert_sql)
                    timestr = timestr2
                    d = d - delta
                    timestr2 = d.strftime('%Y_%m_%d') + '_00_00_00'
            for i in range(1, 4):
                d = datetime.datetime.now(tz)
                delta = datetime.timedelta(days=1)
                timestr = d.strftime('%Y_%m_%d_%H_%M_%S')
                # print(timestr)
                timestr2 = d.strftime('%Y_%m_%d') + '_00_00_00'
                for j in range(10):
                    body = {
                        'query': {
                            'bool': {
                                'must': {
                                    'match_phrase': {
                                        'content': keyword
                                    }
                                },
                                'filter': {
                                    'bool': {
                                        'must': [
                                            {'range': {
                                                'time.keyword': {
                                                    'gte': timestr2,
                                                    'lt': timestr
                                                }
                                            }},
                                            {'term': {
                                                'sentiment': i
                                            }}
                                        ]
                                    }
                                }
                            }
                        },
                        'size': 0
                    }
                    result = es.search(index='crawler', body=body, request_timeout=30)
                    # print(result)
                    insert_sql = "REPLACE INTO sentiment_t VALUES('" + keyword + "','" + timestr2 + "'," + \
                                 str(i) + "," + str(result['hits']['total']) + ")"
                    cursor.execute(insert_sql)
                    print(insert_sql)
                    timestr = timestr2
                    d = d - delta
                    timestr2 = d.strftime('%Y_%m_%d') + '_00_00_00'
            for i in range(4):
                body = {
                    'query': {
                        'match_phrase': {
                            'content': keyword
                        }
                    },
                    'size': 0
                }
                result = es.search(index='crawler', body=body, doc_type=SITES[SITE_TYPES[i]], request_timeout=30)
                # print(result)
                insert_sql = "REPLACE INTO distribution_t VALUES('" + keyword + "','" + SITE_TYPES[i] + "'," + str(
                    result['hits']['total']) + ")"
                cursor.execute(insert_sql)
                print(insert_sql)

            map_dict = {}
            d = datetime.datetime.now(tz)
            delta = datetime.timedelta(days=1)
            timestr = d.strftime('%Y_%m_%d_%H_%M_%S')
            d = d - delta
            timestr2 = d.strftime('%Y_%m_%d_%H_%M_%S')
            for key in addr_dict:
                value = addr_dict[key]
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
                                    'time.keyword': {
                                        'gte': timestr2,
                                        'lt': timestr
                                    }
                                }
                            }
                        }
                    },
                    'size': 0
                }
                num = es.search(index='crawler', doc_type=key, body=body)['hits']['total']
                if value in map_dict:
                    map_dict[value] += num
                else:
                    map_dict[value] = num
            json_str = json.dumps(map_dict)
            json_str = json_str.replace('\\','\\\\')
            insert_sql = "REPLACE INTO map_t VALUES('" + keyword + "','" + timestr2 + "','" + timestr + "','"+json_str+"')"
            cursor.execute(insert_sql)
            print(insert_sql)
            break
        except Exception as e:
            print('Exception',try_i,e)
    sqldb.commit()


sqldb.commit()
cursor.close()
sqldb.close()