from elasticsearch import Elasticsearch
import pymysql
import os
import json
import LDA
import Similarity
import numpy as np
import jieba.analyse
import datetime
import pytz
#import TFIDF
import w2v

ES_HOST = os.getenv('ES_HOST', '114.212.189.147')
ES_PORT = int(os.getenv('ES_PORT', 10142))
MYSQL_HOST = os.getenv('MYSQL_HOST', '114.212.189.147')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 10136))
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWD = os.getenv('MYSQL_PASSWD', 'crawl_nju903')
MYSQL_DB = os.getenv('MYSQL_DB', 'woodpecker')

num_data = 10000
num_topics = 10
num_cand = 100
num_word = 20
num_res = 10

tz = pytz.timezone('Asia/Shanghai')
d = datetime.datetime.now(tz)
timestr = d.strftime('%Y_%m_%d_%H_%M_%S')
timestr_2 = d.strftime('%Y_%m_%d') + '_00_00_00'
delta = datetime.timedelta(days=10)
ds = d -delta
timestr_s = ds.strftime('%Y_%m_%d_%H_%M_%S')

jieba.analyse.set_stop_words('./stopwords-zh.txt')

mysqldb = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, passwd=MYSQL_PASSWD, db=MYSQL_DB, charset='utf8')
sqlcur = mysqldb.cursor()

tableq = "SELECT DISTINCT t.table_name, n.SCHEMA_NAME FROM information_schema.TABLES t, information_schema.SCHEMATA n WHERE t.table_name = 'recommend_t' AND n.SCHEMA_NAME = 'woodpecker'"
sqlcur.execute(tableq)
if len(sqlcur.fetchall())==0:
    sqlcur.execute("CREATE TABLE recommend_t(uid INT, date CHAR(19), words VARCHAR(200), PRIMARY KEY (uid))")
    mysqldb.commit()
    print('CREATE TABLE recommend_t',flush=True)



user_id = []
#user_id.append(32)
# for i in range(32,42):
#     user_id.append(i)
sql = 'SELECT id FROM user_t'
sqlcur.execute(sql)
res = sqlcur.fetchall()
for row in res:
    user_id.append(row[0])
types = ['collectionAgency_','collectionForum_','collectionWeibo_','collectionPortal_']

sqlcur.execute("SELECT name,count(*) FROM keyword_t GROUP BY name")
res = sqlcur.fetchall()
max_count = 0
max_word = ''
for row in res:
    if row[1]>max_count:
        max_word = row[0]
print('max:',max_word,flush=True)
sqlcur.execute("REPLACE INTO recommend_t VALUES(0,'" + timestr_2 + "','" + max_word + "')")
mysqldb.commit()

for u in user_id:
    rec_words = ''
    sql = "SELECT DISTINCT name FROM keyword_t WHERE userid = "+str(u)
    key_set = set()
    sqlcur.execute(sql)
    keywords = sqlcur.fetchall()
    es_keyword = ''
    for row in keywords:
        key_set.add(row[0])
        es_keyword += row[0]
        es_keyword += ' '
    print(es_keyword)
    body = {
        '_source': 'content',
        'query': {
            'bool': {
                'must': {
                    'match': {
                        'content': es_keyword
                    }
                },
                'filter': {
                    'range': {
                        'time.keyword': {
                            'lte': timestr,
                            'gte': timestr_s
                        }
                    }
                }
            }
        },
        # 'sort': {
        #     'time.keyword': {
        #         'order' : 'desc'
        #     }
        # },
        'size': num_data
    }
    for es_i in range(10):
        try:
            print('trying ',es_i)
            es = Elasticsearch([{'host': ES_HOST, 'port': ES_PORT}])
            data_res = es.search(index='crawler', body=body, request_timeout=600)
            break
        except Exception as e:
            print('try elasticsearch',es_i,e,flush=True)
    data = []
    num_data_final = data_res['hits']['total'] if (data_res['hits']['total']<num_data) else num_data
    print(num_data_final)
    if num_data_final <=0:
        continue
    for row in data_res['hits']['hits']:
        data.append(row['_source']['content'])

    udata = []
    for i in range(4):
        sql = 'SELECT data FROM '+ types[i]+str(u)
        sqlcur.execute(sql)
        res = sqlcur.fetchall()
        count = 0
        for row in res:
            if count >= 100:
                break
            jsonobj = json.loads(row[0])
            udata.append(jsonobj['content'])
            data.append(jsonobj['content'])
            count += 1


    # tfidfModel = TFIDF.TFIDFModel(data)
    # ldaModel = LDA.LDAModel(data, n_topics=num_topics)
    # w2vModel = w2v.w2vModel(data)


    # t_data_vec = tfidfModel.get_exist_vecs(0,num_data_final).toarray()
    # t_user_doc_vec = tfidfModel.get_exist_vecs(num_data_final,-1).toarray()
    # data_vec = []
    # s_data_vec = []
    # for i in range(num_data_final):
    #     data_vec.append(ldaModel.get_doc_vec(data[i]))
    #     s_data_vec.append(w2vModel.get_doc_vec(data[i]))
    #
    # user_doc_vec = []
    # s_user_doc_vec = []
    # for text in udata:
    #     user_doc_vec.append(ldaModel.get_doc_vec(text))
    #     s_user_doc_vec.append(w2vModel.get_doc_vec(text))


    # print('TF-IDF')
    # scores = Similarity.avg_sim(t_user_doc_vec, t_data_vec)
    # scores_index = np.argsort(scores)[::-1]
    # words = {}
    #
    # for i in range(num_cand):
    #     # print(data[scores_index[i]])
    #     keys = jieba.analyse.extract_tags(ldaModel.preprocess.deltag(data[scores_index[i]]), num_word, withWeight=True)
    #     # print(keys)
    #     for key in keys:
    #         if key[0] not in key_set:
    #             if key[0] in words:
    #                 words[key[0]] += scores[scores_index[i]] * key[1]
    #             else:
    #                 words[key[0]] = scores[scores_index[i]] * key[1]
    # words = sorted(words.items(), key=lambda i: i[1], reverse=True)[:10]
    # print('avg_', words)
    #
    # scores = Similarity.max_sim(t_user_doc_vec, t_data_vec)
    # scores_index = np.argsort(scores)[::-1]
    # words = {}
    #
    # for i in range(num_cand):
    #     # print(data[scores_index[i]])
    #     keys = jieba.analyse.extract_tags(ldaModel.preprocess.deltag(data[scores_index[i]]), num_word, withWeight=True)
    #     # print(keys)
    #     for key in keys:
    #         if key[0] not in key_set:
    #             if key[0] in words:
    #                 words[key[0]] += scores[scores_index[i]] * key[1]
    #             else:
    #                 words[key[0]] = scores[scores_index[i]] * key[1]
    # words = sorted(words.items(), key=lambda i: i[1], reverse=True)[:10]
    # print('max_', words)
    #
    # scores = Similarity.time_dec_sim(t_user_doc_vec, t_data_vec)
    # scores_index = np.argsort(scores)[::-1]
    # words = {}
    #
    # for i in range(num_cand):
    #     # print(data[scores_index[i]])
    #     keys = jieba.analyse.extract_tags(ldaModel.preprocess.deltag(data[scores_index[i]]), num_word, withWeight=True)
    #     # print(keys)
    #     for key in keys:
    #         if key[0] not in key_set:
    #             if key[0] in words:
    #                 words[key[0]] += scores[scores_index[i]] * key[1]
    #             else:
    #                 words[key[0]] = scores[scores_index[i]] * key[1]
    # words = sorted(words.items(), key=lambda i: i[1], reverse=True)[:10]
    # print('time_', words)
    #
    # scores = Similarity.attention_sim(t_user_doc_vec, t_data_vec)
    # scores_index = np.argsort(scores)[::-1]
    # words = {}
    #
    # for i in range(num_cand):
    #     # print(data[scores_index[i]])
    #     keys = jieba.analyse.extract_tags(ldaModel.preprocess.deltag(data[scores_index[i]]), num_word, withWeight=True)
    #     # print(keys)
    #     for key in keys:
    #         if key[0] not in key_set:
    #             if key[0] in words:
    #                 words[key[0]] += scores[scores_index[i]] * key[1]
    #             else:
    #                 words[key[0]] = scores[scores_index[i]] * key[1]
    # words = sorted(words.items(), key=lambda i: i[1], reverse=True)[:10]
    # print('dot_att_', words)

    if len(udata) > 0:
        ldaModel = LDA.LDAModel(data, n_topics=num_topics)

        data_vec = []
        for i in range(num_data_final):
            data_vec.append(ldaModel.get_doc_vec(data[i]))

        user_doc_vec = []
        for text in udata:
            user_doc_vec.append(ldaModel.get_doc_vec(text))

        scores = Similarity.avg_sim(user_doc_vec, data_vec)
        scores_index = np.argsort(scores)[::-1]
        words = {}

        for i in range(num_cand):
            # print(data[scores_index[i]])
            keys = jieba.analyse.extract_tags(ldaModel.preprocess.deltag(data[scores_index[i]]), num_word,
                                              withWeight=True)
            # print(keys)
            for key in keys:
                if key[0] not in key_set:
                    if key[0] in words:
                        words[key[0]] += scores[scores_index[i]] * key[1]
                    else:
                        words[key[0]] = scores[scores_index[i]] * key[1]
        words = sorted(words.items(), key=lambda i: i[1], reverse=True)[:10]
        for word in words:
            rec_words += word[0]
            rec_words += ' '
        print('content',len(words),flush=True)
    # print('LDA')
    # scores = Similarity.avg_sim(user_doc_vec, data_vec)
    # scores_index = np.argsort(scores)[::-1]
    # words = {}
    #
    # for i in range(num_cand):
    #     #print(data[scores_index[i]])
    #     keys = jieba.analyse.extract_tags(ldaModel.preprocess.deltag(data[scores_index[i]]), num_word, withWeight=True)
    #     #print(keys)
    #     for key in keys:
    #         if key[0] not in key_set:
    #             if key[0] in words:
    #                 words[key[0]] += scores[scores_index[i]] * key[1]
    #             else:
    #                 words[key[0]] = scores[scores_index[i]] * key[1]
    # words = sorted(words.items(), key=lambda i: i[1], reverse=True)[:10]
    # print('avg_', words)

    # scores = Similarity.max_sim(user_doc_vec, data_vec)
    # scores_index = np.argsort(scores)[::-1]
    # words = {}
    #
    # for i in range(num_cand):
    #     # print(data[scores_index[i]])
    #     keys = jieba.analyse.extract_tags(ldaModel.preprocess.deltag(data[scores_index[i]]), num_word, withWeight=True)
    #     # print(keys)
    #     for key in keys:
    #         if key[0] not in key_set:
    #             if key[0] in words:
    #                 words[key[0]] += scores[scores_index[i]] * key[1]
    #             else:
    #                 words[key[0]] = scores[scores_index[i]] * key[1]
    # words = sorted(words.items(), key=lambda i: i[1], reverse=True)[:10]
    # print('max_', words)
    #
    # scores = Similarity.time_dec_sim(user_doc_vec, data_vec)
    # scores_index = np.argsort(scores)[::-1]
    # words = {}
    #
    # for i in range(num_cand):
    #     # print(data[scores_index[i]])
    #     keys = jieba.analyse.extract_tags(ldaModel.preprocess.deltag(data[scores_index[i]]), num_word, withWeight=True)
    #     # print(keys)
    #     for key in keys:
    #         if key[0] not in key_set:
    #             if key[0] in words:
    #                 words[key[0]] += scores[scores_index[i]] * key[1]
    #             else:
    #                 words[key[0]] = scores[scores_index[i]] * key[1]
    # words = sorted(words.items(), key=lambda i: i[1], reverse=True)[:10]
    # print('time_', words)

    # scores = Similarity.attention_sim(user_doc_vec, data_vec)
    # scores_index = np.argsort(scores)[::-1]
    # words = {}
    #
    # for i in range(num_cand):
    #     # print(data[scores_index[i]])
    #     keys = jieba.analyse.extract_tags(ldaModel.preprocess.deltag(data[scores_index[i]]), num_word, withWeight=True)
    #     # print(keys)
    #     for key in keys:
    #         if key[0] not in key_set:
    #             if key[0] in words:
    #                 words[key[0]] += scores[scores_index[i]] * key[1]
    #             else:
    #                 words[key[0]] = scores[scores_index[i]] * key[1]
    # words = sorted(words.items(), key=lambda i: i[1], reverse=True)[:10]
    # print('dot_att_', words)


    # print('w2v')
    #
    # scores = Similarity.avg_sim(s_user_doc_vec, s_data_vec)
    # scores_index = np.argsort(scores)[::-1]
    # words = {}
    #
    # for i in range(num_cand):
    #     # print(data[scores_index[i]])
    #     keys = jieba.analyse.extract_tags(ldaModel.preprocess.deltag(data[scores_index[i]]), num_word, withWeight=True)
    #     # print(keys)
    #     for key in keys:
    #         if key[0] not in key_set:
    #             if key[0] in words:
    #                 words[key[0]] += scores[scores_index[i]] * key[1]
    #             else:
    #                 words[key[0]] = scores[scores_index[i]] * key[1]
    # words = sorted(words.items(), key=lambda i: i[1], reverse=True)[:10]
    # print('avg_', words)

    # scores = Similarity.max_sim(s_user_doc_vec, s_data_vec)
    # scores_index = np.argsort(scores)[::-1]
    # words = {}
    #
    # for i in range(num_cand):
    #     # print(data[scores_index[i]])
    #     keys = jieba.analyse.extract_tags(ldaModel.preprocess.deltag(data[scores_index[i]]), num_word, withWeight=True)
    #     # print(keys)
    #     for key in keys:
    #         if key[0] not in key_set:
    #             if key[0] in words:
    #                 words[key[0]] += scores[scores_index[i]] * key[1]
    #             else:
    #                 words[key[0]] = scores[scores_index[i]] * key[1]
    # words = sorted(words.items(), key=lambda i: i[1], reverse=True)[:10]
    # print('max_', words)
    #
    # scores = Similarity.time_dec_sim(s_user_doc_vec, s_data_vec)
    # scores_index = np.argsort(scores)[::-1]
    # words = {}
    #
    # for i in range(num_cand):
    #     # print(data[scores_index[i]])
    #     keys = jieba.analyse.extract_tags(ldaModel.preprocess.deltag(data[scores_index[i]]), num_word, withWeight=True)
    #     # print(keys)
    #     for key in keys:
    #         if key[0] not in key_set:
    #             if key[0] in words:
    #                 words[key[0]] += scores[scores_index[i]] * key[1]
    #             else:
    #                 words[key[0]] = scores[scores_index[i]] * key[1]
    # words = sorted(words.items(), key=lambda i: i[1], reverse=True)[:10]
    # print('time_', words)

    # scores = Similarity.attention_sim(s_user_doc_vec, s_data_vec)
    # scores_index = np.argsort(scores)[::-1]
    # words = {}
    #
    # for i in range(num_cand):
    #     # print(data[scores_index[i]])
    #     keys = jieba.analyse.extract_tags(ldaModel.preprocess.deltag(data[scores_index[i]]), num_word, withWeight=True)
    #     # print(keys)
    #     for key in keys:
    #         if key[0] not in key_set:
    #             if key[0] in words:
    #                 words[key[0]] += scores[scores_index[i]] * key[1]
    #             else:
    #                 words[key[0]] = scores[scores_index[i]] * key[1]
    # words = sorted(words.items(), key=lambda i: i[1], reverse=True)[:10]
    # print('dot_att_', words)

    if len(key_set) > 0:
        w2vModel = w2v.w2vModel(data)
        key_list = []
        for w2vkey in key_set:
            if w2vModel.model.wv.__contains__(w2vkey):
                key_list.append(w2vkey)
            else:
                print('false:',w2vkey,flush=True)
        words = w2vModel.get_similar_words(pos_words=key_list)
        for word in words:
            rec_words += word[0]
            rec_words += ' '
        print('key',len(words),flush=True)
    sqlcur.execute("REPLACE INTO recommend_t VALUES(" + str(u) + ",'" + timestr_2 + "','" + rec_words + "')")
    mysqldb.commit()

sqlcur.close()
mysqldb.close()



