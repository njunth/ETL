from elasticsearch import Elasticsearch
# from elasticsearch import helpers
import time
import codecs
import jieba
from sklearn.ensemble import RandomForestClassifier
from sklearn.externals import joblib
import numpy as np
import re
import os

ES_HOST = os.getenv('ES_HOST', '114.212.189.147')
ES_PORT = int(os.getenv('ES_PORT', 10142))

def load_standard_data(filename):
    f = open(filename, "r",encoding='utf-8')
    lines = f.readlines()
    titles = []
    labels = []
    for i in range(0, len(lines), 2):
        titles.append(lines[i].strip())
        labels.append(int(lines[i+1]))
    return titles, labels

def save_voc(titles):
    vocabulary_ind = {}
    index = 0
    for l in titles:
        seg_list = jieba.cut(l.strip(), cut_all=True)
        # seg_list = ''.join(seg_list)
        seg_list = '/'.join(seg_list)
        seg_list = seg_list.split('/')
        # print seg_list
        for s in seg_list:
            # print s
            if len(s) > 0 and s not in vocabulary_ind:
                vocabulary_ind[s] = index
                index += 1
    # print len(vocabulary_ind), vocabulary_ind
    file = codecs.open('voc.txt', 'w', 'utf-8')
    vocabulary = ""
    for w in vocabulary_ind:
        vocabulary += w
        vocabulary += '\n'
        vocabulary += str(vocabulary_ind[w])
        vocabulary += '\n'
    file.write(vocabulary)
    file.close()
    return vocabulary_ind

def load_voc():
    vocabulary_ind = {}
    f = codecs.open('voc.txt', 'r', 'utf-8')
    voc_lines = f.readlines()
    for i in range(0, len(voc_lines), 2):
        vocabulary_ind[voc_lines[i].strip()] = int(voc_lines[i + 1].strip())
    # print len(vocabulary_ind), vocabulary_ind
    return vocabulary_ind

def precision(predict, gold):
    acc = 0.0
    for i, l in enumerate(gold):
        if l == predict[i]:
            acc += 1
    return acc / len(gold)


class DataPreprocess:
    def __init__(self):
        self.pattern = re.compile(r'<[^>]+>',re.S)

    # deleting html tag from text
    def delhtmltag(self, text):
        return self.pattern.sub(' ', text)

# class DataAnalysis:
#     def __init__(self):
#         self.c = sentiment.classifier
#
#     def sentiment(self, text):
#         if len(text) == 0: return 2
#         sen_score = self.snlp_sc.classify(text)
#         if sen_score < 0.2: return 1
#         elif sen_score > 0.8: return 3
#         else:  return 2

class DataAnalysis:
    def __init__(self):
        self.voc = load_voc()
        try:
            self.clf = joblib.load('Random_forest_train_model')
        except:
            self.train()

    def train(self):
        titles, labels = load_standard_data("trainset.txt")
        # print len(titles), len(labels), sum(labels)
        train_x, train_y = titles[len(titles) // 10:], labels[len(titles) // 10:]
        test_x, test_y = titles[:len(titles) // 10], labels[:len(titles) // 10]
        # voc = save_voc(train_x)
        voc = save_voc(titles)
        voc = load_voc()
        train_x, train_y = self.data_to_vec(train_x, train_y, voc)
        test_x, test_y = self.data_to_vec(test_x, test_y, voc)

        self.clf = RandomForestClassifier(oob_score=True, random_state=10, max_features='auto', n_estimators=150)
        self.clf.fit(train_x, train_y)
        # result = clf.predict(test_x)
        # print sum(result), len(result)
        # print("Random forest:", precision(result, test_y))
        joblib.dump(self.clf, 'Random_forest_train_model')

    def data_to_vec(self,titles, labels, vocabulary_ind):
        t = []
        for l in titles:
            seg_list = jieba.cut(l.strip(), cut_all=True)
            # seg_list = ''.join(seg_list)
            seg_list = '/'.join(seg_list)
            seg_list = seg_list.split('/')
            temp = [0 for i in range(len(vocabulary_ind))]
            for s in seg_list:
                if len(s) > 0 and s in vocabulary_ind:
                    temp[vocabulary_ind[s]] += 1
            t.append(temp)
        t = np.array(t)
        l = np.array(labels)
        # print t.shape, l.shape
        return t, l

    def text_to_vec(self,titles, vocabulary_ind):
        t = []
        for l in titles:
            seg_list = jieba.cut(l.strip(), cut_all=True)
            # seg_list = ''.join(seg_list)
            seg_list = '/'.join(seg_list)
            seg_list = seg_list.split('/')
            temp = [0 for i in range(len(vocabulary_ind))]
            for s in seg_list:
                if len(s) > 0 and s in vocabulary_ind:
                    temp[vocabulary_ind[s]] += 1
            t.append(temp)
        t = np.array(t)
        # print t.shape, l.shape
        return t

    def predict(self,vecs):
        vecs = np.array(vecs)
        return self.predict(vecs)


danalysis = DataAnalysis()
dpreprocess = DataPreprocess()
while True:
    try:
        es = Elasticsearch([{'host': ES_HOST, 'port': ES_PORT}])
        res = es.search(index='crawler', body={
            'query': {
                'bool': {
                    'filter': {
                        'term': {
                            'sentiment': 0
                        }
                    }
                }
            },
            'sort': {
                'create_time.keyword': {
                    'order' : 'desc'
                }
            },
            'size': 1000
        },request_timeout=300)
        num = res['hits']['total']
        print(num, flush=True)
        print(time.time(), flush=True)
        if num>0:
            print('create_time:',res['hits']['hits'][0]['_source']['create_time'], flush=True)
        ids = []
        vecs = []
        types = []
        for row in res['hits']['hits']:
            ids.append(row['_id'])
            types.append(row['_type'])
            text = dpreprocess.delhtmltag(row['_source']['content'])
            seg_list = jieba.cut(text.strip(), cut_all=True)
            # seg_list = ''.join(seg_list)
            seg_list = '/'.join(seg_list)
            seg_list = seg_list.split('/')
            temp = [0 for i in range(len(danalysis.voc))]
            for s in seg_list:
                if len(s) > 0 and s in danalysis.voc:
                    temp[danalysis.voc[s]] += 1
            vecs.append(temp)
        result = danalysis.predict(vecs)
        for i in range(len(ids)):
            if result[i] == 0:
                es.update(index='crawler', doc_type=types[i], id=ids[i], body={
                    'doc': {
                        'sentiment': 3
                    }
                })
            else:
                es.update(index='crawler', doc_type=types[i], id=ids[i], body={
                    'doc': {
                        'sentiment': 1
                    }
                })
    except Exception as e:
        print(e)
    # i = 0
    # while i < num:
    #     print(i, time.time(), flush=True)
    #     scanbody = {
    #         'query': {
    #             'bool': {
    #                 'filter': {
    #                     'term': {
    #                         'sentiment': 0
    #                     }
    #                 }
    #             }
    #         },
    #         'size': 1000,
    #         'from': i
    #     }
    #     result = helpers.scan(client=es, query=scanbody, index='crawler', request_timeout=30, scroll='10h')
    #     for item in result:
    #         print(time.time())
    #         content = item['_source']['content']
    #         content = dpreprocess.delhtmltag(content)
    #         sentiment = danalysis.sentiment(content)
    #         # print(item['_id'],item['_type'],sentiment,content)
    #         es.update(index='crawler', doc_type=item['_type'], id=item['_id'], body={
    #             'doc': {
    #                 'sentiment': sentiment
    #             }
    #         })
    #     i += 1000