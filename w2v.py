from gensim.models import Word2Vec
from Preprocess import DataPreprocess
import numpy as np
import os
from elasticsearch import Elasticsearch


class w2vModel:
    def __init__(self,text_data,min_count=5,size=100):
        self.min_count = min_count
        self.preprocess = DataPreprocess()
        self.size = size
        data = []
        for text in text_data:
            data.append(self.preprocess.preprocess(text))
        self.model = Word2Vec(data,min_count=self.min_count,size=self.size)

    def get_word_vec(self,word):
        if word in self.model.wv:
            #print('y',word,self.model.wv[word])
            return self.model.wv[word]
        else:
            #print('n', word)
            return np.zeros(self.size)

    def get_doc_vec(self,text):
        count = 0
        vec = np.zeros(self.size)
        for word in self.preprocess.preprocess(text):
            vec += self.get_word_vec(word)
            count += 1
        vec = vec / count
        #print(count)
        return vec

    def get_similar_words(self,pos_words,neg_words=None,num=10):
        return self.model.wv.most_similar(positive=pos_words,negative=neg_words,topn=10)



# ES_HOST = os.getenv('ES_HOST', '114.212.189.147')
# ES_PORT = int(os.getenv('ES_PORT', 10056))
# es = Elasticsearch([{'host': ES_HOST, 'port': ES_PORT}])
# res = es.search(index='crawler',body={
#     '_source': 'content',
#     'query': {
#         'match_phrase':{
#             'content':'学习'
#         }
#     },
#     'size': 1000,
#     'from':0
# })
#
# data = []
# for row in res['hits']['hits']:
#     data.append(row['_source']['content'])
# model = w2vModel(data)
# # for text in data:
# #     print(model.get_doc_vec(text))
# print('new')
# res = es.search(index='crawler',body={
#     '_source': 'content',
#     'query': {
#         'match_phrase':{
#             'content':'学习'
#         }
#     },
#     'size': 10,
#     'from':1000
# })
# for row in res['hits']['hits']:
#     print(model.get_doc_vec(row['_source']['content']))
# print(model.model.wv.most_similar(['学习','框架']))





