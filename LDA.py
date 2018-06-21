from gensim import models,corpora
from elasticsearch import Elasticsearch
import os
from Preprocess import DataPreprocess

class LDAModel:
    def __init__(self,text_data,n_topics=100):
        self.n_topics = n_topics
        self.preprocess = DataPreprocess()
        data = []
        for text in text_data:
            data.append(self.preprocess.preprocess(text))
        self.dictionary = corpora.Dictionary(data)
        self.corpus = [self.dictionary.doc2bow(text) for text in data]
        self.model = models.LdaModel(corpus=self.corpus,num_topics=self.n_topics,id2word=self.dictionary)

    def get_doc_vec(self,doc_text):
        doc_bow = self.dictionary.doc2bow(self.preprocess.preprocess(doc_text))
        topics = self.model.get_document_topics(doc_bow,minimum_probability=0)
        vec = [item[1] for item in topics]
        return vec


#
# ES_HOST = os.getenv('ES_HOST', '114.212.189.147')
# ES_PORT = int(os.getenv('ES_PORT', 10056))
# es = Elasticsearch([{'host': ES_HOST, 'port': ES_PORT}])
# res = es.search(index='crawler',body={
#     '_source': 'content',
#     'query': {
#         'match_all':{}
#     },
#     'size': 10,
#     'from':0
# })
#
# data = []
# for row in res['hits']['hits']:
#     data.append(row['_source']['content'])
# ldamodel = LDAModel(data)
# for text in data:
#     print(ldamodel.get_doc_vec(text))
# print('new')
# res = es.search(index='crawler',body={
#     '_source': 'content',
#     'query': {
#         'match_all':{}
#     },
#     'size': 10,
#     'from':10
# })
# for row in res['hits']['hits']:
#     print(ldamodel.get_doc_vec(row['_source']['content']))
