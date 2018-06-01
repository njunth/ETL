from elasticsearch import Elasticsearch
from elasticsearch import helpers
import time
from snownlp import sentiment
import re
import os

ES_HOST = os.getenv('ES_HOST', '114.212.189.147')
ES_PORT = int(os.getenv('ES_PORT', 10142))

class DataPreprocess:
    def __init__(self):
        self.pattern = re.compile(r'<[^>]+>',re.S)

    # deleting html tag from text
    def delhtmltag(self, text):
        return self.pattern.sub(' ', text)

class DataAnalysis:
    def __init__(self):
        self.snlp_sc = sentiment.classifier

    def sentiment(self, text):
        if len(text) == 0: return 2
        sen_score = self.snlp_sc.classify(text)
        if sen_score < 0.2: return 1
        elif sen_score > 0.8: return 3
        else:  return 2

danalysis = DataAnalysis()
dpreprocess = DataPreprocess()
while True:
    es = Elasticsearch([{'host': ES_HOST, 'port': ES_PORT}])
    num = es.search(index='crawler', body={
        'query': {
            'bool': {
                'filter': {
                    'term': {
                        'sentiment': 0
                    }
                }
            }
        },
        'size': 0
    })['hits']['total']
    print(num, flush=True)
    i = 0
    while i < num:
        print(i, time.time(), flush=True)
        scanbody = {
            'query': {
                'bool': {
                    'filter': {
                        'term': {
                            'sentiment': 0
                        }
                    }
                }
            },
            'size': 1000,
            'from': i
        }
        result = helpers.scan(client=es, query=scanbody, index='crawler', request_timeout=30, scroll='10h')
        for item in result:
            content = item['_source']['content']
            content = dpreprocess.delhtmltag(content)
            sentiment = danalysis.sentiment(content)
            # print(item['_id'],item['_type'],sentiment,content)
            es.update(index='crawler', doc_type=item['_type'], id=item['_id'], body={
                'doc': {
                    'sentiment': sentiment
                }
            })
        i += 1000
