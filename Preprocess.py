import re
import jieba

class DataPreprocess:
    def __init__(self):
        self.pattern = re.compile(r'<[^>]+>',re.S)
        with open('stopwords-zh.txt',encoding='utf8') as f:
            self.stopwords = set(f.read().splitlines())

    def preprocess(self, text):
        data = self.pattern.sub(' ', text)
        res = []
        for w in jieba.cut(data):
            if w not in self.stopwords:
                res.append(w)
        return res

    def deltag(self,text):
        return self.pattern.sub(' ', text)