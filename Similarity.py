import numpy as np


def avg_sim(user_docs,doc_vecs):
    user_vec = np.average(user_docs,axis=0)
    return np.dot(doc_vecs,user_vec)/(np.linalg.norm(user_vec)*np.linalg.norm(doc_vecs,axis=1))

def max_sim(user_docs,doc_vecs):
    user_vec = np.max(user_docs,axis=0)
    return np.dot(doc_vecs,user_vec) / (np.linalg.norm(user_vec) * np.linalg.norm(doc_vecs,axis=1))

def time_dec_sim(user_docs,doc_vecs):
    w = np.exp(np.array(range(0,len(user_docs))))
    w = w/np.sum(w)
    user_vec = np.zeros(len(user_docs[0]))
    for i in range(len(user_docs)):
        user_vec += w[i]*np.array(user_docs[i])
    return np.dot(doc_vecs,user_vec) / (np.linalg.norm(user_vec) * np.linalg.norm(doc_vecs, axis=1))

def attention_sim(user_docs,doc_vecs):
    res = []
    for doc_vec in doc_vecs:
        a = np.exp(np.dot(user_docs,doc_vec))/ np.sqrt(len(doc_vec))
        a = a/np.sum(a)
        user_vec = np.zeros(len(user_docs[0]))
        for i in range(len(user_docs)):
            user_vec += a[i] * np.array(user_docs[i])
        res.append(np.dot(user_vec,doc_vec)/(np.linalg.norm(user_vec)*np.linalg.norm(doc_vec)))
    return res

# a = [[0.3,0.4],
#      [0.9,0.12],
#      [1,1]]
# b = [[0.3,0.4],
#      [0.4,0.4],
#      [0.123,0.2],
#      [0.11,0.1],
#      [0,0.123213]]
# print(avg_sim(a,b))
# print(max_sim(a,b))
# print(time_dec_sim(a,b))
# print(attention_sim(a,b))