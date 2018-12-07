import numpy as np
from sklearn.decomposition import TruncatedSVD
# from gensim.models import Word2Vec
# from sklearn.feature_extraction.text import CountVectorizer


def load_embedding(file_name):
    word_id = {'UNK': 0}
    vecs = []
    with open(file_name,'r') as file_in:
        info = file_in.readline().split(' ')
        l = int(info[0])
        dim = int(info[1])
        # print(l,dim)

        vecs.append(np.zeros(dim,dtype=float))
        for row in range(l):
            r = file_in.readline().split(' ')
            word = r[0]
            vec = list(map(float,r[1:dim+1]))
            if len(vec) != dim:
                print(row+1,word)
            word_id[word] = len(word_id)
            vecs.append(np.asarray(vec,dtype=float))
    vecs = np.asarray(vecs,dtype=float)
    # print(len(word_id),np.shape(vecs))
    return word_id, vecs


def load_weight(file_name, a=1e-3):
    if a < 0:
        a = 1.0
    word_weight = {}
    count = 0
    with open(file_name, 'r') as file_in:
        freq = file_in.readlines()
    for row in freq:
        r = row.split(' ')
        word_weight[r[0]] = float(r[1])
        count += float(r[1])
    for key in word_weight:
        word_weight[key] = a / (a + word_weight[key] / count)
    return word_weight


def get_weighted_average(We, x, w):
    """
    Compute the weighted average vectors
    :param We: We[i,:] is the vector for word i
    :param x: x[i, :] are the indices of the words in sentence i
    :param w: w[i, :] are the weights for the words in sentence i
    :return: emb[i, :] are the weighted average vector for sentence i
    """
    n_samples = x.shape[0]
    emb = np.zeros((n_samples, We.shape[1]))
    for i in range(n_samples):
        emb[i, :] = w[i, :].dot(We[x[i, :], :]) / np.count_nonzero(w[i, :])
    return emb


def compute_pc(X,npc=1):
    """
    Compute the principal components. DO NOT MAKE THE DATA ZERO MEAN!
    :param X: X[i,:] is a data point
    :param npc: number of principal components to remove
    :return: component_[i,:] is the i-th pc
    """
    svd = TruncatedSVD(n_components=npc, n_iter=7, random_state=0)
    svd.fit(X)
    return svd.components_


def remove_pc(X, npc=1):
    """
    Remove the projection on the principal components
    :param X: X[i,:] is a data point
    :param npc: number of principal components to remove
    :return: XX[i, :] is the data point after removing its projection
    """
    pc = compute_pc(X, npc)
    if npc == 1:
        XX = X - X.dot(pc.transpose()) * pc
    else:
        XX = X - X.dot(pc.transpose()).dot(pc)
    return XX,pc


def sif_embedding(We, x, w, rmpc):
    """
    Compute the scores between pairs of sentences using weighted average + removing the projection on the first principal component
    :param We: We[i,:] is the vector for word i
    :param x: x[i, :] are the indices of the words in the i-th sentence
    :param w: w[i, :] are the weights for the words in the i-th sentence
    :param rmpc: if >0, remove the projections of the sentence embeddings to their first principal component
    :return: emb, emb[i, :] is the embedding for sentence i
    """
    emb = get_weighted_average(We, x, w)
    if  rmpc > 0:
        emb,pc  = remove_pc(emb, rmpc)
    return emb,pc


def new_sif_embedding(We, x, w, rmpc, pc):
    emb = get_weighted_average(We, x, w)
    if rmpc > 0:
        emb = emb - emb.dot(pc.transpose()) * pc
    return emb


def get_seq(p1, words):
    p1 = p1.split()
    X1 = []
    for i in p1:
        X1.append(lookup_idx(words, i))
    return X1


def lookup_idx(words, w):
    if len(w) > 1 and w[0] == '#':
        w = w.replace("#", "")
    if w in words:
        return words[w]
    elif 'UUUNKKK' in words:
        return words['UUUNKKK']
    else:
        return len(words) - 1


def prepare_data(list_of_seqs):
    lengths = [len(s) for s in list_of_seqs]
    n_samples = len(list_of_seqs)
    maxlen = np.max(lengths)
    x = np.zeros((n_samples, maxlen)).astype('int32')
    x_mask = np.zeros((n_samples, maxlen)).astype('float32')
    for idx, s in enumerate(list_of_seqs):
        x[idx, :lengths[idx]] = s
        x_mask[idx, :lengths[idx]] = 1.
    x_mask = np.asarray(x_mask, dtype='float32')
    return x, x_mask


def sentences2idx(sentences, words):
    """
    Given a list of sentences, output array of word indices that can be fed into the algorithms.
    :param sentences: a list of sentences
    :param words: a dictionary, words['str'] is the indices of the word 'str'
    :return: x1, m1. x1[i, :] is the word indices in sentence i, m1[i,:] is the mask for sentence i (0 means no word at the location)
    """
    seq1 = []
    for i in sentences:
        seq1.append(get_seq(i, words))
    x1, m1 = prepare_data(seq1)
    return x1, m1


def get_weight(words, word2weight):
    weight4ind = {}
    for word, ind in words.items():
        if word in word2weight:
            weight4ind[ind] = word2weight[word]
        else:
            weight4ind[ind] = 1.0
    return weight4ind


def seq2weight(seq, mask, weight4ind):
    weight = np.zeros(seq.shape).astype('float32')
    for i in range(seq.shape[0]):
        for j in range(seq.shape[1]):
            if mask[i, j] > 0 and seq[i, j] >= 0:
                weight[i, j] = weight4ind[seq[i, j]]
    weight = np.asarray(weight, dtype='float32')
    return weight


class SIF:
    # def __init__(self, sentences, min_count=0, size=100, rmpc=1, a=1e-3):
    #     self.w2idx = {}
    #     self.min_count = min_count
    #     self.size = size
    #     self.rmpc = rmpc
    #     self.a = a
    #     count_vectorizer = CountVectorizer()
    #     X = count_vectorizer.fit_transform(sentences)
    #     self.count = np.sum(X)
    #     self.words = {word: i for i, word in enumerate(count_vectorizer.get_feature_names())}
    #     # self.word_weight = np.array(np.sum(X, axis=0)/self.count).squeeze()
    #     self.word_weight = np.array(self.a / (self.a + np.sum(X, axis=0) / self.count)).squeeze()
    #     corpus = [r.split() for r in sentences]
    #     # print(corpus)
    #     self.wvmodel = Word2Vec(corpus, min_count=self.min_count, size=self.size)
    #     self.wv = np.array([self.wvmodel.wv[k] if k in self.wvmodel.wv else np.zeros(size) for k in self.words])
    def __init__(self, weight_file, embedding_file, rmpc=1, a=1e-3):
        self.rmpc = rmpc
        self.a = a
        self.words, self.wv = load_embedding(embedding_file)
        word_wei = load_weight(weight_file, a)
        self.word_weight = get_weight(self.words, word_wei)

    def get_sif_embedding(self, sentences):
        x, m = sentences2idx(sentences, self.words)
        w = seq2weight(x, m, self.word_weight)
        embed, pc = sif_embedding(self.wv, x, w, self.rmpc)
        self.pc =pc
        return embed

    def get_new_sif_embedding(self, sentences):
        x, m = sentences2idx(sentences, self.words)
        w = seq2weight(x, m, self.word_weight)
        embed = new_sif_embedding(self.wv, x, w, self.rmpc, self.pc)
        return embed

#
# s = ['this is the first document',
#      'this document is the second document',
#      'and this is the third one',
#      'is this the first document',
# ]
#
# a = SIF(s)
# print(a.word_weight)
# embed = a.get_sif_embedding(s)
# print(embed)
