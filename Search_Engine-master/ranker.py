# you can change whatever you want in this module, just make sure it doesn't 
# break the searcher module
import copy
import math

import numpy as np
from numpy.dual import norm


class Ranker:
    def __init__(self, posting_dic, docs_dic, tweet_dic=None):
        self.posting_dic = posting_dic
        self.docs_dic = docs_dic
        self.tweet_dic = tweet_dic



    def rank_relevant_docs(self, relevant_docs):
        """
        This function provides rank for each relevant document and sorts them by their scores.
        The current score considers solely the number of terms shared by the tweet (full_text) and query.
        :param k: number of most relevant docs to return, default to everything.
        :param relevant_docs: dictionary of documents that contains at least one term from the query.
        :return: sorted list of documents by score
        """
        """--------------------------------------init-structures-----------------------------------------"""

        counter_of_terms = relevant_docs[1]  # key = term ___ value = number of times this term was in the query
        relevant_doc = relevant_docs[0]  # the posting dic of all terms of the query

        """--------------------------------------Getting ALL Relevant Tweet ID-----------------------------------------"""
        tweet_id_data = {}
        tweet_id_CosSim = {}

        tweet_id_data.clear()
        tweet_id_CosSim.clear()

        for value in relevant_doc.values():
            for v in value.keys():
                if v not in tweet_id_data:
                    tweet_id_data[v] = self.docs_dic[v]
                    tweet_id_CosSim[v] = [0, 0, 0]


        """--------------------------------------Cos - Sim-----------------------------------------"""

        query_norma = 0
        for value in counter_of_terms.values():
            query_norma += value ** 2

        for key, value in tweet_id_data.items():
            for term in counter_of_terms.keys():
                if term in value[0]:
                    tf_idf = value[0][term]
                    term_f = counter_of_terms[term]
                    tweet_id_CosSim[key][0] += tf_idf * term_f
                    tweet_id_CosSim[key][1] += term_f ** 2


            inner_p = tweet_id_CosSim[key][0]
            norm = math.sqrt(tweet_id_data[key][2] * query_norma)
            cos_sim = round(inner_p / norm, 3)

            tweet_id_CosSim[key][2] = cos_sim

        res = dict(sorted(tweet_id_CosSim.items(), key=lambda e: e[1][2], reverse=True))
        res2 = list(res.keys())

        return res2


    def compute_extend_word(self, relevant_docs):

        """--------------------------------------init-structures-----------------------------------------"""

        counter_of_terms = relevant_docs[1]  # key = term ___ value = number of times this term was in the query
        relevant_doc = relevant_docs[0]  # the posting dic of all terms of the query

        """--------------------------------------Getting ALL Relevant Tweet ID-----------------------------------------"""

        tweet_id_data = {}
        tweet_id_CosSim = {}

        for value in relevant_doc.values():
            for v in value.keys():
                if v not in tweet_id_data:
                    tweet_id_data[v] = self.docs_dic[v]
                    tweet_id_CosSim[v] = [0, 0, 0]


        """--------------------------------------Cos - Sim-----------------------------------------"""

        query_norma = 0
        for value in counter_of_terms.values():
            query_norma += value ** 2

        for key, value in tweet_id_data.items():
            for term in counter_of_terms.keys():
                if term in value[0]:
                    tf_idf = value[0][term]
                    term_f = counter_of_terms[term]
                    tweet_id_CosSim[key][0] += tf_idf * term_f
                    tweet_id_CosSim[key][1] += term_f ** 2



            inner_p = tweet_id_CosSim[key][0]
            norm = math.sqrt(tweet_id_data[key][2] * query_norma)
            cos_sim = round(inner_p / norm, 3)

            tweet_id_CosSim[key][2] = cos_sim


        sorted_cos_sim = dict(sorted(tweet_id_CosSim.items(), key=lambda e: e[1][2], reverse=True)[:200])  # for test

        """--------------------------------------Init Association Matrix-----------------------------------------"""
        association_matrix = {}  # key = term_term ___ value = Cij


        for key, value in tweet_id_data.items():

            if key not in sorted_cos_sim:
                continue
            for term1, val1 in value[0].items():
                for term2, val2 in value[0].items():
                    if (term1 in counter_of_terms.keys() or term2 in counter_of_terms.keys()) or term1 == term2:
                        Cij = (term1, term2)
                        if Cij not in association_matrix:
                            association_matrix[Cij] = val1 * val2
                        else:
                            association_matrix[Cij] += val1 * val2


        """--------------------------------------Normalized Association Matrix-----------------------------------------"""

        association_matrix_sij = {}

        for Sij, value in association_matrix.items():
            if Sij[0] != Sij[1]:
                Cii = (Sij[0], Sij[0])
                Cjj = (Sij[1], Sij[1])
                association_matrix_sij[Sij] = association_matrix[Sij] / (association_matrix[Cii] + association_matrix[Cjj] - association_matrix[Sij])


        association_matrix_sij = dict(sorted(association_matrix_sij.items(), key=lambda e: e[1], reverse=True))

        """--------------------------------------Normalized Association Matrix-----------------------------------------"""

        copy_of_term = copy.deepcopy(counter_of_terms)

        lst_of_word_to_add = []
        for key in association_matrix_sij.keys():
            if association_matrix_sij[key] < 0.49 and len(lst_of_word_to_add) > 1:
                break
            if len(copy_of_term.keys()) == 0:
                break
            if key[0] in copy_of_term.keys():
                if key[1] not in counter_of_terms and key[1] not in lst_of_word_to_add:
                    lst_of_word_to_add.append(key[1])
                del copy_of_term[key[0]]

        return lst_of_word_to_add


    def get_embedding_w2v(self, w2v_model, doc_tokens):
        embeddings = []
        if len(doc_tokens) < 1:
            return np.zeros(300)
        else:
            for tok in doc_tokens:
                if tok in w2v_model.wv.vocab:
                    embeddings.append(w2v_model.wv.word_vec(tok))

            # mean the vectors of individual words to get the vector of the document
            return np.mean(embeddings, axis=0)


    def rank_relevant_docs_w2v(self, w2v_model, query_as_list, relevant_docs):

        # counter_of_terms = relevant_docs[1]  # key = term ___ value = number of times this term was in the query
        relevant_doc = relevant_docs[0]  # the posting dic of all terms of the query

        qvector = self.get_embedding_w2v(w2v_model, query_as_list)
        tweet_id_data = {}
        tweet_id_CosSim = {}

        tweet_id_data.clear()
        tweet_id_CosSim.clear()

        for value in relevant_doc.values():
            for v in value.keys():
                if v not in tweet_id_data:
                    tweet_id_data[v] = self.docs_dic[v]
                    tweet_id_CosSim[v] = [0]


        norm_vec_q = norm(qvector)
        for key, value in tweet_id_data.items():
            for term in query_as_list:
                if '1283449480642064384' == key:
                    x = 1
                vec = self.get_embedding_w2v(w2v_model, self.tweet_dic[key])
                tweet_id_CosSim[key] = np.dot(qvector, vec) / (norm_vec_q * norm(vec))

        res1 = dict(sorted(tweet_id_CosSim.items(), key=lambda e: e[1], reverse=True))  # for test

        res12 = list(res1.keys())
        return res12
