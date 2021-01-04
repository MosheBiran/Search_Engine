# you can change whatever you want in this module, just make sure it doesn't 
# break the searcher module
import math


class Ranker:
    def __init__(self, posting_dic, docs_dic):
        self.posting_dic = posting_dic
        self.docs_dic = docs_dic



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

        for value in relevant_doc.values():
            for v in value.keys():
                if v not in tweet_id_data:
                    tweet_id_data[v] = 0
                    tweet_id_CosSim[v] = [0, 0, 0]

        """-------------------------------------Read All Tweets info-----------------------------------------"""
        # TODO - Maybe Threads
        # TODO - Maybe don't need tweet_id_data

        for key in tweet_id_data.keys():
            tweet_id_data[key] = self.docs_dic[key]

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

        res = sorted(tweet_id_CosSim.items(), key=lambda e: e[1][2], reverse=True)

        return res




        # ranked_results = sorted(relevant_docs.items(), key=lambda item: item[1], reverse=True)
        # if k is not None:
        #     ranked_results = ranked_results[:k]
        # return [d[0] for d in ranked_results]

