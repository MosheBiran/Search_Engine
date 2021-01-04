# you can change whatever you want in this module, just make sure it doesn't
# break the searcher module
import copy
import math


class RankerLocalMethod:
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
                if term in value[0]:  # TODO - term -lower\upper check
                    tf_idf = value[0][term]
                    term_f = counter_of_terms[term]
                    tweet_id_CosSim[key][0] += tf_idf * term_f
                    tweet_id_CosSim[key][1] += term_f ** 2

                elif term.lower() in value[0]:
                    tf_idf = value[0][term.lower()]
                    term_f = counter_of_terms[term]
                    tweet_id_CosSim[key][0] += tf_idf * term_f
                    tweet_id_CosSim[key][1] += term_f ** 2

                elif term.upper() in value[0]:
                    tf_idf = value[0][term.upper()]
                    term_f = counter_of_terms[term]
                    tweet_id_CosSim[key][0] += tf_idf * term_f
                    tweet_id_CosSim[key][1] += term_f ** 2



            inner_p = tweet_id_CosSim[key][0]
            norm = math.sqrt(tweet_id_data[key][2] * query_norma)
            cos_sim = round(inner_p / norm, 3)
            if cos_sim > 1:
                x = 1
                raise TypeError
            tweet_id_CosSim[key][2] = cos_sim

        # res = sorted(tweet_id_CosSim.items(), key=lambda e: e[1][2], reverse=True)  # original
        res = dict(sorted(tweet_id_CosSim.items(), key=lambda e: e[1][2], reverse=True))  # for test
        res2 = list(res.keys())

        return res2

        # ranked_results = sorted(relevant_docs.items(), key=lambda item: item[1], reverse=True)
        # if k is not None:
        #     ranked_results = ranked_results[:k]
        # return [d[0] for d in ranked_results]


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
                if term in value[0]:  # TODO - term -lower\upper check
                    tf_idf = value[0][term]
                    term_f = counter_of_terms[term]
                    tweet_id_CosSim[key][0] += tf_idf * term_f
                    tweet_id_CosSim[key][1] += term_f ** 2

                elif term.lower() in value[0]:
                    tf_idf = value[0][term.lower()]
                    term_f = counter_of_terms[term]
                    tweet_id_CosSim[key][0] += tf_idf * term_f
                    tweet_id_CosSim[key][1] += term_f ** 2

                elif term.upper() in value[0]:
                    tf_idf = value[0][term.upper()]
                    term_f = counter_of_terms[term]
                    tweet_id_CosSim[key][0] += tf_idf * term_f
                    tweet_id_CosSim[key][1] += term_f ** 2

            inner_p = tweet_id_CosSim[key][0]
            norm = math.sqrt(tweet_id_data[key][2] * query_norma)
            cos_sim = round(inner_p / norm, 3)
            if cos_sim > 1:
                x = 1
                raise TypeError  # TODO - Remove!!!
            tweet_id_CosSim[key][2] = cos_sim

        # res = sorted(tweet_id_CosSim.items(), key=lambda e: e[1][2], reverse=True)  # original
        sorted_cos_sim = dict(sorted(tweet_id_CosSim.items(), key=lambda e: e[1][2], reverse=True)[:50])  # for test

        """--------------------------------------Init Association Matrix-----------------------------------------"""
        association_matrix = {}  # key = term_term ___ value = Cij

        # TODO - improve!!!

        for key, value in tweet_id_data.items():

            if key not in sorted_cos_sim:
                continue
            for term1, val1 in value[0].items():
                for term2, val2 in value[0].items():
                    if (term1 in counter_of_terms.keys() or term2 in counter_of_terms.keys()) or term1 == term2:  # TODO - check if we can save only half
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
                association_matrix_sij[Sij] = association_matrix[Sij] / (
                        association_matrix[Cii] + association_matrix[Cjj] - association_matrix[Sij])

        association_matrix_sij = dict(sorted(association_matrix_sij.items(), key=lambda e: e[1], reverse=True))

        """--------------------------------------Normalized Association Matrix-----------------------------------------"""

        copy_of_term = copy.deepcopy(counter_of_terms)

        lst_of_word_to_add = []
        for key in association_matrix_sij.keys():
            if len(copy_of_term.keys()) == 0:
                break
            if key[0] in copy_of_term.keys():
                lst_of_word_to_add.append(key[1])
                del copy_of_term[key[0]]

        return lst_of_word_to_add

