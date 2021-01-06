import copy

from ranker import Ranker
import utils
from nltk.corpus import wordnet


# DO NOT MODIFY CLASS NAME
class Searcher:
    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit. The model 
    # parameter allows you to pass in a precomputed model that is already in 
    # memory for the searcher to use such as LSI, LDA, Word2vec models. 
    # MAKE SURE YOU DON'T LOAD A MODEL INTO MEMORY HERE AS THIS IS RUN AT QUERY TIME.
    def __init__(self, parser, indexer, model=None):
        self._parser = parser
        self._indexer = indexer
        indexer_dic = indexer.load_index("idx_bench.pkl")
        self._ranker = Ranker(indexer_dic["posting"], indexer_dic["docs"])
        self._model = model

        self.posting_dic = indexer_dic["posting"]
        self.invert_dic = indexer_dic["invert"]
        self.doc_dic = indexer_dic["docs"]

        if "global" in indexer_dic:
            self.Sij_dic = indexer_dic["global"]
        else:
            self.Sij_dic = None

        if "wordnet" in indexer_dic:
            self.word_net = True
        else:
            self.word_net = False

        if "local" in indexer_dic:
            self.local = True
        else:
            self.local = False


        self.relevant_docs = {}
        self.counter_of_terms = {}
        self.unique_tweets_num = set()


    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def search(self, query, k=None):
        """ 
        Executes a query over an existing index and returns the number of 
        relevant docs and an ordered list of search results (tweet ids).
        Input:
            query - string.
            k - number of top results to return, default to everything.
        Output:
            A tuple containing the number of relevant search results, and 
            a list of tweet_ids where the first element is the most relavant 
            and the last is the least relevant result.
        """
        query_as_list = self._parser.parse_sentence(query)


        if self.Sij_dic is not None:
            query_as_list.extend(self.expand_query_global_method(query_as_list))


        if self.word_net:
            expend = []
            for term in query_as_list:
                res = self.WordNet(term, query_as_list)
                if res is not None:
                    expend.append(res)

            if len(expend) != 0:
                query_as_list.extend(expend)


        if self.local:
            lst_before_extend = self._relevant_docs_from_posting(query_as_list)

            add_to_query = Ranker.compute_extend_word(self._ranker, lst_before_extend)  # TODO - what about k

            query_as_list.extend(add_to_query)  # TODO - Maybe improve

            self.counter_of_terms.clear()
            self.unique_tweets_num = set()
            self.relevant_docs.clear()

            lst_After_extend = self._relevant_docs_from_posting(query_as_list)

            ranked_doc_ids = Ranker.rank_relevant_docs(self._ranker, lst_After_extend)  # TODO - what about k

            return len(ranked_doc_ids), ranked_doc_ids



        relevant_docs = self._relevant_docs_from_posting(query_as_list)
        ranked_doc_ids = Ranker.rank_relevant_docs(self._ranker, relevant_docs)  # TODO - what about k


        return len(ranked_doc_ids), ranked_doc_ids


    # feel free to change the signature and/or implementation of this function
    # or drop altogether.
    def _relevant_docs_from_posting(self, query_as_list):
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query_as_list: parsed query tokens
        :return: dictionary of relevant documents mapping doc_id to document frequency.
        """

        for term in query_as_list:
            try:  # an example of checks that you have to do

                upper_term = term.upper()
                lower_term = term.lower()
                if term not in self.invert_dic and lower_term not in self.invert_dic and upper_term not in self.invert_dic:
                    continue
                elif lower_term in self.invert_dic:
                    term = lower_term
                elif upper_term in self.invert_dic:
                    term = upper_term

                """--------------------------------------Counter of terms in the query-----------------------------------------"""

                if term in self.relevant_docs.keys():
                    self.counter_of_terms[term] += 1
                    continue
                else:
                    self.counter_of_terms[term] = 1

                """--------------------------------------Open and Close posting files-----------------------------------------"""
                self.relevant_docs[term] = self.posting_dic[term]
                self.unique_tweets_num.update(set(list(self.posting_dic[term].keys())))


            except:
                print('term {} not found in posting'.format(term))

        """--------------------------------------improved Searcher-----------------------------------------"""

        # for term1 in query_as_list:
        #     for term2 in query_as_list:
        #         try:
        #
        #             if term1 == term2:
        #                 continue
        #
        #             upper_term1 = term1.upper()
        #             lower_term1 = term1.lower()
        #             upper_term2 = term2.upper()
        #             lower_term2 = term2.lower()
        #
        #             if term1 not in self.invert_dic and lower_term1 not in self.invert_dic and upper_term1 not in self.invert_dic:
        #                 continue
        #             if lower_term1 in self.invert_dic:
        #                 term1 = lower_term1
        #             elif upper_term1 in self.invert_dic:
        #                 term1 = upper_term1
        #
        #             if term2 not in self.invert_dic and lower_term2 not in self.invert_dic and upper_term2 not in self.invert_dic:
        #                 continue
        #
        #             if lower_term2 in self.invert_dic:
        #                 term2 = lower_term2
        #
        #             elif upper_term2 in self.invert_dic:
        #                 term2 = upper_term2
        #
        #             if term1 in self.invert_dic and term2 in self.invert_dic:
        #
        #                 if term1 in self.relevant_docs.keys():
        #                     self.counter_of_terms[term1] += 1
        #                     continue
        #                 else:
        #                     self.counter_of_terms[term1] = 1
        #
        #                 self.relevant_docs[term1] = self.posting_dic[term1]
        #                 self.unique_tweets_num.update(set(list(self.posting_dic[term1].keys())))
        #                 break
        #
        #
        #         except:
        #             print('term {} not found in posting'.format(term1))
        #
        # final = {}
        #
        # for id in self.unique_tweets_num:
        #     tweet = self.doc_dic[id][0]
        #     for term1 in self.counter_of_terms:
        #         for term2 in self.counter_of_terms:
        #
        #             if term1 == term2:
        #                 continue
        #
        #             if term1 in tweet and term2 in tweet:
        #                 final[id] = 0
        #
        # return [final, self.counter_of_terms]


        return [self.relevant_docs, self.counter_of_terms]



    def expand_query_global_method(self, query_as_list):

        # query_copy = copy.deepcopy(query_as_list)  # TODO - Maybe make dic
        query_copy = {}
        for term in query_as_list:


            upper_term = term.upper()
            lower_term = term.lower()
            if term not in self.invert_dic and lower_term not in self.invert_dic and upper_term not in self.invert_dic:
                continue
            if lower_term in self.invert_dic:
                term = lower_term
            elif upper_term in self.invert_dic:
                term = upper_term

            query_copy[term] = 0

        query_copy_not_delete = copy.deepcopy(query_copy)  # TODO - change the name xD

        lst_of_word_to_add = []
        for key, value in self.Sij_dic.items():
            if self.Sij_dic[key] < 0.25 and len(lst_of_word_to_add) > 1:
                break
            if len(query_copy) == 0:
                break
            if key[0] in query_copy:
                if key[1] not in query_copy_not_delete and key[1] not in lst_of_word_to_add:
                    lst_of_word_to_add.append(key[1])
                # query_copy.remove(key[0])
                del query_copy[key[0]]

        return lst_of_word_to_add


    def WordNet(self, term, query_as_list):
        syns_for_term = wordnet.synsets(term)
        for syns in syns_for_term:
            lemmas = set(syns._lemma_names)
            for lemma in lemmas:
                if lemma not in query_as_list and lemma in self.invert_dic:
                    return lemma
                elif lemma.upper() not in query_as_list and lemma.upper() in self.invert_dic:
                    return lemma.upper()
                elif lemma.lower() not in query_as_list and lemma.lower() in self.invert_dic:
                    return lemma.lower()

        return None
