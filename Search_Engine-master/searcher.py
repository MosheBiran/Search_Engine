import copy

from spellchecker import SpellChecker
from ranker import Ranker
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
        # indexer_dic = indexer.load_index("idx_bench.pkl")
        indexer_dic = indexer.load_index("idx.pkl")  # TODO - this we need to submit
        if "tweet_dic" in indexer_dic:
            self._ranker = Ranker(indexer_dic["posting"], indexer_dic["docs"], indexer_dic["tweet_dic"])
        else:
            self._ranker = Ranker(indexer_dic["posting"], indexer_dic["docs"])

        self._model = model

        self.posting_dic = indexer_dic["posting"]
        self.invert_dic = indexer_dic["invert"]
        self.doc_dic = indexer_dic["docs"]

        if "word2vec" in indexer_dic and model is not None:
            self.word2vec = True
        else:
            self.word2vec = False

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

        if "spellChecker" in indexer_dic:
            self.spellcheck = True
        else:
            self.spellcheck = False


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

        if self.spellcheck:
            query_as_list = self.spell_check_query(query_as_list)

        if self.Sij_dic is not None:
            query_as_list.extend(self.expand_query_global_method(query_as_list))


        if self.word_net:
            expend = []
            for term in query_as_list:
                res = self.WordNet(term, query_as_list)
                if res is not None:
                    expend.append(res)

            if len(expend) != 0:
                print(expend)  # TODO - For Report
                query_as_list.extend(expend)

        if self.word2vec:

            if self.local:

                lst_before_extend = self._relevant_docs_from_posting(query_as_list)

                add_to_query = Ranker.compute_extend_word(self._ranker, lst_before_extend)

                query_as_list.extend(add_to_query)

                self.counter_of_terms.clear()
                self.unique_tweets_num.clear()
                self.relevant_docs.clear()


            relevant_docs = self.second(query_as_list)
            ranked_doc_ids = Ranker.rank_relevant_docs_w2v(self._ranker, self._model, query_as_list, relevant_docs)

            return len(ranked_doc_ids), ranked_doc_ids


        if self.local:
            lst_before_extend = self._relevant_docs_from_posting(query_as_list)

            add_to_query = Ranker.compute_extend_word(self._ranker, lst_before_extend)

            query_as_list.extend(add_to_query)

            self.counter_of_terms.clear()
            self.unique_tweets_num.clear()
            self.relevant_docs.clear()

            lst_After_extend = self.second(query_as_list)

            ranked_doc_ids = Ranker.rank_relevant_docs(self._ranker, lst_After_extend)

            return len(ranked_doc_ids), ranked_doc_ids



        relevant_docs = self.second(query_as_list)
        ranked_doc_ids = Ranker.rank_relevant_docs(self._ranker, relevant_docs)

        return len(ranked_doc_ids), ranked_doc_ids


    # feel free to change the signature and/or implementation of this function
    # or drop altogether.
    def _relevant_docs_from_posting(self, query_as_list):
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query_as_list: parsed query tokens
        :return: dictionary of relevant documents mapping doc_id to document frequency.
        """

        """--------------------------------------Original Searcher-----------------------------------------"""


        for term in query_as_list:
            try:

                upper_term = term.upper()
                lower_term = term.lower()
                if term not in self.invert_dic and lower_term not in self.invert_dic and upper_term not in self.invert_dic:
                    new_term = self.WordNet(term, query_as_list)
                    if new_term is None:
                        continue
                    term = new_term
                elif lower_term in self.invert_dic:
                    term = lower_term
                elif upper_term in self.invert_dic:
                    term = upper_term

                """--------------------------------------Counter of terms in the query-----------------------------------------"""

                if term in self.counter_of_terms.keys():
                    self.counter_of_terms[term] += 1
                    continue
                else:
                    self.counter_of_terms[term] = 1

                """--------------------------------------Open and Close posting files-----------------------------------------"""

                self.relevant_docs[term] = self.posting_dic[term]
                self.unique_tweets_num.update(set(list(self.posting_dic[term].keys())))


            except:
                print('term {} not found in posting'.format(term))

        return [self.relevant_docs, self.counter_of_terms]


    def expand_query_global_method(self, query_as_list):

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

        query_copy_not_delete = copy.deepcopy(query_copy)

        lst_of_word_to_add = []
        for key, value in self.Sij_dic.items():
            if self.Sij_dic[key] < 0.25 and len(lst_of_word_to_add) > 1:
                break
            if len(query_copy) == 0:
                break
            if key[0] in query_copy:
                if key[1] not in query_copy_not_delete and key[1] not in lst_of_word_to_add:
                    lst_of_word_to_add.append(key[1])
                del query_copy[key[0]]

        for word in lst_of_word_to_add:  # TODO - for the report
            print(word)

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


    def second(self, query_as_list):

        for term in query_as_list:
            try:

                upper_term = term.upper()
                lower_term = term.lower()
                if term not in self.invert_dic and lower_term not in self.invert_dic and upper_term not in self.invert_dic:
                    continue
                elif lower_term in self.invert_dic:
                    term = lower_term
                elif upper_term in self.invert_dic:
                    term = upper_term

                """--------------------------------------Counter of terms in the query-----------------------------------------"""

                if term in self.counter_of_terms.keys():
                    self.counter_of_terms[term] += 1
                    continue
                else:
                    self.counter_of_terms[term] = 1

                """--------------------------------------Open and Close posting files-----------------------------------------"""
                to_add = {}
                for key in self.posting_dic[term]:
                    tweet = self.doc_dic[key]
                    flag = False
                    if len(tweet[0]) >= len(query_as_list):
                        to_add[key] = self.posting_dic[term][key]
                        self.unique_tweets_num.update(set(key))

                if len(to_add) > 0:
                    self.relevant_docs[term] = to_add

            except:
                print('term {} not found in posting'.format(term))

        return [self.relevant_docs, self.counter_of_terms]


    def spell_check_query(self, query_as_list):
        checker = SpellChecker()
        new_query = []
        for term in query_as_list:
            if term not in self.invert_dic and term.lower() not in self.invert_dic and term.upper() not in self.invert_dic:
                correction = checker.correction(term)
                if correction in self.invert_dic:
                    new_query.append(correction)
                    continue
                elif correction.upper() in self.invert_dic:
                    new_query.append(correction.upper())
                    continue
                elif correction.lower() in self.invert_dic:
                    new_query.append(correction.lower())
                    continue

            new_query.append(term)

        return new_query

    def WordNet_w2v(self, term, query_as_list):
        syns_for_term = wordnet.synsets(term)
        # if term not in self._model.wv.vocab:
        for syns in syns_for_term:
            lemmas = set(syns._lemma_names)
            for lemma in lemmas:
                # print(lemma)
                if lemma.lower() not in query_as_list and lemma.lower() in self.invert_dic and lemma in self._model.wv.vocab:
                    # if not self._model.wv.doesnt_match(query_as_list + [lemma]) == lemma:
                    return lemma.lower()

        return None

    def Word2VecExpansion(self, query_as_list):
        syns_for_term = self._model.wv.most_similar(query_as_list)[:15]
        for lemma, val in syns_for_term:
            # if lemma not in query_as_list and lemma in self.invert_dic:
            if val > 0.6:
                return lemma.lower()
            else:
                return None

        return None
