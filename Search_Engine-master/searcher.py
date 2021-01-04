from ranker import Ranker
import utils


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

        self.relevant_docs = {}
        self.counter_of_terms = {}
        self.n_relevant = 0

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

        relevant_docs = self._relevant_docs_from_posting(query_as_list)
        n_relevant = len(relevant_docs)
        ranked_doc_ids = Ranker.rank_relevant_docs(self._ranker, relevant_docs)[:10]  # TODO - what about k
        return self.n_relevant, ranked_doc_ids
        # return n_relevant, ranked_doc_ids  # original
        # return ranked_doc_ids  # not test

    # feel free to change the signature and/or implementation of this function
    # or drop altogether.
    def _relevant_docs_from_posting(self, query_as_list):
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query_as_list: parsed query tokens
        :return: dictionary of relevant documents mapping doc_id to document frequency.
        """
        # relevant_docs = {}
        # for term in query_as_list:
        #     posting_list = self._indexer.get_term_posting_list(term)
        #     for doc_id, tf in posting_list:
        #         df = relevant_docs.get(doc_id, 0)
        #         relevant_docs[doc_id] = df + 1
        # return relevant_docs


        # flag_open = True
        # file_name = ""

        # posting_dic = {}
        posting_dic = self._indexer.load_index("idx_bench.pkl")["posting"]
        # invert_dic = {}
        invert_dic = self._indexer.load_index("idx_bench.pkl")["invert"]

        for term in query_as_list:
            try:  # an example of checks that you have to do

                upper_term = term.upper()
                lower_term = term.lower()
                if term not in invert_dic and lower_term not in invert_dic and upper_term not in invert_dic:
                    continue
                if lower_term in invert_dic:
                    term = lower_term
                elif upper_term in invert_dic:
                    term = upper_term

                """--------------------------------------Counter of terms in the query-----------------------------------------"""

                if term in self.relevant_docs.keys():
                    self.counter_of_terms[term] += 1
                    continue
                else:
                    self.counter_of_terms[term] = 1

                """--------------------------------------Open and Close posting files-----------------------------------------"""
                self.relevant_docs[term] = posting_dic[term]
                self.n_relevant += len(posting_dic[term])
                # if self._indexer[term][1] != file_name and not flag_open:
                #     file.close()
                #     flag_open = True
                #
                # if flag_open:
                #     file_name = self._indexer[term][1]
                #     with open(file_name, 'rb') as file:
                #         information = dict(pickle.load(file))
                #     flag_open = False
                #
                # self.relevant_docs[term] = information[term]

            except:
                print('term {} not found in posting'.format(term))


        return [self.relevant_docs, self.counter_of_terms]
