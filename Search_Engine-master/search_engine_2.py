import math
import os
import time

import pandas as pd
from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils


# DO NOT CHANGE THE CLASS NAME
class SearchEngine:

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation, but you must have a parser and an indexer.
    def __init__(self, config=None):
        self._config = config
        self._parser = Parse()
        self._indexer = Indexer(config)
        self._model = None

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def build_index_from_parquet(self, fn):
        """
        Reads parquet file and passes it to the parser, then indexer.
        Input:
            fn - path to parquet file
        Output:
            No output, just modifies the internal _indexer object.
        """
        df = pd.read_parquet(fn, engine="pyarrow")
        documents_list = df.values.tolist()
        # Iterate over every document in the file
        number_of_documents = 0

        for idx, document in enumerate(documents_list):
            # parse the document
            parsed_document = self._parser.parse_doc(document)
            number_of_documents += 1
            # index the document data
            self._indexer.add_new_doc(parsed_document)
        print('Finished parsing and indexing.')

        self._indexer.save_index("idx_bench.pkl")

        indexer_dic = utils.load_obj("idx_bench")
        #
        # self._indexer.save_index("idx.pkl")  # TODO - we need submit this
        #
        # indexer_dic = utils.load_obj("idx")  # TODO - we need submit this

        localMethod = False
        globalMethod = True
        wordNet = False
        spellChecker = False

        if localMethod:
            indexer_dic["local"] = True

        if wordNet:
            indexer_dic["wordnet"] = True

        if spellChecker:
            indexer_dic["spellChecker"] = True



        if globalMethod:
            docs_dic, Sij_dic = compute_Wi(indexer_dic, globalMethod)
            indexer_dic["docs"] = docs_dic
            indexer_dic["global"] = Sij_dic
        else:
            docs_dic = compute_Wi(indexer_dic)
            indexer_dic["docs"] = docs_dic

        utils.save_obj(indexer_dic, "idx_bench")
        # utils.save_obj(indexer_dic, "idx")  # TODO - we need submit this



    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def load_index(self, fn):
        """
        Loads a pre-computed index (or indices) so we can answer queries.
        Input:
            fn - file name of pickled index.
        """
        self._indexer.load_index(fn)

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def load_precomputed_model(self, model_dir=None):
        """
        Loads a pre-computed model (or models) so we can answer queries.
        This is where you would load models like word2vec, LSI, LDA, etc. and
        assign to self._model, which is passed on to the searcher at query time.
        """
        pass

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def search(self, query):
        """
        Executes a query over an existing index and returns the number of
        relevant docs and an ordered list of search results.
        Input:
            query - string.
        Output:
            A tuple containing the number of relevant search results, and
            a list of tweet_ids where the first element is the most relavant
            and the last is the least relevant result.
        """

        searcher = Searcher(self._parser, self._indexer, model=self._model)
        return searcher.search(query)



def compute_Wi(indexer, globalMethod=None):

    information = indexer["docs"]
    invert = indexer["invert"]

    if globalMethod is not None:
        Cij_dic = {}

    for key, value in information.items():

        to_remove = []
        to_change = {}

        if globalMethod is not None:
            tweet_copy = {}

        for k, v in value[0].items():

            if k not in invert and k.upper() in invert:
                term = k.upper()
                to_change[k] = term

            elif k not in invert and k.lower() in invert:
                term = k.lower()
                to_change[k] = term

            elif k not in invert:
                to_remove.append(k)
                continue

            else:
                term = k

            if globalMethod is not None:
                tweet_copy[term] = value[0][k]

            tf = v / value[1]
            idf = math.log2(len(indexer["docs"]) / invert[term])
            tf_idf = round(tf * idf, 3)
            value[0][k] = tf_idf
            x = tf_idf ** 2
            information[key][2] += tf_idf ** 2

        for k in to_remove:
            del value[0][k]

        for old, new in to_change.items():
            value[0][new] = value[0].pop(old)

        if globalMethod is not None:
            for term1, frq1 in tweet_copy.items():
                for term2, frq2 in tweet_copy.items():

                    key = (term1, term2)
                    if key not in Cij_dic:
                        Cij_dic[key] = frq1*frq2
                    else:
                        Cij_dic[key] += frq1*frq2

    if globalMethod is None:
        return information

    else:
        Sij_dic = {}
        for Sij, value in Cij_dic.items():
            if Sij[0] == Sij[1]:
                continue
            Cii = (Sij[0], Sij[0])
            Cjj = (Sij[1], Sij[1])
            Sij_dic[Sij] = Cij_dic[Sij] / (Cij_dic[Cii] + Cij_dic[Cjj] - Cij_dic[Sij])

        Sij_dic = dict(sorted(Sij_dic.items(), key=lambda e: e[1], reverse=True))

        return information, Sij_dic

