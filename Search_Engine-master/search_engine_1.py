import math
import os
import time

import pandas as pd
from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher_local_method import SearcherLocalMethod
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

        # if self._config is None:
        #     config = ConfigClass()
        #     config.set__corpusPath("")
        #     self._config = config

        # r = ReadFile(corpus_path=self._config.get__corpusPath())
        # """------------"""
        # """------------"""
        # number_of_documents = 0
        # documents_list = []
        # for root_path, direc, files_in_dir in os.walk(fn):
        #     r.set_new_Root(root_path)
        #     for file in files_in_dir:
        #         if file.endswith(".parquet"):
        #             documents_list += r.read_file(file)
        # Iterate over every document in the file

        for idx, document in enumerate(documents_list):
            # parse the document
            parsed_document = self._parser.parse_doc(document)
            number_of_documents += 1
            # index the document data
            self._indexer.add_new_doc(parsed_document)
        print('Finished parsing and indexing.')

        self._indexer.save_index("idx_bench.pkl")

        indexer_dic = utils.load_obj("idx_bench")
        docs_dic = compute_Wi(indexer_dic)  # TODO - check this shit
        indexer_dic["docs"] = docs_dic
        utils.save_obj(indexer_dic, "idx_bench")





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
        searcher = SearcherLocalMethod(self._parser, self._indexer, model=self._model)
        return searcher.search(query)




def main(corpus_path, output_path, queries, k):

    timeOfBuild = time.time()

    config = ConfigClass()
    config.set__corpusPath(corpus_path)
    config.set__outputPath(output_path)

    searchEngine = SearchEngine(config)
    searchEngine.build_index_from_parquet(corpus_path)




    print("Time To Build The Engine :%.2f" % ((time.time() - timeOfBuild) / 60) + '\n\r')

    query_counter = 0

    if type(queries) is list:  # TODO - maybe remove
        Lines = queries

    else:
        Lines = []
        with open(queries, 'rb') as fp:
            line = fp.readline()
            while line:
                if line.decode().strip():
                    Lines.append(line.decode().strip())
                line = fp.readline()

    for query in Lines:
        query_counter += 1

        print("Query Number : " + str(query_counter))  # TODO - Remove
        start = time.time()

        for doc_tuple in searchEngine.search(query):
            print('Tweet id: {} Score: {}'.format(doc_tuple[0], doc_tuple[1][2]))

        print("Query time :%.2f" % ((time.time() - start) / 60) + '\n\r')
        print("**************************\n")


def compute_Wi(indexer):

    information = indexer["docs"]
    invert = indexer["invert"]

    for key, value in information.items():

        to_remove = []

        for k, v in value[0].items():

            if k not in invert and k.upper() in invert:
                term = k.upper()

            elif k not in invert and k.lower() in invert:
                term = k.lower()

            elif k not in invert:
                to_remove.append(k)
                continue

            else:
                term = k

            tf = v / value[1]
            idf = math.log2(len(indexer["docs"]) / invert[term])
            tf_idf = round(tf * idf, 3)
            value[0][k] = tf_idf
            x = tf_idf ** 2
            information[key][2] += tf_idf ** 2

        for k in to_remove:
            del value[0][k]

    return information

