# import os
# import pandas as pd
#
# import search_engine_best
# import search_engine_1
# import search_engine_2
# import search_engine_3

import metrics

import os
import sys
import re
from datetime import datetime
import pandas as pd
import pyarrow.parquet as pq
import time
import timeit
import importlib
import logging
import configuration

if __name__ == '__main__':


    parquet_files = []
    df = pd.read_parquet('C:\\benchmark_data_train.snappy.parquet', engine='pyarrow')
    parquet_files.append(df)

    df = pd.concat(parquet_files, sort=False)





    # logging.basicConfig(filename='part_c_tests.log', level=logging.DEBUG,
    #                     filemode='w', format='%(levelname)s %(asctime)s: %(message)s')
    # import metrics
    #
    #
    # def test_file_exists(fn):
    #     if os.path.exists(fn):
    #         return True
    #     logging.error(f'{fn} does not exist.')
    #     return False
    #
    #
    tid_ptrn = re.compile('\d+')
    #
    #
    def invalid_tweet_id(tid):
        if not isinstance(tid, str):
            tid = str(tid)
        if tid_ptrn.fullmatch(tid) is None:
            return True
        return False


    bench_data_path = os.path.join('data', 'benchmark_data_train.snappy.parquet')
    bench_lbls_path = os.path.join('data', 'benchmark_lbls_train.csv')
    queries_path = os.path.join('data', 'queries_train.tsv')
    model_dir = os.path.join('.', 'model')

    start = datetime.now()
    try:

        bench_lbls = pd.read_csv(bench_lbls_path, dtype={'query': int, 'tweet': str, 'y_true': int})

        q2n_relevant = bench_lbls.groupby('query')['y_true'].sum().to_dict()

        logging.info("Successfully loaded benchmark labels data.")


        queries = pd.read_csv(os.path.join('data', 'queries_train.tsv'), sep='\t')


        config = configuration.ConfigClass()

        # # do we need to download a pretrained model?
        # model_url = config.get_model_url()
        # if model_url is not None and config.get_download_model():
        #     import utils
        #
        #     dest_path = 'model.zip'
        #     utils.download_file_from_google_drive(model_url, dest_path)
        #     if not os.path.exists(model_dir):
        #         os.mkdir(model_dir)
        #     if os.path.exists(dest_path):
        #         utils.unzip_file(dest_path, model_dir)
        #         logging.info(f'Successfully downloaded and extracted pretrained model into {model_dir}.')
        #     else:
        #         logging.error('model.zip file does not exists.')

        # test for each search engine module  #TODO - Remove the '3'
        engine_modules = ['search_engine_' + name for name in ['1', '2', '3', 'best']]
        for engine_module in engine_modules:
            try:
                # try importing the module
                se = importlib.import_module(engine_module)
                print(f"Successfully imported module {engine_module}.")

                engine = se.SearchEngine(config=config)

                # test building an index and doing so in <1 minute
                build_idx_time = timeit.timeit(
                    "engine.build_index_from_parquet(bench_data_path)",
                    globals=globals(), number=1
                )
                print(
                    f"Building the index in {engine_module} for benchmark data took {build_idx_time} seconds.")
                if build_idx_time > 60:
                    print('Parsing and index our *small* benchmark dataset took over a minute!')
                # # test loading precomputed model
                # engine.load_precomputed_model(model_dir)

                # test that we can run one query and get results in the format we expect
                n_res, res = engine.search('bioweapon')
                if n_res is None or res is None or n_res < 1 or len(res) < 1:
                    print('basic query for the word bioweapon returned no results')
                else:
                    print(f"{engine_module} successfully returned {n_res} results for the query 'bioweapon'.")
                    invalid_tweet_ids = [doc_id for doc_id in res if invalid_tweet_id(doc_id)]
                    if len(invalid_tweet_ids) > 0:
                        print("the query 'bioweapon' returned results that are not valid tweet ids: " + str(
                            invalid_tweet_ids[:10]))

                # run multiple queries and test that no query takes > 10 seconds
                queries_results = []
                if queries is not None:
                    for i, row in queries.iterrows():
                        queries_results = []
                        q_id = row['query_id']
                        q_keywords = row['keywords']
                        # q_keywords = row['information_need']
                        start_time = time.time()
                        q_n_res, q_res = engine.search(q_keywords)
                        end_time = time.time()
                        q_time = end_time - start_time
                        if q_n_res is None or q_res is None or q_n_res < 1 or len(q_res) < 1:
                            print(f"Query {q_id} with keywords '{q_keywords}' returned no results.")
                        else:

                            print("\n\n   ***********************************************************   \n\n")

                            print(f"{engine_module} successfully returned {q_n_res} results for query number {q_id}.")
                            invalid_tweet_ids = [doc_id for doc_id in q_res if invalid_tweet_id(doc_id)]

                            # TODO - print 5 Tweet ids
                            # to_show = [1, 2, 4, 7, 8]
                            # if q_id in to_show:
                            #     print("------  " + q_keywords + "  ------")
                            #     for i in range(5):
                            #         print("\n"+ q_res[i])
                            #         df1 = df[df['tweet_id'].str.contains(str(q_res[i]))]
                            #         with pd.option_context('display.max_colwidth', 1000):
                            #             print(df1.full_text)

                            print("------  " + q_keywords + "  ------")
                            for i in range(5):
                                print("\n"+ q_res[i])
                                df1 = df[df['tweet_id'].str.contains(str(q_res[i]))]
                                with pd.option_context('display.max_colwidth', 1000):
                                    print(df1.full_text)

                            if len(invalid_tweet_ids) > 0:
                                print(f"Query  {q_id} returned results that are not valid tweet ids: " + str(invalid_tweet_ids[:10]))

                            queries_results.extend([(q_id, str(doc_id)) for doc_id in q_res if not invalid_tweet_id(doc_id)])
                        if q_time > 10:
                            print(f"Query {q_id} with keywords '{q_keywords}' took more than 10 seconds.")

                        queries_results = pd.DataFrame(queries_results, columns=['query', 'tweet'])

                        # merge query results with labels benchmark
                        q_results_labeled = None
                        if bench_lbls is not None and len(queries_results) > 0:
                            q_results_labeled = pd.merge(queries_results, bench_lbls,
                                                         on=['query', 'tweet'], how='inner', suffixes=('_result', '_bench'))
                            # q_results_labeled.rename(columns={'y_true': 'label'})
                            # zero_recall_qs = [q_id for q_id, rel in q2n_relevant.items() if metrics.recall_single(q_results_labeled, rel, q_id) == 0]
                            zero_recall_qs = []
                            if metrics.recall_single(q_results_labeled, q2n_relevant[q_id], q_id) == 0:
                                zero_recall_qs.append(q_id)
                            if len(zero_recall_qs) > 0:
                                print(
                                    f"{engine_module}'s recall for the following queries was zero {zero_recall_qs}.")

                        if q_results_labeled is not None:
                            # test that MAP > 0
                            # results_map = metrics.map(q_results_labeled)
                            # print(f"{engine_module} results have MAP value of {results_map}.")
                            # if results_map <= 0 or results_map > 1:
                            #     print(f'{engine_module} results MAP value is out of range (0,1).')

                            # test that the average across queries of precision,
                            # precision@5, precision@10, precision@50, and recall
                            # is in [0,1].
                            prec = metrics.precision(q_results_labeled)
                            p5 = metrics.precision(q_results_labeled.groupby('query').head(5))
                            p10 = metrics.precision(q_results_labeled.groupby('query').head(10))
                            p50 = metrics.precision(q_results_labeled.groupby('query').head(50))
                            d = {}
                            d[q_id] = q2n_relevant[q_id]
                            recall = metrics.recall(q_results_labeled, d)
                            # recall = metrics.recall(q_results_labeled, q2n_relevant)  # TODO - Original

                            print(f"{engine_module} results produced average precision of {prec}.")
                            print(f"{engine_module} results produced average precision@5 of {p5}.")
                            print(f"{engine_module} results produced average precision@10 of {p10}.")
                            print(f"{engine_module} results produced average precision@50 of {p50}.")
                            print(f"{engine_module} results produced average recall of {recall}.")
                            if prec < 0 or prec > 1:
                                print(f"The average precision for {engine_module} is out of range [0,1].")
                            if p5 < 0 or p5 > 1:
                                print(f"The average precision@5 for {engine_module} is out of range [0,1].")
                            if p5 < 0 or p5 > 1:
                                print(f"The average precision@5 for {engine_module} is out of range [0,1].")
                            if p50 < 0 or p50 > 1:
                                print(f"The average precision@50 for {engine_module} is out of range [0,1].")
                            if recall < 0 or recall > 1:
                                print(f"The average recall for {engine_module} is out of range [0,1].")

                if engine_module == 'search_engine_best': #  and test_file_exists('idx_bench.pkl'):
                    print('idx_bench.pkl found!')
                    print('idx_bench.pkl')
                    print('Successfully loaded idx_bench.pkl using search_engine_best.')

            except Exception as e:
                print(f'The following error occured while testing the module {engine_module}.')
                print(e)

    except Exception as e:
        print(e)

    run_time = datetime.now() - start
    print(f'Total runtime was: {run_time}')



    """-------------------------------------- My Try Before Copy -----------------------------------------"""

    # corpus_path = "C:\\data_part_c\\data"
    # output_path = "C:\\data_part_c\\output"
    # queries = "C:\\queries.txt"
    # k = 10
    # search_engine_best.main(corpus_path, output_path, queries, k)
    #
    #
    # bench_data_path = os.path.join('data', 'benchmark_data_train.snappy.parquet')
    # bench_lbls_path = os.path.join('data', 'benchmark_lbls_train.csv')
    # queries_path = os.path.join('data', 'queries_train.tsv')
    # model_dir = os.path.join('.', 'model')
    #
    # bench_lbls = pd.read_csv(bench_lbls_path, dtype={'query': int, 'tweet': str, 'y_true': int})
    #
    # q2n_relevant = bench_lbls.groupby('query')['y_true'].sum().to_dict()
    #
    # queries = pd.read_csv(os.path.join('data', 'queries_train.tsv'), sep='\t')
    #
    # #  TODO - if we do model add it here


