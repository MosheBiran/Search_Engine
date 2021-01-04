import search_engine_best

if __name__ == '__main__':
    corpus_path = "C:\\data_part_c\\data"
    output_path = "C:\\data_part_c\\output"
    queries = "C:\\queries.txt"
    k = 10
    search_engine_best.main(corpus_path, output_path, queries, k)
