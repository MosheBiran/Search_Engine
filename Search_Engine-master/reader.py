import os
import pandas as pd


class ReadFile:
    def __init__(self, corpus_path):
        self.corpus_path = corpus_path

    # def read_file(self, file_name):
    #     """
    #     This function is reading a parquet file contains several tweets
    #     The file location is given as a string as an input to this function.
    #     :param file_name: string - indicates the path to the file we wish to read.
    #     :return: a dataframe contains tweets.
    #     """
    #     full_path = os.path.join(self.corpus_path, file_name)
    #     df = pd.read_parquet(full_path, engine="pyarrow")
    #     return df.values.tolist()
    #
    def read_file(self, file_name):
        """
        This function is reading a parquet file contains several tweets
        The file location is given as a string as an input to this function.
        :param file_name: string - indicates the path to the file we wish to read.
        :return: a dataframe contains tweets.
        """
        docs_lst = []
        full_path = os.path.join(self.corpus_path, file_name)


        if ".parquet" in file_name:
            Data_Frame = pd.read_parquet(full_path, engine="pyarrow")
        else:
            for root_path, direc, files_in_dir in os.walk(full_path):
                for file in files_in_dir:
                    if file.endswith(".parquet"):
                        full_path = os.path.join(root_path, file)
                        new_Data_Frame = pd.read_parquet(full_path, engine="pyarrow")
                        docs_lst.append(new_Data_Frame)

            Data_Frame = pd.concat(docs_lst, sort=False)

        return Data_Frame.values.tolist()

    def set_new_Root(self, root):
        self.corpus_path = root
