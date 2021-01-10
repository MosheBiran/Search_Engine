# DO NOT MODIFY CLASS NAME
import pickle


class Indexer:
    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def __init__(self, config):
        self.inverted_idx = {}
        self.postingDict = {}
        self.entities = {}
        self.config = config
        self.doc_dic = {}

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def add_new_doc(self, document):
        """
        This function perform indexing process for a document object.
        Saved information is captures via two dictionaries ('inverted index' and 'posting')
        :param document: a document need to be indexed.
        :return: -
        """

        max_term_f = 0
        doc_len = 0
        unique_terms = 0
        for value in document.term_doc_dictionary.values():
            doc_len += value[0]
            if value[0] == 1:
                unique_terms += 1
            if value[0] > max_term_f:
                max_term_f = value[0]

        """
        docs dic:
        key = doc name : invert | posting | docs
        
        value = the inner dictionary's
        
        invert dic:
        key = term

        value =  number of tweets he in 
                -------------------------------------------------------------------------------------------
        posting dic:
        key = term 

        value = [ {key = doc id : value = [[num in this tweet , [positions], max term freq in the tweet, unique terms in this tweet, doc_le]} ]
                -------------------------------------------------------------------------------------------
        doc dic
        key = tweet id 

        value = [ {key = terms : value = times in doc} ,max f, Wij ]]
        """

        document_dictionary = document.term_doc_dictionary

        self.doc_dic[document.tweet_id] = [{}, max_term_f, 0]

        # Go over each term in the doc
        for term in document_dictionary.keys():
            try:

                self.doc_dic[document.tweet_id][0][term] = document_dictionary[term][0]

                # new word
                upper_term = term.upper()
                lower_term = term.lower()

                """--------------------------------------Entities-----------------------------------------"""

                if " " in term:
                    # new entity
                    if term not in self.entities and term not in self.inverted_idx:
                        self.entities[term] = {}
                        self.entities[term][document.tweet_id] = [document_dictionary[term], max_term_f, unique_terms,
                                                                  doc_len]
                        continue

                    # second time entity
                    elif term in self.entities and term not in self.inverted_idx:
                        self.inverted_idx[term] = 2
                        """---------------point--------------------"""
                        self.postingDict[term] = self.entities[term]
                        self.postingDict[term][document.tweet_id] = [document_dictionary[term], max_term_f,
                                                                     unique_terms, doc_len]
                        del self.entities[term]
                        continue

                    # third and more  entity
                    elif term not in self.entities and term in self.inverted_idx:
                        self.inverted_idx[term] += 1
                        if term not in self.postingDict:
                            self.postingDict[term] = {}
                            """---------------point--------------------"""
                        self.postingDict[term][document.tweet_id] = [document_dictionary[term], max_term_f,
                                                                     unique_terms, doc_len]
                        continue

                """--------------------------------------@ - Terms-----------------------------------------"""

                if term[0] == "@" and term not in self.inverted_idx:
                    self.inverted_idx[term] = 1
                    self.postingDict[term] = {}
                    """---------------point--------------------"""
                    self.postingDict[term][document.tweet_id] = [document_dictionary[term], max_term_f, unique_terms,
                                                                 doc_len]
                    continue
                elif term[0] == "@" and term in self.inverted_idx:
                    self.inverted_idx[term] += 1
                    if term not in self.postingDict:
                        self.postingDict[term] = {}
                        """---------------point--------------------"""
                    self.postingDict[term][document.tweet_id] = [document_dictionary[term], max_term_f, unique_terms,
                                                                 doc_len]
                    continue

                """--------------------------------------Url-----------------------------------------"""

                # term from URL
                if term != lower_term and term != upper_term:
                    if term not in self.inverted_idx:
                        self.inverted_idx[term] = 1
                        self.postingDict[term] = {}
                        """---------------point--------------------"""
                        self.postingDict[term][document.tweet_id] = [document_dictionary[term], max_term_f,
                                                                     unique_terms, doc_len]
                        continue
                    else:
                        self.inverted_idx[term] += 1
                        if term not in self.postingDict:
                            self.postingDict[term] = {}
                            """---------------point--------------------"""
                        self.postingDict[term][document.tweet_id] = [document_dictionary[term], max_term_f,
                                                                     unique_terms, doc_len]
                        continue

                """--------------------------------------All other-----------------------------------------"""

                # all other
                if term not in self.inverted_idx and lower_term not in self.inverted_idx and upper_term not in self.inverted_idx:
                    self.inverted_idx[term] = 1
                    self.postingDict[term] = {}
                    """---------------point--------------------"""
                    self.postingDict[term][document.tweet_id] = [document_dictionary[term], max_term_f, unique_terms,
                                                                 doc_len]

                # Big and then small
                elif upper_term in self.inverted_idx and term.islower():
                    self.inverted_idx[term] = self.inverted_idx[upper_term] + 1
                    if upper_term in self.postingDict:
                        self.postingDict[term] = self.postingDict[upper_term]
                        del self.postingDict[upper_term]
                    else:
                        self.postingDict[term] = {}
                        """---------------point--------------------"""
                    del self.inverted_idx[upper_term]
                    self.postingDict[term][document.tweet_id] = [document_dictionary[term], max_term_f, unique_terms,
                                                                 doc_len]
                # small and then Big
                elif lower_term in self.inverted_idx and term.isupper():
                    self.inverted_idx[lower_term] += 1
                    if lower_term not in self.postingDict:
                        self.postingDict[lower_term] = {}
                        """---------------point--------------------"""
                    self.postingDict[lower_term][document.tweet_id] = [document_dictionary[term], max_term_f,
                                                                       unique_terms, doc_len]
                # small and then small
                else:
                    self.inverted_idx[term] += 1
                    if term not in self.postingDict:
                        self.postingDict[term] = {}
                        """---------------point--------------------"""
                    self.postingDict[term][document.tweet_id] = [document_dictionary[term], max_term_f, unique_terms,
                                                                 doc_len]
            except:
                print('problem with the following key {}'.format(term))

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def load_index(self, fn):
        """
        Loads a pre-computed index (or indices) so we can answer queries.
        Input:
            fn - file name of pickled index.
        """

        with open(fn, 'rb') as f:
            return pickle.load(f)


    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def save_index(self, fn):
        """
        Saves a pre-computed index (or indices) so we can save our work.
        Input:
              fn - file name of pickled index.
        """

        indexer_dic = {}
        indexer_dic["invert"] = self.inverted_idx
        indexer_dic["posting"] = self.postingDict
        indexer_dic["docs"] = self.doc_dic


        with open(fn , 'wb') as f:
            pickle.dump(indexer_dic, f, pickle.HIGHEST_PROTOCOL)


    # feel free to change the signature and/or implementation of this function 
    # or drop altogether.
    def _is_term_exist(self, term):
        """
        Checks if a term exist in the dictionary.
        """
        return term in self.postingDict

    # feel free to change the signature and/or implementation of this function 
    # or drop altogether.
    def get_term_posting_list(self, term):
        """
        Return the posting list from the index for a term.
        """
        return self.postingDict[term] if self._is_term_exist(term) else []
