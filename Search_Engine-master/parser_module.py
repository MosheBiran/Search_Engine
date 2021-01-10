import json

from nltk import TweetTokenizer, re
from nltk.corpus import stopwords
from document import Document


def RepresentsInt(s):
    try:
        int(s)
        return True
    except ValueError:
        try:
            float(s)
            return True
        except ValueError:
            return False


class Parse:

    def __init__(self):
        # self.stop_words = frozenset(stopwords.words('english'))
        self.stop_words = stopwords.words('english')

        d = {}
        for word in self.stop_words:
            d[word] = 0
        self.stop_words = d
        self.stop_words['rt'] = 0
        self.stop_words['RT'] = 0
        self.stop_words['https'] = 0
        self.stop_words['www'] = 0
        self.stop_words['..'] = 0
        self.stop_words['tweeter.com'] = 0
        self.stop_words['twitter.com'] = 0
        self.punc = '''!()[]+{}-;:'"\, <>./|?^&*_~'''
        d = {}
        for word in self.punc:
            d[word] = 0
        w = '...'
        d[w] = 0
        self.punc = d

    def parse_sentence(self, text):
        """
        This function tokenize, remove stop words and apply lower case for every word within the text
        :param text:
        :return:
        """
        tweet_Tokenizer = TweetTokenizer()
        text_tokens = tweet_Tokenizer.tokenize(text)

        k_m_b_lst = ["Thousand", "thousand", "Million", "million", "Billion", "billion"]
        percents_lst = ["%", "percent", "percentage"]
        after_parse_lst = []
        i = 0
        while i < len(text_tokens):
            if not all(ord(char) < 128 for char in text_tokens[i]):
                i += 1
                continue

            if text_tokens[i][0] == '#':
                after_parse_lst.extend(self.hashtags(text_tokens[i]))
                i += 1
                continue

            elif text_tokens[i][0:4] == "http":
                i += 1
                continue

            elif self.word_to_number(text_tokens[i]) != False:
                text_tokens[i] = self.word_to_number(text_tokens[i])
                continue


            elif text_tokens[i] == text_tokens[i].capitalize() and text_tokens[i].isalpha():
                lst_entity_to_send = []
                lst_entity_to_send.append(text_tokens[i])
                i += 1
                while i < len(text_tokens) and text_tokens[i] == text_tokens[i].capitalize() and text_tokens[
                    i].isalpha():
                    lst_entity_to_send.append(text_tokens[i])
                    i += 1
                if len(lst_entity_to_send) > 1:
                    after_parse_lst.extend(self.addEntities(lst_entity_to_send))

                else:
                    after_parse_lst.append(lst_entity_to_send[0].upper())
                i -= 1

            elif text_tokens[i].replace(",", "").isnumeric():
                if i + 1 < len(text_tokens):
                    if text_tokens[i + 1] in k_m_b_lst:
                        after_parse_lst.extend(self.WithoutUnitsNum(text_tokens[i: i + 2]))
                        i += 1
                    elif text_tokens[i + 1] in percents_lst:
                        after_parse_lst.append(text_tokens[i] + "%")
                        i += 1
                    else:
                        after_parse_lst.extend(self.WithoutUnitsNum([text_tokens[i]]))
                else:
                    after_parse_lst.extend(self.WithoutUnitsNum([text_tokens[i]]))
            else:
                if text_tokens[i] not in self.stop_words and text_tokens[i].lower() not in self.stop_words:
                    if text_tokens[i].isupper():
                        after_parse_lst.append(text_tokens[i])
                    else:
                        after_parse_lst.append(text_tokens[i].lower())
            i += 1


        after_parse_lst = [x for x in after_parse_lst if x not in self.stop_words and x not in self.punc and not ' ' in x and not '...' in x and not '\n' in x]
        return after_parse_lst

    def parse_doc(self, doc_as_list):
        """
        This function takes a tweet document as list and break it into different fields
        :param doc_as_list: list re-preseting the tweet.
        :return: Document object with corresponding fields.
        """

        tweet_id = doc_as_list[0]
        tweet_date = doc_as_list[1]
        full_text = doc_as_list[2]
        url = doc_as_list[3]
        retweet_text = doc_as_list[4]
        retweet_url = doc_as_list[5]
        quote_text = doc_as_list[6]
        quote_url = doc_as_list[7]
        term_dict = {}
        tokenized_text = self.parse_sentence(full_text)

        if doc_as_list[3] is None or doc_as_list[3] == '[]' or doc_as_list[3] == '{}':
            url = None
        else:
            url = dict(json.loads(doc_as_list[3]))
            lst = list(url.values())
            tokenized_text += self.urls(lst[0])

            """---------------------------------Division into files------------------------------------"""

        doc_length = len(tokenized_text)  # after text operations.
        place_in_doc = 1
        for term in tokenized_text:

            if term in self.punc:
                doc_length -= 1
                continue

            if term.isalpha() and len(term) < 2:
                doc_length -= 1
                continue

            if term in self.stop_words or term.lower() in self.stop_words:
                doc_length -= 1
                continue

            if term == "":
                doc_length -= 1
                continue

            if term != term.upper() and term != term.lower() and " " not in term:
                if term == term.capitalize():
                    term = term.upper()
                else:
                    term = term.lower()

            if term not in term_dict.keys():
                term_dict[term] = [1, [place_in_doc]]
            else:
                term_dict[term][0] += 1
                term_dict[term][1].append(place_in_doc)
            place_in_doc += 1

        document = Document(tweet_id, tweet_date, full_text, url, retweet_text, retweet_url, quote_text,
                            quote_url, term_dict, doc_length)
        return document

    def hashtags(self, w):
        # w = "#MOSHEBiran_TheKing-OFThe!World19"
        lst_parsed = []
        if w[0] == "#":

            lst_parsed.append(w.replace("_", "").lower())
            w = w[1:len(w)]

            if w.isalpha() and (w.islower() or w.isupper()):
                lst_parsed.append(w.lower())
                return lst_parsed

            if w.isalpha() and ("_" or "-" or "~") not in w:
                myString = re.sub(r'((?<=[a-z])[A-Z]|(?<!\A)[A-Z](?=[a-z]))', r' \1', w)
                lst_parsed.extend([word.lower() for word in myString.split()])
                return lst_parsed

            else:

                myString = re.sub(r'((?<=[a-z])[A-Z]|(?<!\A)[A-Z](?=[a-z]))', r' \1', w)
                temp = ([word1.lower() for word1 in myString.split()])
                for word in temp:
                    if not word.isalpha():
                        start_of_next = 0
                        i = 0
                        while len(word) > i:

                            if word[i].islower():
                                while len(word) > i and word[i].islower():
                                    i += 1

                            elif word[i].isnumeric():
                                while len(word) > i and word[i].isnumeric():
                                    i += 1

                            else:
                                while len(word) > i and not word[i].isalnum():
                                    i += 1
                                    start_of_next = i
                                continue

                            lst_parsed.append(word[start_of_next: i].lower())
                            start_of_next = i
                            continue
                    else:
                        lst_parsed.append(word.lower())
        return lst_parsed

    def urls(self, w):
        w = str(w)
        lst_parsed = []
        start_point = 0
        counter = 0
        i = 0
        while len(w) != i:
            if not w[i].isalpha() and not w[i].isdigit():
                if w[i] == "." and (counter == 2 or (counter == 1 and w[start_point] != "w")):
                    while w[i] != "/" and i + 1 != len(w):
                        i += 1
                    domain = w[start_point: i]
                    return [domain]
                    # we saved only the domain
                    lst_parsed.append(w[start_point: i])
                    start_point = i + 1
                    counter += 1
                else:
                    if w[start_point: i] != "":
                        lst_parsed.append(w[start_point: i])
                        counter += 1
                    start_point = i + 1

            i += 1
        lst_parsed.append(w[start_point: i])
        return lst_parsed

    def WithoutUnitsNum(self, text):
        # text = ["1010.56", "123","Thousand", "10,123", "10,123,000", "55", "Million", "200", "55", "Billion", "35.66"]
        K_list = ["Thousand", "thousand"]
        M_list = ["Million", "million"]
        B_list = ["Billion", "billion"]
        out_list = []
        Key = ''

        size = len(text)
        for i in range(size):
            tempNum = text[i].replace(",", "")
            if RepresentsInt(tempNum):
                if i + 1 != size and (not RepresentsInt(text[i + 1])):
                    if text[i + 1] in K_list:
                        tempNum = tempNum + "000"
                    elif text[i + 1] in M_list:
                        tempNum = tempNum + "000000"
                    elif text[i + 1] in B_list:
                        tempNum = tempNum + "000000000"
                    i = i + 1

                count = float(tempNum)
                if count < 1000:
                    out_list.append(tempNum)
                    continue

                elif 1000 <= count < 1000000:
                    Key = 'K'
                elif 1000000 <= count < 1000000000:
                    Key = 'M'
                elif 1000000000 <= count:
                    Key = 'B'

                if "." in tempNum:
                    tempNum = tempNum.partition(".")[0]
                if len(tempNum) > 2:
                    j = len(tempNum)
                    while j > 0:
                        tempNum = tempNum[:j] + "." + tempNum[j:]
                        j = j - 3
                while tempNum.endswith('0') or tempNum.endswith('.'):
                    tempNum = tempNum[0:len(tempNum) - 1]
                out_list.append(tempNum + Key)

        return out_list

    def addEntities(self, text):
        lst = []
        size = len(text)
        full_name = text[0]
        lst.append(text[0].upper())
        for i in range(1, size):
            if text[i] in self.stop_words:
                continue
            if not all(ord(char) < 128 for char in text[i]):
                i += 1
                continue
            full_name += " " + text[i]
            lst.append(text[i].upper())
        lst.append(full_name)

        return lst

    # def string_to_date(lst):
    #     size = len(lst[1])
    #     s = ' '.join(lst[1])
    #
    #     if size == 1:
    #         d = datetime.strptime(s, '%B')
    #         return d.strftime('%m')
    #     if size == 2:
    #         if lst[0] == "DM" or lst[0] == "MD":
    #             d = datetime.strptime(s, '%d %B')
    #             return d.strftime('%d-%m')
    #         if lst[0] == "MY" or lst[0] == "YM":
    #             d = datetime.strptime(s, '%B %Y')
    #             return (d.strftime('%m-%Y'))
    #     else:
    #         # if len(lst[1][1]) > 3:
    #         #     lst[1][1]=lst[1][1][0:3].lower()
    #         d = datetime.strptime(s, '%d %B %Y')
    #         return d.strftime('%d-%m-%Y')

    # if __name__ == '__main__':
    #
    #     mDict = {**{month: month for month in calendar.month_abbr if month},
    #              **{month: 0 for month in calendar.month_name[1:] if month}}
    #     print(mDict)
    #
    #     text = "Do not forget to wear a mask August 19 2020"
    #     text_token = text.split()
    #
    #     i = 0
    #     date = ""
    #     word = []
    #     while i < len(text_token):
    #
    #         if text_token[i] in mDict:
    #             if i + 1 < len(text_token) and text_token[i + 1].isnumeric() and 0 < len(
    #                     text_token[i + 1]) < 3 and 0 < int(
    #                     text_token[i + 1]) < 32:
    #                 if i + 2 < len(text_token) and text_token[i + 2].isnumeric() and len(text_token[i + 2]) == 4:
    #                     date = string_to_date(["MDY", [text_token[i + 1], text_token[i], text_token[i + 2]]])
    #                     i += 3
    #                     continue
    #                 date = string_to_date(["MD", [text_token[i + 1], text_token[i]]])
    #                 i += 2
    #
    #             elif i + 1 < len(text_token) and text_token[i + 1].isnumeric() and len(text_token[i + 1]) == 4:
    #                 if i + 2 < len(text_token) and text_token[i + 2].isnumeric() and 0 < len(
    #                         text_token[i + 2]) < 3 and 0 < int(
    #                     text_token[i + 2]) < 32:
    #                     date = string_to_date(["MYD", [text_token[i + 2], text_token[i], text_token[i + 1]]])
    #                     i += 3
    #                     continue
    #                 date = string_to_date(["MY", [text_token[i], text_token[i + 1]]])
    #                 i += 2
    #             else:
    #                 date = string_to_date(["M", [text_token[i]]])
    #                 i += 1
    #
    #         elif text_token[i].isnumeric() and 0 < len(text_token[i]) < 3 and 0 < int(text_token[i]) < 32:
    #             if i + 1 < len(text_token) and text_token[i + 1] in mDict:
    #                 if i + 2 < len(text_token) and text_token[i + 2].isnumeric() and len(text_token[i + 2]) == 4:
    #                     date = string_to_date(["DMY", [text_token[i], text_token[i + 1], text_token[i + 2]]])
    #                     i += 3
    #                     continue
    #                 date = string_to_date(["DM", [text_token[i], text_token[i + 1]]])
    #                 i += 2
    #
    #             elif i + 1 < len(text_token) and text_token[i + 1].isnumeric() and len(text_token[i + 1]) == 4:
    #                 if i + 2 < len(text_token) and text_token[i + 2] in mDict:
    #                     date = string_to_date(["DYM", [text_token[i], text_token[i + 2], text_token[i + 1]]])
    #                     i += 3
    #
    #         elif text_token[i].isnumeric() and len(text_token[i]) == 4:
    #             if i + 1 < len(text_token) and text_token[i + 1] in mDict:
    #                 if i + 2 < len(text_token) and text_token[i + 2].isnumeric() and 0 < len(
    #                         text_token[i + 2]) < 3 and 0 < int(text_token[i + 2]) < 32:
    #                     date = string_to_date(["YMD", [text_token[i + 2], text_token[i + 1], text_token[i]]])
    #                     i += 3
    #                     continue
    #                 date = string_to_date(["YM", [text_token[i + 1], text_token[i]]])
    #                 i += 2

    def word_to_number(self, word):
        word_num_dic = {'zero': '0',
                        'one': '1',
                        'two': '2',
                        'three': '3',
                        'four': '4',
                        'five': '5',
                        'six': '6',
                        'seven': '7',
                        'eight': '8',
                        'nine': '9',
                        'ten': '10',
                        'eleven': '11',
                        'twelve': '12',
                        'thirteen': '13',
                        'fourteen': '14',
                        'fifteen': '15',
                        'sixteen': '16',
                        'seventeen': '17',
                        'eighteen': '18',
                        'nineteen': '19',
                        'twenty': '20',
                        'thirty': '30',
                        'forty': '40',
                        'fifty': '50',
                        'sixty': '60',
                        'seventy': '70',
                        'eighty': '80',
                        'ninety': '90',
                        'hundred': '100', }

        if word not in word_num_dic:
            return False
        else:
            return word_num_dic[word]
