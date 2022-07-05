# -*- coding: utf-8 -*-
# @Author  : H. Lewis Lu
# @Time    : ${DATE} ${TIME}
# @Function: ${Class} Preprocessor for the structural topic modelling, 
# using structured scrapy data from weibo of the certain topic. 

import pandas as pds
import jieba
from collections import Counter

class preprocessor(object):
    def __init__(self, filename):
        self._file = filename
        self._encoding = 'utf-8'
        self._data = None

        sstr = filename.split('.')[-1]
        if sstr == 'csv':
            self._data = pds.read_csv(filename, low_memory=False, encoding=self._encoding)
        elif sstr == 'xlsx':
            self._data = pds.read_excel(filename)
        else:
            raise TypeError("File type not supported (support csv or xlsx)")

        self._stopwordsfiles = ["./stopwords/cn_stopwords.txt", "./stopwords/hit_stopwords.txt"]
        self._stopwords = self.init_stopwords()
        self._dataframe = pds.DataFrame(self._file)

    def print_DataFrameHeader(self):
        if self._dataframe is not None:
            print( self._dataframe.head() )
        else:
            raise Warning("data frame not existed.")

    def init_stopwords(self):
        rtn = []
        # iterate the stopword file list
        for file in self._stopwordsfiles:
            tmp =  [line.strip() for line in open(file, 'r', encoding='utf-8').readlines()]
            rtn.append(tmp)
        return rtn

    def format_coloumn_as_pure_chinese(self, coloumn_text):
        '''Purge Non-Chinese Characters'''
        if coloumn_text not in self._dataframe.columns:
            raise ValueError(r"DataFrame does not have colomun named as ${coloumn_text}")
        else:
            document_list = list(self._dataframe[coloumn_text])
            chinese_list = []
            for doc in document_list:
                if type(doc) is not str:
                    continue    
                content_str = ''
                for _char in doc:
                    if (_char >= u'\u4e00' and _char <= u'\u9fa5'):
                        content_str = content_str + _char
                    else:
                        continue
                chinese_list.append(content_str)
            return chinese_list

    def split_jieba(self, split_coloumn_name):
        ''' 
        split the designated coloumn in the dataframe 
        
        @return: return the cleaned (w/o stopwords) splitted word list
        '''
        chinese_list = self.format_coloumn_as_pure_chinese(split_coloumn_name)
        split_word_list = []
        for clist in chinese_list:
            res = []    
            seg_list = jieba.cut(clist)
            for word in seg_list:
                res.append(word)
            split_word_list.append(res)
        
        cleaned_split_word_list = []
        for word_list in split_word_list:
            line_clean = []
            for word in word_list:
                if word in self._stopwords:
                    continue
                line_clean.append(word)
            cleaned_split_word_list.append(line_clean)
        return cleaned_split_word_list

    def combine_split_word(self, cleaned_split_word_list):
        all_words_in_one_vector = []
        for line in cleaned_split_word_list:
            for word in line:
                all_words_in_one_vector.append(word)
        wc_ = Counter(all_words_in_one_vector)
        word_count = wc_.most_common(50)
        # print( wordcount )
        frequence_list = []
        for i in range(len(word_count)):
            frequence_list.append(word_count[i][0])
        # 获取不在停用词库中的高频词
        need_to_add_stopword = []
        for i in frequence_list:
            if i not in self._stopwords:
                need_to_add_stopword.append(i)
            else:
                continue
        # 输出不在停用词库中的高频词
        print("\n".join(str(i) for i in need_to_add_stopword))  

    def cleaned_to_new_dataframe_coloumn(self, split_coloumn_name, new_coloumn_name):
        word_list = []
        cleaned_contents = self.split_jieba(split_coloumn_name)
        for i in range(len( cleaned_contents )):
            k_list=['']
            for j in range(len( cleaned_contents[i] )):
                k_list[0] += (cleaned_contents[i][j] + ' ')
            word_list.append(k_list[0])
    
        df_new_col = pds.DataFrame({new_coloumn_name:word_list})
        print( df_new_col.head() )
        
        self._old_dataframe = self._dataframe
        self._dataframe = self._dataframe.join(df_new_col)

    def save_as_new_csv(self, new_csv_filename):
        if self._old_dataframe is not None:
            raise ValueError("No new coloumn of the Data Frame is designated.")
        else:
            self._dataframe.to_csv(new_csv_filename, index=True, encoding=self._encoding)


def main():
    pass

if __name__ == '__main__':
    main()
    