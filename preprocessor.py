# -*- coding: utf-8 -*-
# @Author  : H. Lewis Lu
# @Time    : ${DATE} ${TIME}
# @Function: ${Class} Preprocessor for the structural topic modelling, 
# using structured scrapy data from weibo of the certain topic. 

import pandas as pds
import jieba
from collections import Counter
from datetime import datetime


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
            raise FileNotFoundError("No .csv or .xlsx is found.")

        self._stopwordsroot = "./stopwords/"
        self._stopwordsfiles = [self._stopwordsroot+"cn_stopwords.txt", self._stopwordsroot+"hit_stopwords.txt", self._stopwordsroot+"baidu_stopwords.txt"]
        self._stopwords = self.init_stopwords()
        self._dataframe = pds.DataFrame(self._data)

    def print_DataFrame_Summary(self):
        print( f"This function list the data frame summary of the {self._file}, including the line numbers and #None in each indexed column" )
        print( self.return_dataFrame_column() )
        print( f"the shape of the data frame is (#rows, #cols): ({self._dataframe.index} , {self._dataframe.columns})" )

    def init_stopwords(self):
        """
        initilize the stopwords for the class member _stopwords
        """
        rtn = []
        # iterate the stopword file list
        for file in self._stopwordsfiles:
            tmp =  [line.strip() for line in open(file, 'r', encoding='utf-8').readlines()]
            rtn.append(tmp)
        return rtn

    def return_dataFrame_column(self):
        if self._dataframe is not None:
            return self._dataframe.columns
        else:
            raise Warning("data frame not existed.")

    def return_dataFrame_header(self):
        if self._dataframe is not None:
            return self._dataframe.head()
        else:
            raise Warning("data frame not existed.")

    def return_dataFrame_timeColumn(self, time_column_name):
        """
        This take the column name as the timing input, such as "发布时间"
        """
        if self._dataframe is None:
            raise Warning("data frame not existed.")
        else:
            time_list = list(self._dataframe[time_column_name])
            return time_list


    def trim_rows_accordingTo_time(self, time_column_name):
        time_list = self.return_dataFrame_timeColumn(time_column_name)
        num_none_time_index_list = []
        new_time_column = []
        base_time = time_list[0];
        sstr = base_time.split(' ')[0];
        dstr = sstr.split('/');
        base_datetime = datetime( int(dstr[2]), int(dstr[0]), int(dstr[1]) )
        for ii in range(len(time_list)):
            tmp = str(time_list[ii])
            #process the time stamp
            try:
                sstr = tmp.split(' ')[0];
                dstr = sstr.split('/');
                cur_datetime = datetime( int(dstr[2]), int(dstr[0]), int(dstr[1]) )
                ds = (cur_datetime - base_datetime).days
                new_time_column.append(ds)                
            except IndexError:
                num_none_time_index_list.append(ii);
    
        if len( num_none_time_index_list ) != 0:
            num_rows_with_time = len( time_list ) - len( num_none_time_index_list );
            print(f"number of the rows of the whole dataset is {len(time_list)}")
            print(f"number of the rows with time stamp is {num_rows_with_time}")
            print(f"number of the rows w/o time stamp is {len(num_none_time_index_list)}")

    def count_emotion(self, emotion_coloumn_name):
        emotion_dist = {}
        emotion_list = list( self._dataframe[emotion_coloumn_name] )
        for ii in range(len(self._dataframe)):
            pass

    def format_column_as_pure_chinese(self, column_name):
        '''Purge Non-Chinese Characters'''
        if column_name not in self._dataframe.columns:
            raise ValueError(f"DataFrame does not have colomun named as {column_name}")
        else:
            origin_list = list(self._dataframe[column_name])
            purged_list = []
            for doc in origin_list:
                if type(doc) is not str:
                    continue    
                content_str = ''
                for _char in doc:
                    if (_char >= u'\u4e00' and _char <= u'\u9fa5'):
                        content_str = content_str + _char
                    else:
                        continue
                purged_list.append(content_str)
            return purged_list

    def split_jieba(self, split_column_name):
        ''' 
        split the designated column in the dataframe 
        
        @return: return the cleaned (w/o stopwords) splitted word list
        '''
        # purged chinese column list
        chinese_list = self.format_column_as_pure_chinese(split_column_name)
        # the split word list to return 
        split_word_list = []
        # for-loop in purged list 
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

    def new_column_freq_count(self, cleaned_split_word_list):
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
        # 
        need_to_add_stopword = []
        for i in frequence_list:
            if i not in self._stopwords:
                need_to_add_stopword.append(i)
            else:
                continue
        # 
        print("\n".join(str(i) for i in need_to_add_stopword))  

    def cleaned_to_new_dataframe_column(self, split_column_name, new_column_name):
        word_list = []
        cleaned_contents = self.split_jieba(split_column_name)
        for i in range(len( cleaned_contents )):
            k_list=['']
            for j in range(len( cleaned_contents[i] )):
                k_list[0] += (cleaned_contents[i][j] + ' ')
            word_list.append(k_list[0])
    
        df_new_col = pds.DataFrame({new_column_name: word_list})        
        self._dataframe = self._dataframe.join(df_new_col)

    def save_as_new_csv(self, new_csv_filename):
        self._dataframe.to_csv(new_csv_filename, index=True, encoding=self._encoding)


# test unit for preprocessor
def main():
    filename = "./data/data_motion.csv"
    proc = preprocessor(filename)

    proc.print_DataFrame_Summary()

    # time_list = proc.return_dataFrame_timeColumn("发布时间")

    proc.trim_rows_accordingTo_time("发布时间")

    # proc.cleaned_to_new_dataframe_column("评论内容", "text")
    # print( proc.return_dataFrame_column() )

if __name__ == '__main__':
    main()
    