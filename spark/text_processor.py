from pyspark.sql.types import *
from pyspark.sql import DataFrameReader, SQLContext
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql import Row
from pyspark.sql import Window

from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml import Pipeline

class CleanTrackName(object):
   
    def __init__(self):
        pass

    def remove_noise(self, df1, col_name):
        df_clean = df1.withColumn(col_name+'_', regexp_replace(col(col_name), '\([\w|\W]+\)', ''))\
                      .withColumn(col_name+'_', regexp_replace(col(col_name+'_'), '[_#+\W+\".!?\\-\/]', ''))
        df_clean = df_clean.filter(df_clean[col_name+'_'] != '')\
                           .withColumn(col_name+'_', trim(lower(col(col_name+'_'))))\
                           .withColumnRenamed(col_name, 'original_'+col_name)\
                           .withColumnRenamed(col_name+'_', col_name)
        return df_clean

    def is_digit(self, val):
        if val:
            return val.isdigit()
        else:
            return False

    def add_name_key(self, df1, which='tiktok'):
        if which == 'tiktok':
            mark = 't'
        else:
            mark = 'm'

        is_digit_udf = udf(self.is_digit, BooleanType())        
           
        df1 = df1.withColumn('track_id', concat(lit(mark), col('track_id')))\
                 .withColumn('name_key_1', f.when(is_digit_udf(substring(col('song_title'),0,1)), f.lit('others')).otherwise(substring(col('song_title'),0,1)))
        return df1

class CalTextSimilarity(object):

    def __init__(self, sc):
        self.sc = sc

    def cal_tfidf(self, df1, df2):
        columns = ['song_title', 'artist_name']
        dataCombined = df1.union(df2)
        preProcStages = []
        for col in columns:
            regexTokenizer = RegexTokenizer(gaps=False, pattern='\w', inputCol=col, outputCol=col+'_Token')
            #stopWordsRemover = StopWordsRemover(inputCol=col+'Token', outputCol=col+'SWRemoved') many of the words in song titles/artist names maybe stop words
            countVectorizer = CountVectorizer(minDF=1, inputCol=col+'_Token', outputCol=col+'_TF') # default minDF=1, calculate all the words
            idf = IDF(inputCol=col+'_TF', outputCol=col+'_IDF') 
            preProcStages += [regexTokenizer, countVectorizer, idf] #stopWordsRemover
        pipeline = Pipeline(stages=preProcStages)
        model = pipeline.fit(dataCombined)
        dataCombined = model.transform(dataCombined)
        return dataCombined.select('track_id', 'song_title_IDF', 'artist_name_IDF')

    def cosine_similarity(self, X, Y):
        denom = X.norm(2) * Y.norm(2)
        if denom == 0.0:
            return -1.0
        else:
            return X.dot(Y) / float(denom)

    def generate_lookup_table(self, sc, dataCombined):

        lookupTable = sc.broadcast(dataCombined.rdd.map(lambda x: (x['track_id'], {'song_title_IDF':x['song_title_IDF'], 'artist_name_IDF':x['artist_name_IDF']})).collectAsMap())

        return lookupTable
        
    def similarities(self, idMusic, idTiktok, lookupTable):
        X, Y = lookupTable.value[idMusic], lookupTable.value[idTiktok] 
        title_simi = self.cosine_similarity(X['song_title_IDF'], Y['song_title_IDF'])
        artist_simi = self.cosine_similarity(X['artist_name_IDF'], Y['artist_name_IDF'])
        return title_simi, artist_simi

   def cal_text_simi(self, df1, df2):
       dataCombined = self.cal_tfidf(df1, df2)
       dataCombined = dataCombined.persist()
       lookupTable = self.generate_lookup_table(self.sc, dataCombined)
       pairId = df1.select('track_id').rdd\
                         .flatMap(list)\
                         .cartesian(df2.select('track_id')\
                         .rdd.flatMap(list))\
                         .persist()  ## change df name
       pairProdDF = pairId.map(lambda x: x + cts.similarities(x[0], x[1], lookupTable))
       pairProdDF = pairProdDF.persist()
       measureMapping = spark.createDataFrame(pairProdDF.map(lambda x: Row(music_track_id=x[0], 
                                                                       tiktok_track_id=x[1], 
                                                                       total_simi = float(x[2])+float(x[3]),
                                                                       title_simi=float(x[2]),
                                                                       artist_simi=float(x[3]))))
       return measureMapping 
