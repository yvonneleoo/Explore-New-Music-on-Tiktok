from pyspark.sql.types import *
from pyspark.sql import DataFrameReader, SQLContext
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql import Row
from pyspark.sql import Window

from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml import Pipeline

import boto3

class CleanTrackName(object):
   
    def __init__(self):
        pass

    def remove_noise(self, df1, col_name):
    """
    clean up the noise info (e.g. version) in title/artist name
    """
        df_clean = df1.withColumn(col_name+'_', regexp_replace(col(col_name), '\([\w|\W]+\)', ''))\
                      .withColumn(col_name+'_', regexp_replace(col(col_name+'_'), '[_#+\W+\".!?\\-\/]', ''))
        df_clean = df_clean.filter(df_clean[col_name+'_'] != '')\
                           .withColumn(col_name+'_', trim(lower(col(col_name+'_'))))\
                           .withColumnRenamed(col_name, 'original_'+col_name)\
                           .withColumnRenamed(col_name+'_', col_name)
        return df_clean

    def is_digit(self, val):
    """
    to help categorize song titles with the first letter to be digits
    """
        if val:
            return val.isdigit()
        else:
            return False

    def add_name_key(self, df1, which='tiktok'):
    """
    get the first letter of the song title to partition
    """
        if which == 'tiktok':
            mark = 't'
        else:
            mark = 'm'

        is_digit_udf = udf(self.is_digit, BooleanType())
           
        df1 = df1.withColumn('track_id', concat(lit(mark), col('track_id')))\
                 .withColumn('name_key_1', f.when(is_digit_udf(substring(col('song_title'),0,1)), f.lit('others')).otherwise(substring(col('song_title'),0,1)))
        return df1


class CleanMusicInfo(CleanTrackName):

    def __init__(self, spark):
        super().__init__()
        self.client = boto3.client('s3')
        self.spark = spark
        self.s3_ = boto3.resource('s3')
        self.bucketName = os.environ["S3_BUCKET"]
        self.route =

    def read_csv_s3(self, route, df_name):
    """
    read csv from s3
    """
        obj_track = self.client.get_object(Bucket=self.bucketName, Key=route+df_name)
        df = pd.read_csv(obj_track['Body'], header=[0, 1], skipinitialspace=True)
        return df

    def read_parquet_s3(self, df, route, partition=None, partition_key=None):
    """
    read parquet from s3
    """
        path = "s3a://{}/{}}/".format(self.bucketName, route)
        if partition:
            path += '{}={}'.format(partition, partition_key)
        return self.spark.read.load(path)
        
    def write_parquet_s3(self, df, route, partition=None):
    """
    write parquet to s3
    """
        path = "s3a://{}/{}}/".format(self.bucketName, route)
        if partition:
            df.write\
              .partitionBy(partition)\
              .option("header","false")\
              .parquet(path=path, mode="append")
        else:
            df.write\
              .option("header","false")\
              .parquet(path=path, mode="append")


    def flatten_multi_index(self, df):
    """
    flatten the multi index for the track.csv
    """
        df.columns.set_levels(['track_id', 'album', 'artist', 'set', 'track'],level=0,inplace=True) # set the flatten column names
        col_1 = df.columns.levels[1].tolist()
        col_1[0] = 'track_id' # rename the index to track_id
        df.columns.set_levels(col_1, level=1, inplace=True)
        return df

    def clean_sub_df(self, sub_df_name, sub_info_type=None):
    """
    clean song info, artist info, and genres info
    sub_df_name = [genre, track]
    sub_info_type = [None, song, artist]
    """
        if sub_df_name == 'genre':
            df = self.read_csv_s3('tiktok-music/fma_metadata/', 'genres.csv')
            return spark.createDataFrame(df.astype(str))
        elif sub_df_name == 'track':
            df = self.read_csv_s3('tiktok-music/fma_metadata/', 'track.csv')
            df1 = df.loc[:, df.columns.get_level_values(0).isin({'track_id',sub_info_type})]
            df1.columns = df1.columns.droplevel()
            df1 = spark.createDataFrame(df1.astype(str))
            assert " " not in ''.join(df1.columns)

            if sub_info_type == 'song':
                df1 = df1.withColumn('genre_1', regexp_extract(col('genres'), '(\[)(\w+)(.+)', 2)) # flatten genre info and  capture the first genre
                df1 = self.remove_noise(df1, 'title')

            elif sub_info_type == 'artist':
                df1 = self.remove_noise(df1, 'name')

            return df1

    def merge_clean_info(self, track, artist, genres):
    """
    merge the track, artist and genres table to be a clean info table, for fast query in later steps;
    set the first letter of song titles as a partition key, to partition the dataframe, to speed up title/artist similarity search in later steps;
    """
        clean_info = track.join(artist,track.track_id == artist.track_id, how = 'inner')\
                          .select(track['track_id'],track['title'].alias('song_title'),
                                  artist['name'].alias("artist_name"),
                                  track['genre_1'],track['original_title'].alias('original'))
        clean_info = clean_info.join(genres, clean_info.genre_1 == genres.genre_id,how = 'left')\
                               .select(clean_info['*'],genres['title'].alias('subgenre_name'),
                                       genres['top_level']).sort(col('song_title'))

        clean_info = clean_info.dropDuplicates(['song_title', 'artist_name']) # drop duplicate records if any
        print ("The total number of clean record is: {}".format(clean_info.count()))

        clean_info = self.add_name_key(clean_info, which='music') # set the first letter of song titles as a partition key
        
        return clean_df
      
      
      
class CalTextSimilarity(object):

    def __init__(self, sc):
        self.sc = sc

    def cal_tfidf(self, df1, df2):
    """
    calculate tfidf score to vectorize text
    """
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
    """
    udf cosine similarity function
    """
        denom = X.norm(2) * Y.norm(2)
        if denom == 0.0:
            return -1.0
        else:
            return X.dot(Y) / float(denom)

    def generate_lookup_table(self, sc, dataCombined):
    """
    generate lookuptable for vectors for fast look up
    """
        lookupTable = sc.broadcast(dataCombined.rdd.map(lambda x: (x['track_id'],
                                                                   {'song_title_IDF':x['song_title_IDF'],
                                                                    'artist_name_IDF':x['artist_name_IDF']})).collectAsMap())

        return lookupTable
        
    def similarities(self, idMusic, idTiktok, lookupTable):
    """
    functions to calculate the similarity score
    """
        X, Y = lookupTable.value[idMusic], lookupTable.value[idTiktok]
        title_simi = self.cosine_similarity(X['song_title_IDF'], Y['song_title_IDF'])
        artist_simi = self.cosine_similarity(X['artist_name_IDF'], Y['artist_name_IDF'])
        return title_simi, artist_simi

   def cal_text_simi(self, df1, df2):
   """
   main function to calculate text similarities
   """
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

