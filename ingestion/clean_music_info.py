import boto3
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql import DataFrameReader, SQLContext
from pyspark.sql.functions import *
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.sql.types import BooleanType
from pyspark.sql import functions as f

import pandas as pd
import os
import time

from posgresql import PosgreConnector
from text_processor import CleanTrackName

if __name__ == '__main__':

    # set up coding environment and connection to s3
    os.environ["PYSPARK_PYTHON"]="/usr/bin/python3.7"
    os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/bin/python3.7"
    os.environ["SPARK_CLASSPATH"]='/usr/bin/postgresql-42.2.9.jar'
    s3_ = boto3.resource('s3')
    client = boto3.client('s3')
    bucketName = 'yvonneleoo'
    conf = SparkConf().setAppName('tiktok-music').setMaster('spark://10.0.0.12:7077')
    sc = SparkContext(conf=conf)
    sc.addPyFile('text_processor.py')
    sc.addPyFile('posgresql.py')

    sqlContext = SQLContext(sc)
    spark = SparkSession(sc).builder.appName('tiktok-music').getOrCreate()
    ## spark.conf.set("spark.sql.execution.arrow.enabled", "true") 

    # read from s3 and flatten the multiindex column names
    obj_track = client.get_object(Bucket=bucketName, Key='tiktok-music/fma_metadata/tracks.csv')
    df = pd.read_csv(obj_track['Body'], header=[0, 1], skipinitialspace=True).iloc[1:,:]
    df.columns.set_levels(['track_id', 'album', 'artist', 'set', 'track'],level=0,inplace=True)
    col_1 = df.columns.levels[1].tolist()
    col_1[0] = 'track_id'
    df.columns.set_levels(col_1, level=1, inplace=True)

    # clean the text info and capture the first genre for each song
    ctn = CleanTrackName()   

    ## track
    df_track = df.loc[:, df.columns.get_level_values(0).isin({'track_id','track'})]
    df_track.columns = df_track.columns.droplevel()
    track = spark.createDataFrame(df_track.astype(str))
     assert " " not in ''.join(track.columns) 
    track = track.withColumn('genre_1', regexp_extract(col('genres'), '(\[)(\w+)(.+)', 2)) # flatten genre info and  capture the first genre
    track = ctn.remove_noise(track, 'title')

    ## artist
    df_artist = df.loc[:, df.columns.get_level_values(0).isin({'track_id','artist'})]
    df_artist.columns = df_artist.columns.droplevel()
    artist = spark.createDataFrame(df_artist.astype(str))
    assert " " not in ''.join(artist.columns)
    artist = ctn.remove_noise(artist, 'name')

    ## genres
    obj_genres = client.get_object(Bucket=bucketName, Key='tiktok-music/fma_metadata/genres.csv')
    df_genres = pd.read_csv(obj_genres['Body'])
    genres = spark.createDataFrame(df_genres.astype(str))
    ## clean info
   
    clean_info = track.join(artist,track.track_id == artist.track_id, how = 'inner')\
                      .select(track['track_id'],track['title'].alias('song_title'),artist['name'].alias("artist_name"),track['genre_1'],track['original_title'].alias('original$
                      #.withColumn('name_key_2', substring(artist['name'],0,1))
    clean_info = clean_info.join(genres, clean_info.genre_1 == genres.genre_id,how = 'left')\
                           .select(clean_info['*'],genres['title'].alias('subgenre_name'),genres['top_level']).sort(col('song_title'))

    clean_info = clean_info.dropDuplicates(['song_title', 'artist_name'])
    clean_info = ctn.add_name_key(clean_info, which='music')
                         
    clean_info.show() 
    print(clean_info.count())
    # calculate the TFIDF score vectors for each song title and artist name    
    text_df = clean_info.select('track_id','song_title', 'artist_name', 'name_key_1')
    text_df.show()

    # save to the postgre
    pc = PosgreConnector(sqlContext)
    pc.write_to_db(track, 'meta_data_song')
    pc.write_to_db(artist, 'meta_data_artist')
    pc.write_to_db(genres, 'meta_data_genres')
    pc.write_to_db(clean_info, 'music_clean_info')
    text_df.repartition("name_key_1").write\
           .partitionBy("name_key_1")\
           .option("header","false")\
           .parquet(path="s3a://yvonneleoo/music-name/", mode="append")    

    
