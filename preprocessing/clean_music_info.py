import boto3
import pyspark

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.feature import RegexTokenizer, \
        StopWordsRemover, CountVectorizer, IDF

import os
import pandas as pd
import time

import sys
sys.path.append("../utils")
sys.path.append("../processor")
from posgresql import PosgreConnector
from text_processor import CleanMusicInfo

if __name__ == '__main__':

    # set up coding environment, connection to s3, and spark configure
    os.environ["PYSPARK_PYTHON"]="/usr/bin/python3.7"
    os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/bin/python3.7"
    os.environ["SPARK_CLASSPATH"]='/usr/bin/postgresql-42.2.9.jar'
    
    spark_hn = os.environ["SPARK_HN"] 
    conf = SparkConf().setAppName('tiktok-music')\
            .setMaster('spark:{}//:7077'.format(spark_hn))
    sc = SparkContext(conf=conf)
    sc.addPyFile('../processor/text_processor.py')
    sc.addPyFile('../utils/posgresql.py')
    sqlContext = SQLContext(sc)
    spark = SparkSession(sc).builder\
                            .appName('tiktok-music')\
                            .getOrCreate()
    
    cmi = CleanMusicInfo(spark) 
    
    # read from s3 and flatten the multiindex column names
    df = cmi.read_csv_s3('tiktok-music/fma_metadata/', 'track.csv')
    df = df.iloc[1:,:] ## remove meaningless wrong info due to the format
    df = cmi.flatten_multi_index(df)

    # clean the text info and capture the first genre for each song
    ## track
    track = cmi.clean_sub_df('track', sub_info_type='song')

    ## artist
    artist = cmi.clean_sub_df('track', sub_info_type='artist')

    ## genres
    genres = cmi.clean_sub_df('genres', sub_info_type=None)
    
    ## clean info
    clean_df = cmi.merge_clean_info(track, artist, genres)
                          
    ## select out the info, for title/artist similarity search in later steps; 
    ## set the first letter of song titles as a partition key(name_key_1), to speed up title/artist similarity search in later steps;
    text_df = clean_info.select('track_id','song_title', 'artist_name', 'name_key_1')

    # save to the postgre or s3
    pc = PosgreConnector(sqlContext)
    pc.write_to_db(track, 'meta_data_song')
    pc.write_to_db(artist, 'meta_data_artist')
    pc.write_to_db(genres, 'meta_data_genres')
    pc.write_to_db(clean_info, 'music_clean_info') 
    cmi.write_parquet_s3(text_df, route='music-name', partition='name_key_1')
    
