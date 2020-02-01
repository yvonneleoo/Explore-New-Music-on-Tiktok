import boto3
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql import DataFrameReader, SQLContext
from pyspark.sql.functions import *

import pandas as pd
import os
import time

if __name__ == '__main__':

    # set up coding environment and connection to s3
    os.environ["PYSPARK_PYTHON"]="/usr/bin/python3.7"
    os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/bin/python3.7"
    os.environ["SPARK_CLASSPATH"]='/usr/bin/postgresql-42.2.9.jar'
    s3_ = boto3.resource('s3')
    client = boto3.client('s3')
    bucketName = 'yvonneleoo'
    conf = SparkConf().setAppName('tiktok-music').setMaster('local')
    sc = SparkContext(conf=conf)
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

    ## track
    df_track = df.loc[:, df.columns.get_level_values(0).isin({'track_id','track'})]
    df_track.columns = df_track.columns.droplevel()
    track = spark.createDataFrame(df_track.astype(str))
    assert " " not in ''.join(track.columns) 
    track = track.withColumn('genre_1', regexp_extract(col('genres'), '(\[)(\w+)(.+)',2)) # flatten genre info and  capture the first genre
    track = track.withColumn('song_name', trim(lower(col('title'))))

    ## artist
    df_artist = df.loc[:, df.columns.get_level_values(0).isin({'track_id','artist'})]
    df_artist.columns = df_artist.columns.droplevel()
    artist = spark.createDataFrame(df_artist.astype(str))
    assert " " not in ''.join(artist.columns)
    artist = artist.withColumn('artist_name', trim(lower(col('name'))))

    ## genres
    obj_genres = client.get_object(Bucket=bucketName, Key='tiktok-music/fma_metadata/genres.csv')
    df_genres = pd.read_csv(obj_genres['Body'])
    genres = spark.createDataFrame(df_genres.astype(str))

    ## clean info
    clean_info = track.join(artist,track.track_id == artist.track_id, how = 'left').select(track['track_id'], concat(track['song_name'],lit('_'),artist['artist_name']).alias("track_name"), track['genre_1'], track['title'].alias('original_song_name'), artist['title'].alias('original_artist_name'))
    clean_info = clean_info.join(genres, clean_info.genre_1 == genres.genre_id, how = 'left').select(clean_info['*'], genres['title'].alias('genre_name'), genres['top_level'])
    clean_info.show() 
    
    # save to the postgre
    url = 'jdbc:postgresql://ec2-34-197-195-174.compute-1.amazonaws.com:5432/music_tiktok'
    properties = {'user': 'yvonneleoo', 'password':'hongamo669425','driver':'org.postgresql.Driver'}
    track.write.jdbc(url=url, table = 'public.meta_data_song', mode='append', properties=properties)
    artist.write.jdbc(url=url, table = 'public.meta_data_artist', mode='append', properties=properties) 
    genres.write.jdbc(url=url, table = 'public.meta_data_genres', mode='append', properties=properties)
    clean_info.write.jdbc(url=url, table='public.clean_music_info', mode='append', properties=properties)
    
