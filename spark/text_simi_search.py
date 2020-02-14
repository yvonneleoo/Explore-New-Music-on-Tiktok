import os
import pandas as pd
import numpy as np
import math
import string

import pyspark
import boto3
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrameReader, SQLContext
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql import Row
from pyspark.sql import Window

from text_processor import CleanMusicInfo, CalTextSimilarity
from posgresql import PosgreConnector

if __name__ == '__main__':
    
    # set up coding environment and spark config
    os.environ["PYSPARK_PYTHON"]="/usr/bin/python3.7"
    os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/bin/python3.7"
    os.environ["SPARK_CLASSPATH"]='/usr/bin/postgresql-42.2.9.jar'
    ## spark 
    spark_hn = os.environ["SPARK_HN"]
    conf = SparkConf()\
           .setAppName('tiktok-music')\
           .setMaster('spark://{}:7077'.format(spark_hn))\
           .set("spark.executor.instances",4)\
           .set("spark.executor.cores",5)\
           .set("spark.executor.memory","6g")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    spark = SparkSession(sc).builder\
                            .appName('tiktok-music')\
                            .getOrCreate()
    # conf.set("spark.sql.caseSensitive", "true")
    sc.addPyFile('text_processor.py')
    sc.addPyFile('posgresql.py')
    
    cmi = CleanMusicInfo(spark)
    
    # loop throught each first letter
    name_key_list = [x for x in list(string.ascii_lowercase) if x != 'a'] + ['others'] 
    for name_key in name_key_list:
        try:
            # read music info (id, title, artist) from parquet
            df1 = cmi.read_parquet_s3(df, 
                                      route='music-name',
                                      partition='name_key_1', 
                                      partition_key='name_key')
            # read tiktok info (id, title, artist) from parquet
            df2 = cmi.read_parquet_s3(df, 
                                      route='tiktok-name', 
                                      partition='name_key_1', 
                                      partition_key='name_key')
            df1 = df1.persist() 
            df2 = df2.persist() 
           
            # calculate similarity pairs
            cts = CalTextSimilarity(sc)
            measureMapping = cts.cal_text_simi(df1, df2)
            measureMapping = measureMapping.persist()    
            
            # for each music record in the fma dataset, find the tiktok music record that has the highest similarity score
            w = Window().partitionBy("music_track_id")\
                        .orderBy(f.col("total_simi").desc())
            df = measureMapping.withColumn("rn", row_number().over(w))\
                               .where(col("rn") == 1)\
                               .drop('rn')\
                               .persist()

            # write the table to posgresql
            table_name = 'name_pair_{}'.format(name_key)
            pc = PosgreConnector(sqlContext)
            pc.write_to_db(df, table_name)

        except Exception as e:
            print('{}: {}'.format(name_key, repr(e)))
