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

from text_processor import CleanTrackName, CalTextSimilarity
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
    #conf.set("spark.sql.caseSensitive", "true")

    sc.addPyFile('text_processor.py')
    sc.addPyFile('posgresql.py')

    # loop throught each first letter
    name_key_list = [x for x in list(string.ascii_lowercase) if x != 'a'] + ['others'] 
    
    for name_key in name_key_list:

        try:
            path_1  = 's3a://yvonneleoo/music-name/name_key_1={}'.format(name_key)
            path_2 = 's3a://yvonneleoo/tiktok-name/name_key_1={}'.format(name_key)
    
            df1 = spark.read.load(path_1).persist() # music
            df2 = spark.read.load(path_2).persist() # tiktok
           
            # calculate similarity pairs
            cts = CalTextSimilarity()
            dataCombined = cts.cal_tfidf(df1, df2)
            dataCombined = dataCombined.persist()
            lookupTable = cts.generate_lookup_table(sc, dataCombined)
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
            measureMapping = measureMapping.persist()    
            
            w = Window().partitionBy("music_track_id")\
                        .orderBy(f.col("total_simi").desc())
            df = measureMapping.withColumn("rn", row_number().over(w))\
                               .where(col("rn") == 1)\
                               .drop('rn')\
                               .persist()
            #df.show()
            print(name_key, df.first())

            # write the table to posgresql
            table_name = 'name_pair_{}'.format(name_key)
            pc = PosgreConnector(sqlContext)
            pc.write_to_db(df, table_name)

        except Exception as e:
            print('{}: {}'.format(name_key, repr(e)))
