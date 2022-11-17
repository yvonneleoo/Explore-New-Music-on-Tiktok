from pyspark import SparkContext, SparkConf
import boto3
from pyspark.sql.session import SparkSession
from pyspark.sql.types import FloatType
import time

import sys
sys.path.append("../utils")
sys.path.append("../processor")
from posgresql import PosgreConnector
from music_processor import Vectorization

if __name__ == '__main__':   
    # set up coding environment and connection to s3
    os.environ["PYSPARK_PYTHON"]="/usr/bin/python3.7"
    os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/bin/python3.7"
    os.environ["SPARK_CLASSPATH"]='/usr/bin/postgresql-42.2.9.jar'
    ## s3
    spark_hn = os.environ["SPARK_HN"]
    s3_ = boto3.resource('s3')
    client = boto3.client('s3')
    bucketName = os.environ["Bucket_Name"]
    ## spark config
    conf = SparkConf().setAppName('tiktok-music').setMaster('spark:{}//:7077'.format(spark_hn))
    sc = SparkContext(conf=conf)
    sc.addPyFile('../processor/music_processor.py')
    sc.addPyFile('../utils/posgresql.py')
    sqlContext = SQLContext(sc)
    spark = SparkSession(sc).builder\
                            .appName('tiktok-music')\
                            .getOrCreate()
    
    # initialization
    vect = Vectorization(client = client)
    col = ['track_id'] + ['feature_' + str(i) for i in range(245)]
    total_record = 106574
    count = 0
    lst = []

    # convert all the music files
    for obj in s3_.Bucket('yvonneleoo').objects.all():

        key = obj.key
    
        if '.mp3' in key:
    	    track_id = int(key.split('/')[3].split('.mp3')[0]) # song track_id                       
            count +=1
            print(count, track_id, key)

            try:
                vect.convert(bucketName, key, track_id, lst) 
            except Exception as e:
                print('{}: {}'.format(track_id, repr(e)))
            
            if count % 10000 == 0 or count == total_record:  
                try:
                    start_time = time.time()
                    df = spark.createDataFrame(lst, col) # into df
                    df.coalesce(1).write\
                      .option("header","false")\
                      .parquet(path="s3a://yvonneleoo/music-vector/", mode="append")
                    duration = round(time.time() - start_time, 4)
                    print(f"save file in {duration} seconds")   
                    lst = [] 
                except Exception as e:
                    print('{}: {}'.format(track_id, repr(e)))
                    
     ## store by genres
     ## from s3 to postgres. if I were to do this project again, I would store the vectors directly to posgres in the former steps.
     
     pc = PosgreConnector(sqlContext)
     music_info = pc.read_from_db('clean_music_info')
     top_level = [a[0] for a in np.array(music_info.select('top_level').distinct().collect())]
     columns = [f.col('feature_' + str(i)) for i in range(245)]
     
     music_vect = spark.read.load("s3a://yvonneleoo/music-vector/")
     for genre in top_level:
       
         if genre:
             id_list = [a[0] for a in music_info.filter(col('top_level')==int(genre))\
                                                .select('track_id').collect()[0].track_id]
             df = music_vect.filter(music_vect.track_id.isin(id_list))\
                            .withColumn('genre', lit(genre))\
                            .withColumn('features', f.array(columns))\
                            .select('track_id', 'genre', 'features')\
                            .persist()
            pc.write_to_db(df,'public.music-vector-by-genres_genre={}'.format(genre))
                            
         else:
             id_list = [a[0] for a in music_info.filter(col('top_level')== genre)\
                                                .select('track_id').collect()[0].track_id]
             df = music_vect.filter(music_vect.track_id.isin(id_list))\
                            .withColumn('genre', lit('None'))\
                            .withColumn('features', f.array(columns))\
                            .select('track_id', 'genre', 'features')\
                            .persist()
                            
            pc.write_to_db(df,'public.music-vector-by-genres_genre={}'.format(genre))
          
