from pyspark import SparkContext, SparkConf
import boto3
from pyspark.sql.session import SparkSession
from pyspark.sql.types import FloatType
import time
from music_vectorization import vectorization

# set up coding environment and connection to s3
os.environ["PYSPARK_PYTHON"]="/usr/bin/python3.7"
os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/bin/python3.7"

s3_ = boto3.resource('s3')
client = boto3.client('s3')
bucketName = 'yvonneleoo'
conf = SparkConf().setAppName('tiktok-music').setMaster('local')
sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName('tiktok-music').getOrCreate()

# initialization
vect = vectorization(client = client)
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
                df.coalesce(1).write.option("header","false").parquet(path="s3a://yvonneleoo/music-vector/", mode="append")
                duration = round(time.time() - start_time, 4)
                print(f"save file in {duration} seconds")   
                lst = [] 
            except Exception as e:
                print('{}: {}'.format(track_id, repr(e)))
