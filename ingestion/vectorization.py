import os
import librosa
import warnings
from scipy import stats
import json
import time
from pyspark import SparkContext, SparkConf
import boto3
from pyspark.sql.session import SparkSession
from pyspark.sql.types import FloatType
import time
from compute_features import ComputeFeatures

class Vectorization(ComputeFeatures):
	
    def __init__(self, client):
	self.client = client
	super().__init__()
	
    def convert(self, bucketName, key, track_id, lst):
    	## download to ec2 master first 
        local_path = './music/' + key.split('/')[3]
        with open(local_path,'wb') as data:
            self.client.download_file(bucketName, key, local_path)
        ## vectorizatin and store into df format
        start_time = time.time()
        lst.append(tuple([track_id] + self.compute_features(local_path)))
        duration = round(time.time() - start_time, 4)
        print(f"vectorization in {duration} seconds") 
