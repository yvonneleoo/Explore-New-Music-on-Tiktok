from pyspark import SparkContext, SparkConf
import boto3
import os
import numpy as np
import pandas as pd
import librosa
import warnings
from scipy import stats
import json
from pyspark.sql.session import SparkSession
from pyspark.sql.types import FloatType
import time

"""
feature extraction functions
"""

def columns():
    feature_sizes = dict(zcr=1, chroma_stft=12, spectral_centroid=1, spectral_rolloff=1, mfcc=20)
    moments = ('mean', 'std', 'skew', 'kurtosis', 'median', 'min', 'max')

    columns1 = []
    for name, size in feature_sizes.items():
        for moment in moments:
            it = ((name, moment, '{:02d}'.format(i+1)) for i in range(size))
            columns1.extend(it)

    names = ('feature', 'statistics', 'number')
    columns1 = pd.MultiIndex.from_tuples(columns1, names=names)

    # More efficient to slice if indexes are sorted.
    return columns1.sort_values()


def compute_features(AUDIO_DIR):

    features = pd.Series(index=columns(), dtype=np.float32, name=AUDIO_DIR)

    def feature_stats(name, values):
        features[name, 'mean'] = np.mean(values, axis=1)
        features[name, 'std'] = np.std(values, axis=1)
        features[name, 'skew'] = stats.skew(values, axis=1)
        features[name, 'kurtosis'] = stats.kurtosis(values, axis=1)
        features[name, 'median'] = np.median(values, axis=1)
        features[name, 'min'] = np.min(values, axis=1)
        features[name, 'max'] = np.max(values, axis=1)

    try:
        x, sr = librosa.load(AUDIO_DIR, sr=None, mono=True)  

        f = librosa.feature.zero_crossing_rate(x, frame_length=2048, hop_length=512)
        feature_stats('zcr', f)

        cqt = np.abs(librosa.cqt(x, sr=sr, hop_length=512, bins_per_octave=12,
                                 n_bins=7*12, tuning=None))
        assert cqt.shape[0] == 7 * 12
        assert np.ceil(len(x)/512) <= cqt.shape[1] <= np.ceil(len(x)/512)+1

        #f = librosa.feature.chroma_cqt(C=cqt, n_chroma=12, n_octaves=7)
        #feature_stats('chroma_cqt', f)
        #f = librosa.feature.chroma_cens(C=cqt, n_chroma=12, n_octaves=7)
        #feature_stats('chroma_cens', f)
        #f = librosa.feature.tonnetz(chroma=f)
        #feature_stats('tonnetz', f)

        del cqt
        stft = np.abs(librosa.stft(x, n_fft=2048, hop_length=512))
        assert stft.shape[0] == 1 + 2048 // 2
        assert np.ceil(len(x)/512) <= stft.shape[1] <= np.ceil(len(x)/512)+1
        del x

        f = librosa.feature.chroma_stft(S=stft**2, n_chroma=12)
        feature_stats('chroma_stft', f)
        #f = librosa.feature.rmse(S=stft)
        #feature_stats('rmse', f)
        f = librosa.feature.spectral_centroid(S=stft)
        feature_stats('spectral_centroid', f)
        #f = librosa.feature.spectral_bandwidth(S=stft)
        #feature_stats('spectral_bandwidth', f)
        #f = librosa.feature.spectral_contrast(S=stft, n_bands=6)
        #feature_stats('spectral_contrast', f)
        f = librosa.feature.spectral_rolloff(S=stft)
        feature_stats('spectral_rolloff', f)

        mel = librosa.feature.melspectrogram(sr=sr, S=stft**2)
        del stft
        f = librosa.feature.mfcc(S=librosa.power_to_db(mel), n_mfcc=20)
        feature_stats('mfcc', f)

    except Exception as e:
        print('{}: {}'.format(AUDIO_DIR, repr(e)))

    return np.array(features).tolist()

def music_vetorization(bucketName, key, track_id, lst):                     
             
    ## download to ec2 master first 
    local_path = './music/' + key.split('/')[3]
    with open(local_path,'wb') as data:
        client.download_file(bucketName, key, local_path)
              
    ## vectorizatin and store into df format
    start_time = time.time()
    lst.append(tuple([track_id] + compute_features(local_path)))
    duration = round(time.time() - start_time, 4)
    print(f"vectorization in {duration} seconds") 
                  
if __name__ == '__main__':

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
    count = 0
    col = ['track_id'] + ['feature_' + str(i) for i in range(245)]
    total_record = 106574
    lst = []

    # loop over music files from the bucket, vectorization  
    for obj in s3_.Bucket('yvonneleoo').objects.all():

        key = obj.key
        rule = ('.mp3' in key)

        if rule: 
            
            track_id= int(key.split('/')[3].split('.mp3')[0]) # song track_id                       
            count +=1
            print(count, track_id, key)

            try:
                music_vetorization(bucketName, key, track_id, lst) 
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
