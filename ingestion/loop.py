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

    # Catch warnings as exceptions (audioread leaks file descriptors).
    # warnings.filterwarnings('error', module='librosa')

    def feature_stats(name, values):
        features[name, 'mean'] = np.mean(values, axis=1)
        features[name, 'std'] = np.std(values, axis=1)
        features[name, 'skew'] = stats.skew(values, axis=1)
        features[name, 'kurtosis'] = stats.kurtosis(values, axis=1)
        features[name, 'median'] = np.median(values, axis=1)
        features[name, 'min'] = np.min(values, axis=1)
        features[name, 'max'] = np.max(values, axis=1)

    try:
        #filepath = utils.get_audio_path(os.environ.get(AUDIO_DIR))
        x, sr = librosa.load(AUDIO_DIR, sr=None, mono=True)  # kaiser_fast

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

    # loop over music files from the bucket, vectorization 
    genres_info = pd.read_csv('./fma_metadata/genres.csv')
       
    count = 0

    col = ['track_id', 'genre'] + ['feature_' + str(i) for i in range(245)]
     
    for obj in s3_.Bucket('yvonneleoo').objects.all():

        key = obj.key
        if 'tiktok-music' in key:
 
            temp = key.split('/')            
            track_id, genre = int(temp[3].split('.mp3')[0]), temp[2] # song info           
            
            if count == 0:
                top_genre = str(genres_info.top_level[genres_info.genre_id == 4].iloc[0]) 
 
            try:
                ## download to ec2 master first 
                local_path = './music/' + temp[3]
                with open(local_path,'wb') as data:
                    client.download_file(bucketName, key, local_path)
              
                ## vectorizatin and store into df format
                try:
                    lst = [tuple([track_id, top_genre] + compute_features(local_path))]
                except:
                   continue
                count += 1 
               
                df = spark.createDataFrame(lst, col) # into df
                df.coalesce(1).write.partitionBy('genre').option("header","false").parquet(path="s3a://yvonneleoo/music-vector/", mode="append")
                   
            except Exception as e:
                print('{}: {}'.format(track_id, repr(e)))


    

