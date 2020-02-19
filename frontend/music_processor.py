import os
import numpy as np
import pandas as pd
import librosa
import warnings
from scipy import stats
import json
import time
import pyspark
import boto3
#import faiss
from compute_features import ComputeFeatures
#import pyarrow
from postgresql import PosgreConnector

class SimilaritySearch():
    def __init__(self):
        self.path = 'public.music-vector-by-genres_genre='
        
    def get_vect_df(self, top_genre_id, engine):
        music_vect = pd.read_sql("SELECT * FROM {}{}".format(self.path, top_genre_id), engine)
        return music_vect    

    def cal_index(self, new_df, vec_df):
        vec = new_df.features[0]
        max_df = vec_df['features']\
                 .apply(lambda x: (np.array(x) - np.array(vec))**2)\
                 .apply(lambda x: sum(x))
        max_df = pd.DataFrame(max_df)
        index = max_df.sort_values('features', ascending = True)\
                 .head(5)\
                 .index

        return index

class MusicProcessor(ComputeFeatures):
    def __init__(self, base_dir, filename):
        super().__init__() 
        self.filename = filename
        self.path = os.path.join(base_dir, 'static', 'music', self.filename)

    def music_vectorization(self, spark):                     
        track_id = str(int(time.time() + int(self.filename.split('.mp3')[0])))
        vect = self.compute_features(self.path)
        df = pd.DataFrame({"track_id":track_id, "features":[vect]})
        return track_id, df, vect
