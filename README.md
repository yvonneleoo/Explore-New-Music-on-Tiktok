# Explore-New-Music-on-Tiktok
> ***picture your audience in the streaming era***

This is a project I completed during the Insight Data Engineering program (New York, 2020 A).

Streaming service has given artists more right to connect to their audience directly.  For a musician, TikTok, a video-sharing social networking app, is a platform with opportunity, since music and video are naturally connected.  As a matter of fact, several heat songs (say Old Town Road) have become popular because of TikTok. 

But how could artists know what content could match their music, from audiences' aspect? On TikTok, you could not search by similar music; and on music similarity searching platform, you could not see the potential application. I built an app to try to match these two points. 

By uploading a music file, you could search the most similar music to yours, and further search the most likely applications on TikTok.  Visit [dataeng.today](http://dataeng.today) to explore it. 

Pipeline
-----------------
Python/jq -> S3 -> Spark -> PostgresSQL -> Flask

![alt text](https://github.com/yvonneleoo/Promote-Music-on-Tiktok/blob/develop/docs/pipeline.png " Pipeline")

### Music Batch Pipeline:
- The python files transformed the FMA mp3 data to vectors in parquet format with audio feature extracted (245 dimensions for 5 features): 
'mfcc(human voice), chromagram(pitch), zero-crossing-rate(rhythm), spectral centroid and spectral roll-off frequency (composition style)'


### Text Batch Pipeline:
- 'TikTok data': Used jq command to parse the TikTok data in nested ndjson format, and select out the following columns:
video_id, video_url, covers, track_id, song_title, artist_name, music_url
Stored the clean TikTok data in PostgresSQL, partitioning by the first letter of the song title.
- 'Music meta data': The python files processed FMA music meta data,  joined the track table and genre table, reducing 161 sub-genres to 16 top-genres. Partitioned the track id by top-genres and by the first letter of song titles, and store in the PostgresSQL. 
-  Built a bag-of-words-TFIDF pipeline on Spark, for each music record (title & artist) in the FMA music dataset, search the most similar record (title & artist) in the TikTok dataset (only search within records with the same first letter in title), based on cosine similarity.  Stored the index pairs and similarity scores into PostgreSQL:
'fma_track_id, tiktok_track_id, title_score, artist_socre, total_score'
  
### Frontend workflow:
- User select a genre, and upload a music file ('mp3' or 'wav' format).
- Vectorize the music file and load vectors for music in the same genre.
- Performed vector-based similiarity search, and return list of indexes ('fma_track_id').
- Query by 'fma_track_id', return the music info data of the similar music; 
  query by 'fma_track_id', return the 'tiktok_track_id' of most likely record on TikTok database based on song title and artist name;
  query by 'tiktok_track_id', return tiktok music info, and cover photos of videos that may used the similar music. 

Environment
-----------------
- 3 AWS EC2 clusters. Spark cluster with 5 m4.large nodes; Posgres cluster with 1 m4.large node; Flask cluster with 1 t2.micro node.
- Please read the 'requirements.txt' for the required dependencies.

Limitations and To Do
-----------------
1. Currently, the overlap between the FMA music dataset and TikTok dataset is not big enough to guarantee high accuracy of matching two datasets by song title/artist name.  

The next step would be:
- Added functions to update the music and TikTok dataset automatically;

2. Currently, I pre-filter the music similarity search by genre, extracting features based on references on music genre classfication, and use euclidean distance recommender system, which may be not accurate in our task. 

The next step would be testing out other feature extraction, and other loss functions.

Reference
-----------------
- [FMA dataset (106,574 tracks of 30s mp3 data collected by 2017-05)](https://github.com/mdeff/fma)
- [TikTok dataset (25,000,000 record samples collected by 2019-12)](https://files.pushshift.io/tiktok/)
