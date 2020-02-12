import os
from flask import Flask, flash, request, redirect, url_for, send_from_directory, render_template, session
from flask_session import Session
from werkzeug.utils import secure_filename
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy import create_engine
import boto3
import faiss

import numpy as np
import pandas as pd
import re
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from music_processor import SimilaritySearch, MusicProcessor

# set up coding environment
client = boto3.client('s3')
os.environ["PYSPARK_PYTHON"]="/usr/bin/python3.7"
os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/bin/python3.7"
conf = SparkConf().setAppName('tiktok-music')\
                  .setMaster('spark://10.0.0.14:7077')
sc = SparkContext(conf=conf)
spark = SparkSession(sc).builder.appName('tiktok-music').getOrCreate()

ss = SimilaritySearch()
base_dir = os.path.abspath(os.path.dirname(__file__))
UPLOAD_FOLDER = './static/music'
ALLOWED_EXTENSIONS = {'wav', 'mp3'} ## only allow users to upload wav and mp3 files

pwd = os.environ['POSTGRES_PWD']
engine = create_engine('postgresql://yvonneleoo:%s@10.0.0.8:5432/music_tiktok'%pwd) 

## genre info
genre_df = pd.read_sql("SELECT genre_id, title FROM meta_data_genres\
                        WHERE genre_id in (SELECT DISTINCT top_level FROM meta_data_genres)", engine)
top_genre_id = genre_df['genre_id'].tolist()                                                                        
top_genre = genre_df['title'].tolist()

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config["SESSION_PERMANENT"] = False
app.config["SESSION_TYPE"] = "filesystem"
Session(app)

## home page
@app.route('/')
def welcome_page():
    return render_template("page.html")

## genre page
@app.route('/genre', methods=['GET', 'POST'])
def select_genre():
    genre_list = top_genre 
    if request.method == 'POST':
        genre = request.form['genre']
        flash(str(genre))
        return redirect(url_for('upload_file',genre=genre)) 
    return render_template("genre.html", genre_list=genre_list)

## upload page
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    genre = request.args['genre']
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join(base_dir, app.config['UPLOAD_FOLDER'], filename))
            return redirect(url_for('song_result',
                                    filename=filename, genre=genre))            
    return render_template("upload.html")
    
## return result of semilar songs
@app.route('/song_result', methods=['GET','POST'])
def song_result():
    # read indexes from the genre
    genre = request.args['genre']
    genre_id = str(top_genre_id[top_genre.index(genre)])
    # vectorization 
    filename = request.args['filename']
    mp = MusicProcessor(base_dir, filename) 
    track_id, new_df, vect = mp.music_vectorization(spark)
    # search based on the vector
    vect_df = ss.get_vect_df(genre_id, spark)
    index = ss.write_to_faiss(new_df, vect_df)
    vector = np.array(vect, dtype=np.float32)
    vector = np.expand_dims(vector, axis=0)
    dists, ids = index.search(vector, 6)
    dists = dists[0, :]
    ids = ["'m%s'"%str(id) for id in ids[0, :]]
    # get the song info for the most similar song
    song_info_query = "SELECT track_id, original_song_title, original_artist_name \
                       FROM music_clean_info\
                       WHERE track_id in"
    ids_format = "{}," * 6
    song_info_query += '(' + ids_format[:-1] + ')'
    song_query = song_info_query.format(*ids)
    song_df = pd.read_sql(song_query, engine)
    song_df['track_id'] = song_df['track_id'].apply(lambda x: '('+x+')')
    song_df.columns = ['track id', 'song title', 'artist name']
    session['data'] = song_df.to_json()
    return render_template("song_result.html", tables=[song_df.to_html(classes='data')], titles='')

## return the result of possible tiktok application 
@app.route('/tiktok', methods=['GET','POST'])
def get_tiktok_info():
    target = request.args['target']
    id = re.search('\(([^)]+)', target).group(1)
    letter = target.split(')')[1][0].lower() 
    return redirect(url_for('get_tiktok_data', id=id , letter=letter))  

@app.route('/tiktok_result', methods=['GET','POST'])
def get_tiktok_data():
    id = request.args['id']
    letter = request.args['letter']
    namepair_df_name = 'name_pair_%s' % letter
    tiktok_id_query = "SELECT * FROM public." + namepair_df_name  + " WHERE music_track_id = '" + id + "'"
    similarity_df = pd.read_sql(tiktok_id_query, engine)
    artist_score, tiktok_id, title_score, total_score = similarity_df.iloc[0,0], similarity_df.iloc[0,2], similarity_df.iloc[0,3], similarity_df.iloc[0,4]  
    tiktok_df_name = 'tiktok_info_%s' % letter
    tiktok_info_query ="SELECT * FROM public." + tiktok_df_name + " WHERE track_id = '" + tiktok_id + "'" 
    tiktok_df =  pd.read_sql(tiktok_info_query, engine)
    song_title, artist_name = tiktok_df.iloc[0, 7], tiktok_df.iloc[0, 8]
    dic = {}
    for i in range(tiktok_df.shape[0]):
        video_url, covers = tiktok_df.iloc[i, 1], tiktok_df.iloc[i, 2]
        print(video_url, covers)
        dic[video_url] =  covers
    return render_template("tiktok_result.html", song_title = song_title, artist_name = artist_name, 
                                                 artist_score=artist_score, tiktok_id=tiktok_id, 
                                                 title_score=title_score, results = dic)


if __name__ == '__main__':
    app.secret_key = os.environ['FLASK_SECRET_KEY']
    #app.run(host='0.0.0.0', debug=True)
    app.run(debug=True, host = '0.0.0.0', threaded = True, port='80')
 
