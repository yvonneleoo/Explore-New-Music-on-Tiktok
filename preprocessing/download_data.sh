# download the fma music data to s3
curl -O https://os.unil.cloud.switch.ch/fma/fma_large.zip;
unzip fma_large.zip;
s3cmd put -vr fma_metalarge s3://yvonneleoo/tiktok-music/; 

# download the tiktok data, parse it, and save to s3
## download and unzip
wget 'https://files.pushshift.io/tiktok/tiktok_25000000_sample.ndjson.zst';
zstd -d tiktok_25000000_sample.ndjson.zst;
## parse the nested ndjson file
echo 'video_id, video_url, video_covers, music_id, music_artist, music_song, music_url' >> tiktok.csv;
while read -r line;
 do echo "$line" | jq '.itemInfos.id,.itemInfos.video.urls[0],.itemInfos.covers[0],.musicInfos.musicId,.musicInfos.authorName,.musicInfos.musicName,.musicInfos.playUrl[0]' | paste -sd, ->> tiktok.csv;
done <  tiktok_25000000_sample.ndjson
## save it to s3
s3cmd put -v ./tiktok.csv s3://yvonneleoo/tiktok/; 

# download the fma meta data to s3
curl -O https://os.unil.cloud.switch.ch/fma/fma_metadata.zip;
unzip fma_metadata.zip;
s3cmd put -vr fma_metadata s3://yvonneleoo/tiktok-music/; 
