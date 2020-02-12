# download the fma mudic data to s3
curl -O https://os.unil.cloud.switch.ch/fma/fma_large.zip;
unzip fma_large.zip;
s3cmd put -vr fma_metalarge s3://yvonneleoo/tiktok-music/; 

# download the tiktok data and save to posgres directly
## download and unzip
wget 'https://files.pushshift.io/tiktok/tiktok_25000000_sample.ndjson.zst';
zstd -d tiktok_25000000_sample.ndjson.zst;
## parse the nested ndjson file
echo 'video_id, video_url, video_covers, music_id, music_artist, music_song, music_url' >> tiktok.csv;
while read -r line;
 do echo "$line" | jq '.itemInfos.id,.itemInfos.video.urls[0],.itemInfos.covers[0],.musicInfos.musicId,.musicInfos.authorName,.musicInfos.musicName,.musicInfos.playUrl[0]' | paste -sd, ->> tiktok.csv;
done <  tiktok_25000000_sample.ndjson
## ingest into posgres
PGPASSWORD='hongamo669425' psql -h ec2-34-197-195-174.compute-1.amazonaws.com -U yvonneleoo -d music_tiktok -c "CREATE TABLE tiktok_csv(video_id varchar(1000), video_url varchar(1000), video_covers varchar(1000), music_id varchar(1000), music_artist varchar(1000), music_song varchar(1000), music_url varchar(1000))";
PGPASSWORD='hongamo669425' psql -h ec2-34-197-195-174.compute-1.amazonaws.com -U yvonneleoo -d music_tiktok -c "\copy tiktok_csv(video_id,video_url,video_covers,music_id,music_artist,music_song,music_url) FROM '~/tiktok.csv' DELIMITER ',' CSV";

# download the fma meta data to s3
curl -O https://os.unil.cloud.switch.ch/fma/fma_metadata.zip;
unzip fma_metadata.zip;
s3cmd put -vr fma_metadata s3://yvonneleoo/tiktok-music/; 
