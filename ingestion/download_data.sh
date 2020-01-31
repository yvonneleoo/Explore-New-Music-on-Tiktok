# download the fma mudic data to s3
curl -O https://os.unil.cloud.switch.ch/fma/fma_large.zip;
unzip fma_large.zip;
s3cmd put -vr fma_metalarge s3://yvonneleoo/tiktok-music/; 

# download the tiktok data to prosgresql directly
wget 'https://files.pushshift.io/tiktok/tiktok_25000000_sample.ndjson.zst';
zstd -d tiktok_25000000_sample.ndjson.zst;
psql -h ec2-34-197-195-174.compute-1.amazonaws.com -U postgres -d music_tiktok -c "CREATE TABLE tiktok(data varchar(1000))";
cat tiktok_25000000_sample.ndjson | psql -h ec2-34-197-195-174.compute-1.amazonaws.com -U postgres -d music_tiktok -c "\COPY tiktok (data) FROM STDIN";

# download the fma meta data to s3
curl -O https://os.unil.cloud.switch.ch/fma/fma_metadata.zip;
unzip fma_metadata.zip;
s3cmd put -vr fma_metadata s3://yvonneleoo/tiktok-music/; 
