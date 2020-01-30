# download the fma mudic data
curl -O https://os.unil.cloud.switch.ch/fma/fma_large.zip;
unzip fma_large.zip;
s3cmd put -vr fma_metalarge s3://yvonneleoo/tiktok-music/; 

# download the tiktok data
wget 'https://files.pushshift.io/tiktok/tiktok_25000000_sample.ndjson.zst';
zstd -d tiktok_25000000_sample.ndjson.zst;
s3cmd put -vr tiktok_25000000_sample.ndjson s3://yvonneleoo/tiktok-music/; 

# download the fma meta data
curl -O https://os.unil.cloud.switch.ch/fma/fma_metadata.zip;
unzip fma_metadata.zip;
s3cmd put -vr fma_metadata s3://yvonneleoo/tiktok-music/; 
