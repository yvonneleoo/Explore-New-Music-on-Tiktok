
# download the tiktok data
wget 'https://files.pushshift.io/tiktok/tiktok_25000000_sample.ndjson.zst';
zstd -d tiktok_25000000_sample.ndjson.zst;
s3cmd put -vr tiktok_25000000_sample.ndjson s3://yvonneleoo/tiktok-music/; 

# download the fma data
wget 'https://os.unil.cloud.switch.ch/fma/fma_full.zip';
unzip fma_full.zip;
s3cmd put -vr fma_full s3://yvonneleoo/tiktok-music/; 
