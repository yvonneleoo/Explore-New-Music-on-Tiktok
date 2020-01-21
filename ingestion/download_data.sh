# download the fma mudic data
curl -O https://os.unil.cloud.switch.ch/fma/fma_large.zip echo "497109f4dd721066b5ce5e5f250ec604dc78939e  fma_large.zip" | sha1sum -c - unzip fma_large.zip;
s3cmd put -vr fma_large s3://yvonneleoo/tiktok-music/; 

# download the tiktok data
wget 'https://files.pushshift.io/tiktok/tiktok_25000000_sample.ndjson.zst';
zstd -d tiktok_25000000_sample.ndjson.zst;
s3cmd put -vr tiktok_25000000_sample.ndjson s3://yvonneleoo/tiktok-music/; 
