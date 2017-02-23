cd /home/twitter/trump-twitter
source env/bin/activate
curl -o realdonaldtrump.zip https://raw.githubusercontent.com/bpb27/political_twitter_archive/master/realdonaldtrump/realdonaldtrump_long.zip
python load_archive.py