from tweepy import OAuthHandler, API
import config
import logging

auth = OAuthHandler(config.consumer_key, config.consumer_secret)
auth.set_access_token(config.access_token, config.access_secret)
api = API(auth, wait_on_rate_limit=True,
          wait_on_rate_limit_notify=True)

template = {}
for i in config.update.get("what"):
    template[i] = ""


def UpdateTweet(data):
    chunks = [data[x:x + 100] for x in range(0, len(data), 100)]
    logging.warning("%s chunks with 100 tweets each" % len(chunks))
    tweets = []
    cnt = 1
    for c in chunks:
        logging.warning("%s - looking up a tweet chunk...." % cnt)
        new = api.statuses_lookup(c)
        tweets += new
        cnt += 1
    return tweets
