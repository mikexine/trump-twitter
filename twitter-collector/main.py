from utils import UpdateTweet
from models import Tweet, db_connect, create_db_session
from config import logger, update
import logging


logging.basicConfig(level=logger.get("level"),
                    format=logger.get("format"))


db_session = create_db_session(db_connect())

size = update.get("size")
rows = db_session.query(Tweet).filter(Tweet.UserScreenName == "realDonaldTrump").order_by(Tweet.TweetDate.desc()).limit(size)
ids = [r.TweetId for r in rows]

tweets = UpdateTweet(ids)

cnt = 1
for t in tweets:
    retweets = t.retweet_count
    favorites = t.favorite_count
    if hasattr(t, 'retweeted_status'):
        favorites = t.retweeted_status.favorite_count

    old = db_session.query(Tweet).filter(Tweet.TweetId == t.id).all()[0]
    msg = "%s - ID: %s | oR: %6s, oF: %6s | nR: %6s, nF: %6s |" % (
        cnt, t.id, old.RetweetCount, old.FavoriteCount,
        retweets, favorites)
    logging.warning(msg)

    old.RetweetCount = retweets
    old.FavoriteCount = favorites
    cnt += 1

db_session.commit()

# print(db_session.query(Tweet).all())
# UpdateTweet([833838311315763200])
