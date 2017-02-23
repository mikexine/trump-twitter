#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import arrow
from models import Tweet, db_connect, create_db_session, create_tables
import config
import zipfile
from threading import Thread
from urllib.request import urlopen

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

import ssl
import os

db_eng = db_connect()
create_tables(db_eng)

def download(account):
    # with zipfile.ZipFile(account + ".zip", "r") as zip_ref:
    #     zip_ref.extractall(".")
    url = config.giturl.replace("NAME", account)
    print(url)

    # download file
    context = ssl._create_unverified_context()
    request = urlopen(url, context=context)

    # save
    output = open(account + ".zip", 'wb')
    output.write(request.read())
    output.close()

    filename = "%s/%s.zip" % (os.getcwd(), account)
    with zipfile.ZipFile(filename) as z:
        z.extractall(".")


def parse(account):
    db_engine = db_connect()
    db_session = create_db_session(db_engine)
    with open(account + '_long.json', 'r') as content_file:
        content = json.loads(content_file.read())
        print("%s has %s tweets" % (account, len(content)))
        cnt = 0
        for doc in content:

            TweetId = doc.get('id')
            TweetDate = arrow.get(doc.get('created_at'), "ddd MMM DD HH:mm:ss Z YYYY").format('YYYY-MM-DD HH:mm:ss ZZ')
            Text = doc.get("text")
            RetweetCount = doc.get("retweet_count")
            FavoriteCount = doc.get("favorite_count")

            Retweeted = None
            OriginalTweetId = None
            OriginalTweetUserId = None
            OriginalTweetUserName = None
            OriginalTweetUserScreenName = None
            OriginalTweetDate = None

            if doc.get("retweeted_status") is not None:
                Retweeted = True
                OriginalTweetId = doc.get("retweeted_status").get('id')
                OriginalTweetUserId = doc.get("retweeted_status").get('user').get('id')
                OriginalTweetUserName = doc.get("retweeted_status").get('user').get('name')
                OriginalTweetUserScreenName = doc.get("retweeted_status").get('user').get('screen_name')
                OriginalTweetDate = arrow.get(doc.get("retweeted_status").get('created_at'), "ddd MMM DD HH:mm:ss Z YYYY").format('YYYY-MM-DD HH:mm:ss ZZ')

            Favorited = doc.get("favorited")
            TweetLang = doc.get('lang')
            Source = doc.get('source', "").partition('>')[-1].rpartition('<')[0]

            Mentions = []
            if doc.get('entities').get('user_mentions'):
                for m in doc.get('entities').get('user_mentions'):
                    Mentions.append(m.get('screen_name'))

            Hashtags = []
            if doc.get('entities').get('hashtags'):
                for m in doc.get('entities').get('hashtags'):
                    Hashtags.append(m.get('text'))

            Urls = []
            if doc.get('entities').get('urls'):
                for m in doc.get('entities').get('urls'):
                    Urls.append(m.get('expanded_url'))

            UserId = doc.get('user').get('id')
            UserName = doc.get('user').get('name')
            UserScreenName = doc.get('user').get('screen_name')
            UserCreatedDate = arrow.get(doc.get('user').get('created_at'), "ddd MMM DD HH:mm:ss Z YYYY").format('YYYY-MM-DD HH:mm:ss ZZ')
            UserLang = doc.get('user').get('lang')
            UserLocation = doc.get('user').get('location')

            UserTimeZone = doc.get('user').get('time_zone')
            UserUTCOffset = doc.get('user').get('utc_offset')

            UserFollowerCount = doc.get('user').get('followers_count')
            UserFriendsCount = doc.get('user').get('friends_count')
            UserFavoritesCount = doc.get('user').get('statuses_count')
            UserStatusesCount = doc.get('user').get('favourites_count')

            tweet = Tweet(TweetId=TweetId,
                          TweetDate=TweetDate,
                          Text=Text,
                          RetweetCount=RetweetCount,
                          FavoriteCount=FavoriteCount,
                          Retweeted=Retweeted,
                          Favorited=Favorited,
                          TweetLang=TweetLang,
                          Source=Source,
                          Mentions=Mentions,
                          Hashtags=Hashtags,
                          Urls=Urls,
                          OriginalTweetId=OriginalTweetId,
                          OriginalTweetUserId=OriginalTweetUserId,
                          OriginalTweetUserName=OriginalTweetUserName,
                          OriginalTweetUserScreenName=OriginalTweetUserScreenName,
                          OriginalTweetDate=OriginalTweetDate,
                          UserId=UserId,
                          UserName=UserName,
                          UserScreenName=UserScreenName,
                          UserCreatedDate=UserCreatedDate,
                          UserLang=UserLang,
                          UserLocation=UserLocation,
                          UserTimeZone=UserTimeZone,
                          UserUTCOffset=UserUTCOffset,
                          UserFollowerCount=UserFollowerCount,
                          UserFriendsCount=UserFriendsCount,
                          UserFavoritesCount=UserFavoritesCount,
                          UserStatusesCount=UserStatusesCount)

            db_session.add(tweet)
            cnt += 1

            if not cnt % config.commitnumber:
                db_session.commit()
                print("Committed %s rows from account %s" % (cnt, account))
                if config.isDev:
                    break
        db_session.commit()


def main():
    accounts = config.accounts
    downloads = []
    parsers = []

    # for account in accounts:
    #     td = threading.Thread(target=download, args=(account,), kwargs={})
    #     downloads.append(td)
    #     tp = threading.Thread(target=parse, args=(account,), kwargs={})
    #     downloads.append(tp)

    downloads = [Thread(target=download, args=(account,)) for account in accounts]
    [d.start() for d in downloads]
    [d.join() for d in downloads]


    parsers = [Thread(target=parse, args=(account,)) for account in accounts]
    [p.start() for p in parsers]
    [p.join() for p in parsers]

    # for account in accounts:
    #     parse(account)

    # for p in parsers:
    #     p.start()

    # for p in parsers:
    #     p.join()


if __name__ == '__main__':
    main()
