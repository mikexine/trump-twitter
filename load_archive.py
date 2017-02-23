#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import arrow
from models import Tweet, db_connect, create_db_session, create_tables
import config
import zipfile


with zipfile.ZipFile("realdonaldtrump.zip", "r") as zip_ref:
    zip_ref.extractall(".")

db_engine = db_connect()
create_tables(db_engine)
db_session = create_db_session(db_engine)


def main():
    with open('realdonaldtrump_long.json', 'r') as content_file:
        content = json.loads(content_file.read())
        print(len(content))
        cnt = 0
        for doc in content:
            print("Tweet number: %s " % cnt)
            TweetId = doc.get('id')
            TweetDate = arrow.get(doc.get('created_at'), "ddd MMM DD HH:mm:ss Z YYYY").format('YYYY-MM-DD HH:mm:ss ZZ')
            Text = doc.get("text")
            RetweetCount = doc.get("retweet_count")
            FavoriteCount = doc.get("favorite_count")
            Retweeted = doc.get("retweeted")
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

            OriginalTweetId = None
            OriginalTweetUserId = None
            OriginalTweetDate = None

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

            db_session.merge(tweet)
            cnt += 1

            if not cnt % 5000:
                db_session.commit()
                print("Committed 100 rows")
        db_session.commit()

if __name__ == '__main__':
    main()
