#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import arrow
from models import Tweet, db_connect, create_db_session, create_tables, FakeNews
import config
import zipfile
from threading import Thread
from urllib.request import urlopen
import logging
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

import ssl
import os

import requests
from time import sleep


def get_real_url(url):
    s = requests.Session()
    s.headers['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131'
    logging.debug("Getting %s ..." % url)
    response = s.get(url)
    try:
        if response.history:
            # logging.debug("Request was redirected")
            for resp in response.history:
                pass
                # logging.debug(resp.status_code, resp.url)
            # logging.debug("Final destination:")
            # logging.debug(response.url)
            return response.url
        else:
            pass
            # logging.debug("Request was not redirected")
            return url
    except:
        return url



logger = logging.getLogger('fakenews')
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

logger.addHandler(ch)

db_eng = db_connect()
db_session = create_db_session(db_eng)

#  and 

tweets = db_session.query(Tweet).filter(Tweet.UserScreenName == "realDonaldTrump").all()

count = 0 
for t in tweets:
    print(count, t.TweetId)
    count += 1
    if t.Urls:
        for url in t.Urls:
            tweetid = t.TweetId
            fullurl = get_real_url(url)
            website = fullurl.replace("https://", "").replace("http://", "").replace("www.", "").split("/")[0]
            fake = FakeNews(tweetid=tweetid, fullurl=fullurl, website=website, rawurl=url)
            print("%s - %s - %s - %s" % (tweetid, fullurl, website, url))
            db_session.add(fake)
            db_session.commit()

