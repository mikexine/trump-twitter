from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import config

DeclarativeBase = declarative_base()


def db_connect():
    return create_engine(config.DB_URL)


def create_db_session(engine):
    Session = sessionmaker(bind=engine, autoflush=False)
    session = Session()
    return session


def create_tables(engine):
    DeclarativeBase.metadata.create_all(engine)


class Tweet(DeclarativeBase):
    __tablename__ = "tweets"

    TweetId = Column(BigInteger, primary_key=True)
    TweetDate = Column('tweetDate', DateTime, nullable=True)
    Text = Column('text', String, nullable=True)
    RetweetCount = Column('retweetCount', BigInteger, nullable=True)
    FavoriteCount = Column('favoriteCount', BigInteger, nullable=True)
    Retweeted = Column('retweeted', Boolean, nullable=True)
    Favorited = Column('favorited', Boolean, nullable=True)
    TweetLang = Column('tweetLang', String, nullable=True)
    Source = Column('source', String, nullable=True)
    Mentions = Column('mentions', ARRAY(String), nullable=True)
    Hashtags = Column('hashtags', ARRAY(String), nullable=True)
    Urls = Column('urls', ARRAY(String), nullable=True)

    OriginalTweetId = Column('originalTweetId', BigInteger, nullable=True)
    OriginalTweetUserId = Column('originalTweetUserId', BigInteger, nullable=True)
    OriginalTweetUserName = Column('originalTweetUserName', String, nullable=True)
    OriginalTweetUserScreenName = Column('originalTweetUserScreenName', String, nullable=True)
    OriginalTweetDate = Column('originalTweetDate', DateTime, nullable=True)

    UserId = Column('userId', BigInteger, nullable=True)
    UserName = Column('userName', String, nullable=True)
    UserScreenName = Column('userScreenName', String, nullable=True)
    UserCreatedDate = Column('userCreatedDate', DateTime, nullable=True)
    UserLang = Column('userLang', String, nullable=True)
    UserLocation = Column('userLocation', String, nullable=True)
    UserTimeZone = Column('userTimeZone', String, nullable=True)
    UserUTCOffset = Column('userUTCOffset', String, nullable=True)
    UserFollowerCount = Column('userFollowerCount', BigInteger, nullable=True)
    UserFriendsCount = Column('userFriendsCount', BigInteger, nullable=True)
    UserFavoritesCount = Column('userFavoritesCount', BigInteger, nullable=True)
    UserStatusesCount = Column('userStatusesCount', BigInteger, nullable=True)


class FakeNews(DeclarativeBase):
    __tablename__ = "fakenews"

    Id = Column("id", BigInteger, primary_key=True)
    tweetid = Column(BigInteger, nullable=True)
    fullurl = Column(String, nullable=True)
    website = Column(String, nullable=True)
    rawurl = Column(String, nullable=True)
