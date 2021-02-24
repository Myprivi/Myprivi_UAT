from __future__ import print_function
import boto3
import base64, time, json, sys
import pymysql
import os
import logging
import time
import requests
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

comprehend = boto3.client("comprehend")

try:
    client = boto3.client('secretsmanager', region_name="us-east-2")
    response = client.get_secret_value(SecretId="Secrets_Pre_Prod")
    secret = json.loads(response['SecretString'])
    print("Secrets_Pre_Prod: ", secret)
except ClientError as e:
    if e.response['Error']['Code'] == 'DecryptionFailureException':
        raise e
    elif e.response['Error']['Code'] == 'InternalServiceErrorException':
        raise e
    elif e.response['Error']['Code'] == 'InvalidParameterException':
        raise e
    elif e.response['Error']['Code'] == 'InvalidRequestException':
        raise e
    elif e.response['Error']['Code'] == 'ResourceNotFoundException':
        raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = response['SecretString']
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])

RDS_HOSTNAME = secret["Rds_Hostname_Pre_Prod"]
DBUSERNAME = secret["Db_Username_Pre_Prod"]
DBPASSWORD = secret["Db_Password_Pre_Prod"]
DATABASE = secret["Database_Pre_Prod"]
CONNECTION_TIMEOUT = secret["Db_Connection_Timeout_Pre_Prod"]

'''
ssmservice = boto3.client('ssm')

dbusername = ssmservice.get_parameter(Name='dbusername', WithDecryption=True)
DBUSERNAME = dbusername['Parameter']['Value']

dbpassword = ssmservice.get_parameter(Name='dbpassword', WithDecryption=True)
DBPASSWORD = dbpassword['Parameter']['Value']

rds_hostname = ssmservice.get_parameter(Name='rds_hostname', WithDecryption=True)
RDS_HOSTNAME = rds_hostname['Parameter']['Value']
'''

def upload_profile_photo(_PROFILE_URL):
    # _PROFILE_PHOTO = payload['user']['profile_image_url_https']
    _MEDIA_FILE_NAME = _PROFILE_URL.rsplit("/")[-1]
    _FOLDER_PATH = "twitter/profile/"
    r = requests.get(_PROFILE_URL, stream=True)
    session = boto3.Session()
    region = session.region_name
    s3 = session.resource('s3')
    bucket_name = 'myprivi-media-pre-prod'
    bucket = s3.Bucket(bucket_name)
    bucket_result = bucket.upload_fileobj(r.raw, _FOLDER_PATH + _MEDIA_FILE_NAME)
    _S3_BUCKET_NAME = f'{bucket_name}'
    _PROFILE_PHOTO = f'{_FOLDER_PATH}{_MEDIA_FILE_NAME}'
    print('_PROFILE_PHOTO: ', _PROFILE_PHOTO)
    return _PROFILE_PHOTO

def upload_S3_bucket(_MEDIA_FILE,_POST_ID, _COMMENTS_POST_ID,_CREATED_AT, _UPDATED_ON):
    try:
        conct = pymysql.connect(host=RDS_HOSTNAME,
                                user=DBUSERNAME,
                                passwd=DBPASSWORD,
                                database=DATABASE,
                                connect_timeout=CONNECTION_TIMEOUT)
        #print("Connected to %s as %s" % (RDS_HOSTNAME, DBUSERNAME))
        if _MEDIA_FILE != None:
            for item in _MEDIA_FILE:
                _MEDIA_TYPE = item["type"]
                if _MEDIA_TYPE == 'photo':
                    _TWITTER_MEDIA_URL = item["media_url_https"]
                    _MEDIA_FILE_NAME = _TWITTER_MEDIA_URL.rsplit("/")[-1]
                    _FOLDER_PATH = "twitter/photo/"
                elif _MEDIA_TYPE == 'animated_gif':
                    _TWITTER_MEDIA_URL = item['video_info']['variants'][0]['url']
                    _MEDIA_FILE_NAME = _TWITTER_MEDIA_URL.rsplit("/")[-1]
                    _FOLDER_PATH = 'twitter/animated_gif/'
                else:
                    _TWITTER_MEDIA_URL = item['video_info']['variants'][0]['url']
                    _MEDIA_FILE_NAME = _TWITTER_MEDIA_URL.rsplit("/")[-1].rsplit("?")[-2]
                    _FOLDER_PATH = 'twitter/video/'
                if 'description' not in item:
                    _MEDIA_DESCRIPTION = None
                else:
                    _MEDIA_DESCRIPTION = item['description']
                r = requests.get(_TWITTER_MEDIA_URL, stream=True)
                session = boto3.Session()
                region = session.region_name
                s3 = session.resource('s3')
                bucket_name = 'myprivi-media-pre-prod'
                bucket = s3.Bucket(bucket_name)
                bucket_result = bucket.upload_fileobj(r.raw, _FOLDER_PATH + _MEDIA_FILE_NAME)
                _S3_BUCKET_NAME = f'{bucket_name}'
                _S3_STORAGE_MEDIA_URL = f'{_FOLDER_PATH}{_MEDIA_FILE_NAME}'
                sql_insert = """INSERT IGNORE into myprivi_live_demo.twitter_media (POST_ID,COMMENTS_POST_ID,TWITTER_MEDIA_URL,S3_BUCKET_NAME,S3_STORAGE_MEDIA_URL,MEDIA_TYPE,MEDIA_NAME,MEDIA_DESCRIPTION,CREATED_AT,UPDATED_ON) VALUES  (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                with conct.cursor() as curs1:
                    try:
                        curs1.execute(sql_insert, (
                            _POST_ID, _COMMENTS_POST_ID, _TWITTER_MEDIA_URL, _S3_BUCKET_NAME, _S3_STORAGE_MEDIA_URL,
                            _MEDIA_TYPE, _MEDIA_FILE_NAME, _MEDIA_DESCRIPTION, _CREATED_AT, _UPDATED_ON))
                        conct.commit()
                    except Exception as e:
                        print("Exception Caught while inserting into twitter_media table: ", e)
    except Exception as e:
        print()
    # return _MEDIA_TYPE, _TWITTER_MEDIA_URL, _S3_BUCKET_NAME, _S3_STORAGE_MEDIA_URL, _MEDIA_FILE_NAME, _MEDIA_DESCRIPTION
    return None

def save_events(payload):
    try:
        conct = pymysql.connect(host=RDS_HOSTNAME,
                                user=DBUSERNAME,
                                passwd=DBPASSWORD,
                                database=DATABASE,
                                connect_timeout=CONNECTION_TIMEOUT)
        # print("Connected to %s as %s" % (RDS_HOSTNAME, DBUSERNAME))

        #####################################################     1. TWEET AND RETWEET    ########################################################
        if payload['in_reply_to_status_id'] == None and payload['is_quote_status'] == False:

            _POST_ID = payload['id']
            _POST_USER_ID = payload['user']['id_str']
            _POST_OWNER = payload['user']['screen_name']

            try:
                if payload['user']['profile_image_url_https']:
                    _PROFILE_URL = payload['user']['profile_image_url_https']
                    _PROFILE_PHOTO = upload_profile_photo(_PROFILE_URL)
            except Exception as e:
                print("Profile photo field exception: ", e)

            try:
                if not 'retweeted_status' in payload:
                    if payload['truncated'] == False and not 'display_text_range' in payload:
                        _POST = payload['text']
                    elif payload['truncated'] == False and 'display_text_range' in payload:
                        _DISPLAY_TEXT = payload['display_text_range'][1]
                        _POST = payload['text'][:_DISPLAY_TEXT]
                    elif payload['truncated'] == True and 'extended_tweet' in payload:
                        if 'display_text_range' in payload['extended_tweet']:
                            _DISPLAY_TEXT = payload['extended_tweet']['display_text_range'][1]
                            _POST = payload['extended_tweet']['full_text'][:_DISPLAY_TEXT]
                        else:
                            _POST = payload['extended_tweet']['full_text']
                    else:
                        _POST = None
                else:
                    if payload['retweeted_status']['truncated'] == False and not 'display_text_range' in payload[
                        'retweeted_status']:
                        _POST = payload['retweeted_status']['text']
                    elif payload['retweeted_status']['truncated'] == False and 'display_text_range' in payload[
                        'retweeted_status']:
                        _DISPLAY_TEXT = payload['retweeted_status']['display_text_range'][1]
                        _POST = payload['retweeted_status']['text'][:_DISPLAY_TEXT]
                    elif payload['retweeted_status']['truncated'] == True and 'extended_tweet' in payload[
                        'retweeted_status']:
                        if 'display_text_range' in payload['retweeted_status']['extended_tweet']:
                            _DISPLAY_TEXT = payload['retweeted_status']['extended_tweet']['display_text_range'][1]
                            _POST = payload['retweeted_status']['extended_tweet']['full_text'][:_DISPLAY_TEXT]
                        else:
                            _POST = payload['retweeted_status']['extended_tweet']['full_text']
                    else:
                        _POST = None
            except Exception as e:
                print("Post field exception: ", e)

            try:
                if payload['text']:
                    if len(_POST) != 0:
                        _message_sentiment = comprehend.detect_sentiment(Text=_POST, LanguageCode="en")
                        if str(_message_sentiment['Sentiment']) == 'NEGATIVE' or str(
                                _message_sentiment['Sentiment']) == 'MIXED':
                            _IS_MALICIOUS = 1
                        if str(_message_sentiment['Sentiment']) == 'POSITIVE' or str(
                                _message_sentiment['Sentiment']) == 'NEUTRAL':
                            _IS_MALICIOUS = 2
                    else:
                        _IS_MALICIOUS = 2
            except Exception as e:
                print("Malicious field exception: ", e)

            try:
                if not 'retweeted_status' in payload:
                    if "extended_tweet" in payload:
                        _MENTIONS_TWEET = payload['extended_tweet']["entities"]["user_mentions"]
                    else:
                        _MENTIONS_TWEET = payload["entities"]["user_mentions"]
                    if len(_MENTIONS_TWEET) == 0:
                        _MENTIONS = None
                    else:
                        mention_list = []
                        for item in _MENTIONS_TWEET:
                            mention_details = item["screen_name"]
                            mention_list.append(mention_details)
                        _MENTIONS = ", ".join(mention_list)
                else:
                    if "extended_tweet" in payload["retweeted_status"]:
                        _MENTIONS_RETWEET = payload['retweeted_status']['extended_tweet']["entities"]["user_mentions"]
                    else:
                        _MENTIONS_RETWEET = payload['retweeted_status']["entities"]["user_mentions"]
                    if len(_MENTIONS_RETWEET) == 0:
                        _MENTIONS = None
                    else:
                        mention_list = []
                        for item in _MENTIONS_RETWEET:
                            mention_details = item["screen_name"]
                            mention_list.append(mention_details)
                        _MENTIONS = ", ".join(mention_list)
            except Exception as e:
                print("Mentions field exception: ", e)

            try:
                if not 'retweeted_status' in payload:
                    if "extended_tweet" in payload:
                        _HASH_TAGS_TWEET = payload['extended_tweet']['entities']['hashtags']
                    else:
                        _HASH_TAGS_TWEET = payload['entities']['hashtags']
                    if len(_HASH_TAGS_TWEET) == 0:
                        _HASH_TAGS = None
                    else:
                        hashtag_list = []
                        for item in _HASH_TAGS_TWEET:
                            hashtag_details = item["text"]
                            hashtag_list.append(hashtag_details)
                        _HASH_TAGS = ", ".join(hashtag_list)
                else:
                    if "extended_tweet" in payload["retweeted_status"]:
                        _HASH_TAGS_RETWEET = payload['retweeted_status']['extended_tweet']['entities']['hashtags']
                    else:
                        _HASH_TAGS_RETWEET = payload['retweeted_status']['entities']['hashtags']
                    if len(_HASH_TAGS_RETWEET) == 0:
                        _HASH_TAGS = None
                    else:
                        hashtag_list = []
                        for item in _HASH_TAGS_RETWEET:
                            hashtag_details = item["text"]
                            hashtag_list.append(hashtag_details)
                        _HASH_TAGS = ", ".join(hashtag_list)
            except Exception as e:
                print("Hashtags field exception: ", e)

            try:
                if payload['created_at']:
                    _CREATED_AT = time.strftime('%Y-%m-%d %H:%M:%S',
                                                time.strptime(payload['created_at'], '%a %b %d %H:%M:%S +0000 %Y'))
            except KeyError as e:
                print("Created_at field exception: ", e)

            try:
                if payload['created_at']:
                    _UPDATED_ON = time.strftime('%Y-%m-%d %H:%M:%S',
                                                time.strptime(payload['created_at'], '%a %b %d %H:%M:%S +0000 %Y'))
            except KeyError as e:
                print("Updated_on field exception: ", e)

            # ========Inserting Tweet or Retweet into feeds_table ========== #

            sql_insert = """INSERT IGNORE into myprivi_live_demo.twitter_feeds(POST_ID, POST_USER_ID, POST_OWNER, PROFILE_PHOTO, POST, IS_MALICIOUS, MENTIONS ,HASH_TAGS ,CREATED_AT ,UPDATED_ON) VALUES  (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            with conct.cursor() as curs1:
                try:
                    curs1.execute(sql_insert, (
                    _POST_ID, _POST_USER_ID, _POST_OWNER, _PROFILE_PHOTO, _POST, _IS_MALICIOUS, _MENTIONS, _HASH_TAGS,
                    _CREATED_AT, _UPDATED_ON))
                    conct.commit()
                except Exception as e:
                    print("Exception Caught while inserting into twitter_feeds table: ", e)

            try:
                if payload['in_reply_to_status_id'] == None:
                    _COMMENTS_POST_ID = None
                else:
                    _COMMENTS_POST_ID = payload['in_reply_to_status_id']
            except Exception as e:
                print("Comments_post_id field exception: ", e)

            try:
                if not 'retweeted_status' in payload:
                    if not 'extended_tweet' in payload:
                        if not 'extended_entities' in payload:
                            _MEDIA_FILE = None
                        else:
                            _MEDIA_FILE = payload['extended_entities']['media']
                    else:
                        if not 'extended_entities' in payload['extended_tweet']:
                            _MEDIA_FILE = None
                        else:
                            _MEDIA_FILE = payload['extended_tweet']['extended_entities']['media']
                else:
                    if not 'extended_tweet' in payload['retweeted_status']:
                        if not 'extended_entities' in payload['retweeted_status']:
                            _MEDIA_FILE = None
                        else:
                            _MEDIA_FILE = payload['retweeted_status']['extended_entities']['media']
                    else:
                        if not 'extended_entities' in payload['retweeted_status']['extended_tweet']:
                            _MEDIA_FILE = None
                        else:
                            _MEDIA_FILE = payload['retweeted_status']['extended_tweet']['extended_entities']['media']

                if _MEDIA_FILE is not None:
                    # _MEDIA_TYPE, _TWITTER_MEDIA_URL, _S3_BUCKET_NAME, _S3_STORAGE_MEDIA_URL, _MEDIA_FILE_NAME, _MEDIA_DESCRIPTION = upload_S3_bucket(_MEDIA_FILE)
                    upload_S3_bucket(_MEDIA_FILE, _POST_ID, _COMMENTS_POST_ID, _CREATED_AT, _UPDATED_ON)

                    '''
                    sql_insert = """INSERT IGNORE into myprivi_live_demo.twitter_media (POST_ID,COMMENTS_POST_ID,TWITTER_MEDIA_URL,S3_BUCKET_NAME,S3_STORAGE_MEDIA_URL,MEDIA_TYPE,MEDIA_NAME,MEDIA_DESCRIPTION,CREATED_AT,UPDATED_ON) VALUES  (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                    with conct.cursor() as curs1:
                        try:
                            curs1.execute(sql_insert, (
                            _POST_ID, _COMMENTS_POST_ID, _TWITTER_MEDIA_URL, _S3_BUCKET_NAME, _S3_STORAGE_MEDIA_URL,
                            _MEDIA_TYPE, _MEDIA_FILE_NAME, _MEDIA_DESCRIPTION, _CREATED_AT, _UPDATED_ON))
                            conct.commit()
                        except Exception as e:
                            print("Exception Caught while inserting into twitter_media table: ", e)
                    '''

            except Exception as e:
                print("Media code exception", e)

        #################################################   2. QUOTE TWEET (Retweet with comments)   ####################################################
        elif payload['is_quote_status'] == True:
            _POST_ID = payload['id']
            _POST_USER_ID = payload['user']['id']
            _POST_OWNER = payload['user']['screen_name']
            _COMMENTS_POST_ID = payload['id']

            try:
                if payload['user']['profile_image_url_https']:
                    _PROFILE_URL = payload['user']['profile_image_url_https']
                    _PROFILE_PHOTO = upload_profile_photo(_PROFILE_URL)
            except Exception as e:
                print("Profile photo field exception: ", e)

            try:
                if payload['quoted_status']['truncated'] == False and not 'display_text_range' in payload[
                    'quoted_status']:
                    _POST = payload['quoted_status']['text']
                elif payload['quoted_status']['truncated'] == False and 'display_text_range' in payload[
                    'quoted_status']:
                    _DISPLAY_TEXT = payload['quoted_status']['display_text_range'][1]
                    _POST = payload['quoted_status']['text'][:_DISPLAY_TEXT]
                elif payload['quoted_status']['truncated'] == True and 'extended_tweet' in payload['quoted_status']:
                    if 'display_text_range' in payload['quoted_status']['extended_tweet']:
                        _DISPLAY_TEXT = payload['quoted_status']['extended_tweet']['display_text_range'][1]
                        _POST = payload['quoted_status']['extended_tweet']['full_text'][:_DISPLAY_TEXT]
                    else:
                        _POST = payload['quoted_status']['extended_tweet']['full_text']
                else:
                    _POST = None
            except Exception as e:
                print("Post field exception: ", e)

            try:
                if payload['text']:
                    if len(_POST) != 0:
                        _message_sentiment = comprehend.detect_sentiment(Text=_POST, LanguageCode="en")
                        if str(_message_sentiment['Sentiment']) == 'NEGATIVE' or str(
                                _message_sentiment['Sentiment']) == 'MIXED':
                            _IS_POST_MALICIOUS = 1
                        if str(_message_sentiment['Sentiment']) == 'POSITIVE' or str(
                                _message_sentiment['Sentiment']) == 'NEUTRAL':
                            _IS_POST_MALICIOUS = 2
                    else:
                        _IS_POST_MALICIOUS = 2
            except Exception as e:
                print("Malicious field exception: ", e)

            try:
                if 'extended_tweet' in payload['quoted_status']:
                    _MENTIONS_RC = payload['quoted_status']['extended_tweet']["entities"]["user_mentions"]
                else:
                    _MENTIONS_RC = payload['quoted_status']["entities"]["user_mentions"]
                if len(_MENTIONS_RC) == 0:
                    _POST_MENTIONS = None
                else:
                    mention_list = []
                    for item in _MENTIONS_RC:
                        mention_details = item["screen_name"]
                        mention_list.append(mention_details)
                    _POST_MENTIONS = ", ".join(mention_list)
            except Exception as e:
                print("Mentions field exception: ", e)

            try:
                if 'extended_tweet' in payload['quoted_status']:
                    _HASH_TAGS_RC = payload['quoted_status']['extended_tweet']["entities"]['hashtags']
                else:
                    _HASH_TAGS_RC = payload['quoted_status']['entities']['hashtags']
                if len(_HASH_TAGS_RC) == 0:
                    _POST_HASH_TAGS = None
                else:
                    hashtag_list = []
                    for item in _HASH_TAGS_RC:
                        hashtag_details = item["text"]
                        hashtag_list.append(hashtag_details)
                    _POST_HASH_TAGS = ",".join(hashtag_list)
            except Exception as e:
                print("Hastags field exception:", e)

            try:
                if payload['created_at']:
                    _CREATED_AT = time.strftime('%Y-%m-%d %H:%M:%S',
                                                time.strptime(payload['created_at'], '%a %b %d %H:%M:%S +0000 %Y'))
            except Exception as e:
                print("Created_at field exception: ", e)

            try:
                if payload['created_at']:
                    _UPDATED_ON = time.strftime('%Y-%m-%d %H:%M:%S',
                                                time.strptime(payload['created_at'], '%a %b %d %H:%M:%S +0000 %Y'))
            except Exception as e:
                print("Updated_on field exception: ", e)

            sql_insert = """INSERT IGNORE into myprivi_live_demo.twitter_feeds(POST_ID, POST_USER_ID, POST_OWNER, PROFILE_PHOTO, POST, IS_MALICIOUS, MENTIONS ,HASH_TAGS ,CREATED_AT ,UPDATED_ON) VALUES  (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            with conct.cursor() as curs1:
                try:
                    curs1.execute(sql_insert, (
                    _POST_ID, _POST_USER_ID, _POST_OWNER, _PROFILE_PHOTO, _POST, _IS_POST_MALICIOUS, _POST_MENTIONS,
                    _POST_HASH_TAGS, _CREATED_AT, _UPDATED_ON))
                    conct.commit()
                except Exception as e:
                    print("Exception Caught while inserting into twitter_feeds table: ", e)

            try:
                if not 'extended_tweet' in payload['quoted_status']:
                    if not 'extended_entities' in payload['quoted_status']:
                        _MEDIA_FILE = None
                    else:
                        _MEDIA_FILE = payload['quoted_status']['extended_entities']['media']
                else:
                    if not 'extended_entities' in payload['quoted_status']['extended_tweet']:
                        _MEDIA_FILE = None
                    else:
                        _MEDIA_FILE = payload['quoted_status']['extended_tweet']['extended_entities']['media']

                if _MEDIA_FILE is not None:
                    # _MEDIA_TYPE, _TWITTER_MEDIA_URL, _S3_BUCKET_NAME, _S3_STORAGE_MEDIA_URL, _MEDIA_FILE_NAME, _MEDIA_DESCRIPTION = upload_S3_bucket(_MEDIA_FILE)
                    upload_S3_bucket(_MEDIA_FILE, _POST_ID, _COMMENTS_POST_ID, _CREATED_AT, _UPDATED_ON)

                    '''
                    sql_insert = """INSERT IGNORE into myprivi_live_demo.twitter_media (POST_ID,COMMENTS_POST_ID,TWITTER_MEDIA_URL, S3_BUCKET_NAME, S3_STORAGE_MEDIA_URL,MEDIA_TYPE,MEDIA_NAME,MEDIA_DESCRIPTION,CREATED_AT,UPDATED_ON) VALUES  (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                    with conct.cursor() as curs1:
                        try:
                            curs1.execute(sql_insert, (
                            _POST_ID, _COMMENTS_POST_ID, _TWITTER_MEDIA_URL, _S3_BUCKET_NAME, _S3_STORAGE_MEDIA_URL,
                            _MEDIA_TYPE, _MEDIA_FILE_NAME, _MEDIA_DESCRIPTION, _CREATED_AT, _UPDATED_ON))
                            conct.commit()
                        except Exception as e:
                            print("Exception Caught while inserting into twitter_media table: ", e)
                    '''

            except Exception as e:
                print("Media code exception", e)

            _REPLY_COMMENTS_ID = payload['quoted_status']['id_str']
            _COMMENTER_ID = payload['user']['id_str']
            _COMMENTER_NAME = payload['user']['screen_name']

            try:
                if payload['user']['profile_image_url_https']:
                    _PROFILE_PHOTO = payload['user']['profile_image_url_https']
            except Exception as e:
                print("Profile_photo field exception: ", e)

            try:
                if payload['truncated'] == False and not 'display_text_range' in payload:
                    _COMMENTS = payload['text']
                elif payload['truncated'] == False and 'display_text_range' in payload:
                    _DISPLAY_TEXT = payload['display_text_range'][1]
                    _COMMENTS = payload['text'][:_DISPLAY_TEXT]
                elif payload['truncated'] == True and 'extended_tweet' in payload:
                    if 'display_text_range' in payload['extended_tweet']:
                        _DISPLAY_TEXT = payload['extended_tweet']['display_text_range'][1]
                        _COMMENTS = payload['extended_tweet']['full_text'][:_DISPLAY_TEXT]
                    else:
                        _COMMENTS = payload['extended_tweet']['full_text']
                else:
                    _COMMENTS = None
            except Exception as e:
                print("Comments field exception: ", e)

            try:
                if payload['text']:
                    if len(_COMMENTS) != 0:
                        _message_sentiment = comprehend.detect_sentiment(Text=_COMMENTS, LanguageCode="en")
                        if str(_message_sentiment['Sentiment']) == 'NEGATIVE' or str(
                                _message_sentiment['Sentiment']) == 'MIXED':
                            _IS_COMMENTS_MALICIOUS = 1
                        if str(_message_sentiment['Sentiment']) == 'POSITIVE' or str(
                                _message_sentiment['Sentiment']) == 'NEUTRAL':
                            _IS_COMMENTS_MALICIOUS = 2
                    else:
                        _IS_COMMENTS_MALICIOUS = 2
            except Exception as e:
                print("Malicious field exception: ", e)

            try:
                if 'extended_tweet' in payload:
                    _MENTIONS_RC_ = payload['extended_tweet']["entities"]["user_mentions"]
                else:
                    _MENTIONS_RC_ = payload["entities"]["user_mentions"]
                if len(_MENTIONS_RC_) == 0:
                    _COMMENTS_MENTIONS = None
                else:
                    mention_list = []
                    for item in _MENTIONS_RC_:
                        mention_details = item["screen_name"]
                        mention_list.append(mention_details)
                    _COMMENTS_MENTIONS = ", ".join(mention_list)
            except Exception as e:
                print("Mentions field exception: ", e)

            try:
                if 'extended_tweet' in payload:
                    _HASH_TAGS_RC_ = payload['extended_tweet']['entities']['hashtags']
                else:
                    _HASH_TAGS_RC_ = payload['entities']['hashtags']
                if len(_HASH_TAGS_RC_) == 0:
                    _COMMENTS_HASH_TAGS = None
                else:
                    hashtag_list = []
                    for item in _HASH_TAGS_RC_:
                        hashtag_details = item["text"]
                        hashtag_list.append(hashtag_details)
                    _COMMENTS_HASH_TAGS = ",".join(hashtag_list)
            except Exception as e:
                print("Hashtags field exception: ", e)

            sql_insert = """INSERT IGNORE into myprivi_live_demo.twitter_comments (COMMENTS_POST_ID,POST_ID,REPLY_COMMENTS_ID,COMMENTER_ID,COMMENTER_NAME,PROFILE_PHOTO,COMMENTS,IS_MALICIOUS,MENTIONS,HASH_TAGS,CREATED_AT,UPDATED_ON) VALUES  (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            with conct.cursor() as curs1:
                try:
                    curs1.execute(sql_insert, (
                    _COMMENTS_POST_ID, _POST_ID, _REPLY_COMMENTS_ID, _COMMENTER_ID, _COMMENTER_NAME, _PROFILE_PHOTO,
                    _COMMENTS, _IS_COMMENTS_MALICIOUS, _COMMENTS_MENTIONS, _COMMENTS_HASH_TAGS, _CREATED_AT,
                    _UPDATED_ON))
                    conct.commit()
                except Exception as e:
                    print("Exception Caught while inserting into twitter_comments table: ", e)

            try:
                if not 'extended_tweet' in payload:
                    if not 'extended_entities' in payload:
                        _MEDIA_FILE = None
                    else:
                        _MEDIA_FILE = payload['extended_entities']['media']
                else:
                    if not 'extended_entities' in payload['extended_tweet']:
                        _MEDIA_FILE = None
                    else:
                        _MEDIA_FILE = payload['extended_tweet']['extended_entities']['media']

                if _MEDIA_FILE is not None:
                    # _MEDIA_TYPE, _TWITTER_MEDIA_URL, _S3_BUCKET_NAME, _S3_STORAGE_MEDIA_URL, _MEDIA_FILE_NAME, _MEDIA_DESCRIPTION = upload_S3_bucket(_MEDIA_FILE)
                    upload_S3_bucket(_MEDIA_FILE, _POST_ID, _COMMENTS_POST_ID, _CREATED_AT, _UPDATED_ON)

                    '''
                    sql_insert = """INSERT IGNORE into myprivi_live_demo.twitter_media (POST_ID,COMMENTS_POST_ID,TWITTER_MEDIA_URL,S3_BUCKET_NAME, S3_STORAGE_MEDIA_URL,MEDIA_TYPE,MEDIA_NAME,MEDIA_DESCRIPTION,CREATED_AT,UPDATED_ON) VALUES  (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                    with conct.cursor() as curs1:
                        try:
                            curs1.execute(sql_insert, (
                            _POST_ID, _COMMENTS_POST_ID, _TWITTER_MEDIA_URL, _S3_BUCKET_NAME, _S3_STORAGE_MEDIA_URL,
                            _MEDIA_TYPE, _MEDIA_FILE_NAME, _MEDIA_DESCRIPTION, _CREATED_AT, _UPDATED_ON))
                            conct.commit()
                        except Exception as e:
                            print("Exception Caught while inserting into twitter_media table: ", e)
                    '''

            except Exception as e:
                print("Media code exception", e)

        #################################################  3. COMMENTS     #####################################################
        else:
            _COMMENTS_POST_ID = payload['id']
            _REPLY_COMMENTS_ID = payload['in_reply_to_status_id_str']
            _COMMENTER_ID = payload['user']['id_str']
            _COMMENTER_NAME = payload['user']['screen_name']

            try:
                if payload['user']['profile_image_url_https']:
                    _PROFILE_URL = payload['user']['profile_image_url_https']
                    _PROFILE_PHOTO = upload_profile_photo(_PROFILE_URL)
            except Exception as e:
                print("Profile photo field exception: ", e)

            try:
                if payload['truncated'] == False and not 'display_text_range' in payload:
                    _COMMENTS = payload['text']
                elif payload['truncated'] == False and 'display_text_range' in payload:
                    _DISPLAY_TEXT = payload['display_text_range'][1]
                    _COMMENTS = payload['text'][:_DISPLAY_TEXT]
                elif payload['truncated'] == True and 'extended_tweet' in payload:
                    if 'display_text_range' in payload['extended_tweet']:
                        _DISPLAY_TEXT = payload['extended_tweet']['display_text_range'][1]
                        _COMMENTS = payload['extended_tweet']['full_text'][:_DISPLAY_TEXT]
                    else:
                        _COMMENTS = payload['extended_tweet']['full_text']
            except Exception as e:
                print("Comments field exception: ", e)

            try:
                if payload['text']:
                    if len(_COMMENTS) != 0:
                        _message_sentiment = comprehend.detect_sentiment(Text=_COMMENTS, LanguageCode="en")
                        if str(_message_sentiment['Sentiment']) == 'NEGATIVE' or str(
                                _message_sentiment['Sentiment']) == 'MIXED':
                            _IS_MALICIOUS = 1
                        if str(_message_sentiment['Sentiment']) == 'POSITIVE' or str(
                                _message_sentiment['Sentiment']) == 'NEUTRAL':
                            _IS_MALICIOUS = 2
                    else:
                        _IS_MALICIOUS = 2
            except Exception as e:
                print("Malicious field exception: ", e)

            try:
                if 'extended_tweet' in payload:
                    _MENTIONS_C = payload['extended_tweet']["entities"]["user_mentions"]
                else:
                    _MENTIONS_C = payload["entities"]["user_mentions"]
                if len(_MENTIONS_C) == 0:
                    _MENTIONS = None
                else:
                    mention_list = []
                    for item in _MENTIONS_C:
                        mention_details = item["screen_name"]
                        mention_list.append(mention_details)
                    _MENTIONS = ", ".join(mention_list)
            except Exception as e:
                print("Mentions field exception: ", e)

            try:
                if "extended_tweet" in payload:
                    _HASH_TAGS_C = payload['extended_tweet']['entities']['hashtags']
                else:
                    _HASH_TAGS_C = payload['entities']['hashtags']
                if len(_HASH_TAGS_C) == 0:
                    _HASH_TAGS = None
                else:
                    hashtag_list = []
                    for item in _HASH_TAGS_C:
                        hashtag_details = item["text"]
                        hashtag_list.append(hashtag_details)
                    _HASH_TAGS = ",".join(hashtag_list)
            except Exception as e:
                print("Hashtags field exception: ", e)

            try:
                if payload['created_at']:
                    # _UPDATED_ON = payload['created_at']
                    _UPDATED_ON = time.strftime('%Y-%m-%d %H:%M:%S',
                                                time.strptime(payload['created_at'], '%a %b %d %H:%M:%S +0000 %Y'))
            except KeyError as e:
                print("Updated_on field exception: ", e)

            try:
                if payload['created_at']:
                    _CREATED_AT = time.strftime('%Y-%m-%d %H:%M:%S',
                                                time.strptime(payload['created_at'], '%a %b %d %H:%M:%S +0000 %Y'))
            except KeyError as e:
                print("Created_at field exception: ", e)

            # =====Query to capture POST_ID value in all comments =============
            Capture_Post_ID = """select POST_ID from myprivi_live_demo.twitter_comments where COMMENTS_POST_ID = %s """
            with conct.cursor() as curs1:
                curs1.execute(Capture_Post_ID, _REPLY_COMMENTS_ID)
                _PostId = curs1.fetchall()
                if (_PostId):
                    _POST_ID = _PostId
                else:
                    _POST_ID = _REPLY_COMMENTS_ID

            sql_insert = """INSERT IGNORE into myprivi_live_demo.twitter_comments(COMMENTS_POST_ID,POST_ID,REPLY_COMMENTS_ID,COMMENTER_ID,COMMENTER_NAME,PROFILE_PHOTO,COMMENTS,IS_MALICIOUS,MENTIONS,HASH_TAGS,CREATED_AT,UPDATED_ON) VALUES  (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            with conct.cursor() as curs1:
                try:
                    curs1.execute(sql_insert, (
                    _COMMENTS_POST_ID, _POST_ID, _REPLY_COMMENTS_ID, _COMMENTER_ID, _COMMENTER_NAME, _PROFILE_PHOTO,
                    _COMMENTS, _IS_MALICIOUS, _MENTIONS, _HASH_TAGS, _CREATED_AT, _UPDATED_ON))
                    conct.commit()
                except Exception as e:
                    print("Exception Caught while inserting into twitter_comments table: ", e)

            try:
                if not 'extended_tweet' in payload:
                    if not 'extended_entities' in payload:
                        _MEDIA_FILE = None
                    else:
                        _MEDIA_FILE = payload['extended_entities']['media']
                else:
                    if not 'extended_entities' in payload['extended_tweet']:
                        _MEDIA_FILE = None
                    else:
                        _MEDIA_FILE = payload['extended_tweet']['extended_entities']['media']

                if _MEDIA_FILE is not None:
                    # _MEDIA_TYPE, _TWITTER_MEDIA_URL, _S3_BUCKET_NAME, _S3_STORAGE_MEDIA_URL, _MEDIA_FILE_NAME, _MEDIA_DESCRIPTION = upload_S3_bucket(_MEDIA_FILE)
                    upload_S3_bucket(_MEDIA_FILE, _POST_ID, _COMMENTS_POST_ID, _CREATED_AT, _UPDATED_ON)

                    '''
                    sql_insert = """INSERT IGNORE into myprivi_live_demo.twitter_media (POST_ID,COMMENTS_POST_ID,TWITTER_MEDIA_URL,S3_BUCKET_NAME, S3_STORAGE_MEDIA_URL,MEDIA_TYPE,MEDIA_NAME,MEDIA_DESCRIPTION,CREATED_AT,UPDATED_ON) VALUES  (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                    with conct.cursor() as curs1:
                        try:
                            curs1.execute(sql_insert, (
                            _POST_ID, _COMMENTS_POST_ID, _TWITTER_MEDIA_URL, _S3_BUCKET_NAME, _S3_STORAGE_MEDIA_URL,
                            _MEDIA_TYPE, _MEDIA_FILE_NAME, _MEDIA_DESCRIPTION, _CREATED_AT, _UPDATED_ON))
                            conct.commit()
                        except Exception as e:
                            print("Exception Caught while inserting into twitter_media table: ", e)
                    '''

            except Exception as e:
                print("Media code exception", e)

        # Commit your changes
        conct.commit()
    except Exception as e:
        print("Exception in code", e)

    finally:
        conct.close()
        curs1.close()

def lambda_handler(event, context):
    #   #Kinesis data is base64 encoded so decode here
    records = []
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        # We also assume payload comes as JSON form
        payload = json.loads(base64.b64decode(record['kinesis']['data']))
        records.append(payload)
        save_events(payload)