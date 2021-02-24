import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import boto3
import time
import schedule
import pymysql
import sys
import subprocess
import requests
import os
import base64
from botocore.exceptions import ClientError
from datetime import datetime
import time
from datetime import date

class MyStreamListener(StreamListener):

  def on_connect(self):
    print("We are connected to the production stream now!")

  def on_data(self, data):
    tweet = json.loads(data)
    print(tweet)
    try:
        put_response = kinesis_client.put_record(
            StreamName = STREAM_NAME,
            Data=json.dumps(tweet),
            PartitionKey=str(tweet['user']['screen_name']))
    except (AttributeError, Exception) as e:
        print("Caught Exception in on_data()",e)
        pass
    return True

  def on_error(self, status_code):
    print("Status_Code", status_code)
    try:
        if status_code == 420:
            #returning False in on_data() that disconnects the stream
            print("A client makes too many login attempts in a short period of time!",status_code)

            #Writing 420 error code information into the file at '/dbfs/FileStore/tables/Streaming_Process/Pre_Prod/Error_Log_Files/'
            Filename = '/dbfs/FileStore/tables/Streaming_Process/Pre_Prod/Error_Log_Files/Error_on_'
            Date = str(date.today())
            timestamp = str(time.time())
            FilePath = Filename + Date +"_"+timestamp
            ErrorAt = datetime.now()
            Error_Message = str(status_code) + '\n' +'Error on: '+ str(ErrorAt)
            with open(FilePath + ".txt", "w") as file:
                file.write(Error_Message)
            try:
                RDS_HOSTNAME = secret["Rds_Hostname_Pre_Prod"]
                DBUSERNAME = secret["Db_Username_Pre_Prod"]
                DBPASSWORD = secret["Db_Password_Pre_Prod"]
                DATABASE = secret["Database_Pre_Prod"]
                CONNECTION_TIMEOUT = secret["Db_Connection_Timeout_Pre_Prod"]
                connection = pymysql.connect(host=RDS_HOSTNAME, user=DBUSERNAME, passwd=DBPASSWORD, database=DATABASE, connect_timeout=CONNECTION_TIMEOUT)
                q1 = """update control_stream set status = 2 WHERE program_name = 'Streaming_Pre_Prod_2.py' """
                q2 = """update control_stream set status = 1 WHERE program_name = 'Streaming_Pre_Prod_1.py' """
                cursor = connection.cursor()
                cursor.execute(q1)
                cursor.execute(q2)
                connection.commit()
                cursor.close()
                connection.close()
                subprocess.Popen("python3 /dbfs/FileStore/tables/Streaming_Process/Pre_Prod/Streaming_Pre_Prod_1.py", shell=True)
                print("Started the streaming process-1 successfully..!")
            except Exception as e:
                print("Exception in code: ", e)
            return False

        elif status_code == 429:
            #returning False in on_data() that disconnects the stream
            print("Too Many Request!",status_code)

            #Writing 429 error code information into the file at '/dbfs/FileStore/tables/Streaming_Process/Pre_Prod/Error_Log_Files/'
            Filename = '/dbfs/FileStore/tables/Streaming_Process/Pre_Prod/Error_Log_Files/Error_on_'
            Date = str(date.today())
            timestamp = str(time.time())
            FilePath = Filename + Date +"_"+timestamp
            ErrorAt = datetime.now()
            Error_Message = str(status_code) + '\n' +'Error on: '+ str(ErrorAt)
            with open(FilePath + ".txt", "w") as file:
                file.write(Error_Message)
            try:
                RDS_HOSTNAME = secret["Rds_Hostname_Pre_Prod"]
                DBUSERNAME = secret["Db_Username_Pre_Prod"]
                DBPASSWORD = secret["Db_Password_Pre_Prod"]
                DATABASE = secret["Database_Pre_Prod"]
                CONNECTION_TIMEOUT = secret["Db_Connection_Timeout_Pre_Prod"]
                connection = pymysql.connect(host=RDS_HOSTNAME, user=DBUSERNAME, passwd=DBPASSWORD, database=DATABASE, connect_timeout=CONNECTION_TIMEOUT)
                q1 = """update control_stream set status = 2 WHERE program_name = 'Streaming_Pre_Prod_2.py' """
                q2 = """update control_stream set status = 1 WHERE program_name = 'Streaming_Pre_Prod_1.py' """
                cursor = connection.cursor()
                cursor.execute(q1)
                cursor.execute(q2)
                connection.commit()
                cursor.close()
                connection.close()
                subprocess.Popen("python3 /dbfs/FileStore/tables/Streaming_Process/Pre_Prod/Streaming_Pre_Prod_1.py", shell=True)
                print("Started the streaming process-1 successfully..!")
            except Exception as e:
                print("Exception in code: ", e)
            return False

        elif status_code == 401:
            print("HTTP authentication failed. Check your Access & Secret API Keys", status_code)
            return False

        elif status_code == 403:
            print("Forbidden Connection!, Check your Twitter Developer Account Endpoint details", status_code)
            return False

        elif status_code == 406:
            print("Problem with your IDs_To_Follow parameter. Check your IDs_To_Follow. At least one Filter should be mentioned.", status_code)
            return False

        elif status_code == 413:
            print("IDs_To_Follow parameters in the Filter Endpoint has been exceeded..!", status_code)
            return False

        elif status_code == 503:
            print("Twitter Streaming Server is temporarily unavailable!", status_code)
            return False

        else:
            print("Unknown/Unhandled Exception caught..!",status_code)
            try:
                RDS_HOSTNAME = secret["Rds_Hostname_Pre_Prod"]
                DBUSERNAME = secret["Db_Username_Pre_Prod"]
                DBPASSWORD = secret["Db_Password_Pre_Prod"]
                DATABASE = secret["Database_Pre_Prod"]
                CONNECTION_TIMEOUT = secret["Db_Connection_Timeout_Pre_Prod"]
                connection = pymysql.connect(host=RDS_HOSTNAME, user=DBUSERNAME, passwd=DBPASSWORD, database=DATABASE, connect_timeout=CONNECTION_TIMEOUT)
                q1 = """update control_stream set status = 2 WHERE program_name = 'Streaming_Pre_Prod_2.py' """
                q2 = """update control_stream set status = 1 WHERE program_name = 'Streaming_Pre_Prod_1.py' """
                cursor = connection.cursor()
                cursor.execute(q1)
                cursor.execute(q2)
                connection.commit()
                cursor.close()
                connection.close()
                subprocess.Popen("python3 /dbfs/FileStore/tables/Streaming_Process/Pre_Prod/Streaming_Pre_Prod_1.py", shell=True)
                print("Started the streaming process-1 successfully..!")
            except Exception as e:
                print("Exception in code: ", e)
            return False
    except Exception as e:
        print("Unknown/Unhandled Exception caught in the main on_error try catch..!",e)
        pass

  def on_exception(self, exception):
    print("Inside on_exception block")
    print("Exception Name: ",exception)

    # Writing exception information into the file at '/dbfs/FileStore/tables/Streaming_Process/Pre_Prod/Error_Log_Files/'
    Filename = '/dbfs/FileStore/tables/Streaming_Process/Pre_Prod/Error_Log_Files/Exception_on_'
    Date = str(date.today())
    timestamp = str(time.time())
    FilePath = Filename + Date + "_" + timestamp
    ErrorAt = datetime.now()
    Error_Message = str(exception) + '\n' + 'Exception on: ' + str(ErrorAt)
    with open(FilePath + ".txt", "w") as file:
        file.write(Error_Message)

    if ("OSError" or "ECONNRESET" or "104") in str(exception):
        try:
            print("Inside on OSError/ECONNRESET/104 Exception")
            RDS_HOSTNAME = secret["Rds_Hostname_Pre_Prod"]
            DBUSERNAME = secret["Db_Username_Pre_Prod"]
            DBPASSWORD = secret["Db_Password_Pre_Prod"]
            DATABASE = secret["Database_Pre_Prod"]
            CONNECTION_TIMEOUT = secret["Db_Connection_Timeout_Pre_Prod"]
            connection = pymysql.connect(host=RDS_HOSTNAME, user=DBUSERNAME, passwd=DBPASSWORD, database=DATABASE, connect_timeout=CONNECTION_TIMEOUT)
            q1 = """update control_stream set status = 2 WHERE program_name = 'Streaming_Pre_Prod_2.py' """
            q2 = """update control_stream set status = 1 WHERE program_name = 'Streaming_Pre_Prod_1.py' """
            cursor = connection.cursor()
            cursor.execute(q1)
            cursor.execute(q2)
            connection.commit()
            cursor.close()
            connection.close()
            subprocess.Popen("python3 /dbfs/FileStore/tables/Streaming_Process/Pre_Prod/Streaming_Pre_Prod_1.py", shell=True)
            print("Started the streaming process-1 successfully..!")
        except Exception as e:
            print("Exception in code: ", e)
    else:
        print("Unhandled Exception..!",str(exception))
        try:
            print("Inside on Unhandled Exception")
            RDS_HOSTNAME = secret["Rds_Hostname_Pre_Prod"]
            DBUSERNAME = secret["Db_Username_Pre_Prod"]
            DBPASSWORD = secret["Db_Password_Pre_Prod"]
            DATABASE = secret["Database_Pre_Prod"]
            CONNECTION_TIMEOUT = secret["Db_Connection_Timeout_Pre_Prod"]
            connection = pymysql.connect(host=RDS_HOSTNAME, user=DBUSERNAME, passwd=DBPASSWORD, database=DATABASE, connect_timeout=CONNECTION_TIMEOUT)
            cursor = connection.cursor()
            q1 = """update control_stream set status = 2 WHERE program_name = 'Streaming_Pre_Prod_2.py' """
            cursor.execute(q1)
            q2 = """update control_stream set status = 1 WHERE program_name = 'Streaming_Pre_Prod_1.py' """
            cursor.execute(q2)
            connection.commit()
            cursor.close()
            connection.close()
            subprocess.Popen("python3 /dbfs/FileStore/tables/Streaming_Process/Pre_Prod/Streaming_Pre_Prod_1.py", shell=True)
            print("Started the streaming process-1 successfully..!")
        except Exception as e:
            print("Exception in code: ", e)
    return False

  def keep_alive(self):
    #print("Inside Keep_alive method")
    return True

  def on_limit(self, track):
    print("Limit Message: ", track)
    return

  def on_timeout(self):
    print("Inside timeout method")
    time.sleep(3)
    return True

  def on_disconnect(self, notice):
    print("Inside on_disconnect message:", notice)
    time.sleep(3)
    return True

  def on_status_withheld(self, notice):
    print("on_status_withheld message: ", notice)
    return

  def on_warning(self, notice):
    print("Warning message: ",notice)
    return

try:
  client = boto3.client('secretsmanager',region_name="us-east-2")
  response = client.get_secret_value(SecretId="Secrets_Pre_Prod")
  secret = json.loads(response['SecretString'])
  print("My_Credentials: ",secret)
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

# Retrieving all the production secret credentials
RDS_HOSTNAME = secret["Rds_Hostname_Pre_Prod"]
DBUSERNAME = secret["Db_Username_Pre_Prod"]
DBPASSWORD = secret["Db_Password_Pre_Prod"]
DATABASE = secret["Database_Pre_Prod"]
CONNECTION_TIMEOUT = secret["Db_Connection_Timeout_Pre_Prod"]
STREAM_NAME = secret["Kinesis_Stream_Name_Pre_Prod"]
REGION_NAME = secret["Aws_Region_Name_Pre_Prod"]
AWS_ACCESS_KEY_ID = secret["Aws_Access_Key_Id_Pre_Prod"]
AWS_SECRET_ACCESS_KEY = secret["Aws_Secret_Access_Key_Pre_Prod"]
TWITTER_CONSUMER_KEY = secret["Consumer_Key_Pre_Prod_2"]
TWITTER_CONSUMER_SECRET = secret["Consumer_Secret_Pre_Prod_2"]
TWITTER_ACCESS_TOKEN = secret["Access_Token_Pre_Prod_2"]
TWITTER_ACCESS_TOKEN_SECRET = secret["Access_Token_Secret_Pre_Prod_2"]

if __name__ == "__main__":
    #print("Inside MAIN BLOCK of Streaming program..")

    kinesis_client = boto3.client('kinesis',
                                  region_name = REGION_NAME,
                                  aws_access_key_id = AWS_ACCESS_KEY_ID,
                                  aws_secret_access_key = AWS_SECRET_ACCESS_KEY)

    try:
        connection = pymysql.connect(host=RDS_HOSTNAME, user=DBUSERNAME, passwd=DBPASSWORD, database=DATABASE, connect_timeout=CONNECTION_TIMEOUT)
        records = """SELECT DISPLAY_NAME FROM personal_intelligence where STATUS = 1 and CHANNEL_TYPE = 3"""
        cursor = connection.cursor()
        cursor.execute(records)
        DISPLAY_NAME = cursor.fetchall()

        # ID_rem=Social_User_ID.deleteCharAt(0)
        ID = (((str(DISPLAY_NAME).replace("(", "")).replace(")", "")).replace(",,", ",").replace("'",""))
        ID_removelast = ID[:-1]

        UserName_To_Follow = ID_removelast.split(", ")
        #print("UserName_To_Follow", UserName_To_Follow)

        Track_Users = ["@" + x for x in UserName_To_Follow]
        print("Track_Users_list",Track_Users)

        # Writing tracked users into afile
        length = len(Track_Users)
        Tracked_Users = "Total Number of Tracked Users: "
        Total_Tracked_Users = Tracked_Users+str(length)
        with open("/dbfs/FileStore/tables/Streaming_Process/Pre_Prod/CurrentTracking.txt","w") as output:
            output.write(Total_Tracked_Users+'\n\n')

        count = 0
        with open("/dbfs/FileStore/tables/Streaming_Process/Pre_Prod/CurrentTracking.txt", 'w') as output:
            for row in Track_Users:
                count = count + 1
                output.write(str(count)+' '+str(row) + '\n')

    except Exception as e:
        print("Exception in code: ", e)

    listener = MyStreamListener()

    # Authenticating the Twitter app via User Access & secret API keys method
    auth = tweepy.OAuthHandler(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET)
    auth.set_access_token(TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    # Authenticating the Twitter app via Application Access API Keys
    # auth = tweepy.AppAuthHandler(API_KEY1, API_SECRET_KEY1)
    # api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    # stream = MyStream(auth,listener)

    stream = Stream(auth, listener)
    stream.filter(track=Track_Users)