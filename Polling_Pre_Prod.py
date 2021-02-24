# Databricks notebook source
import schedule
import os
import pymysql
import boto3
import time
import json
import subprocess
import sys
from botocore.exceptions import ClientError

# This get_max_id will be called once everytime we start this polling program
def get_max_id():
    try:
        mydb = pymysql.connect(
            host = RDS_HOSTNAME,
            user = DBUSERNAME,
            password = DBPASSWORD,
            database = DATABASE,
            connect_timeout = CONNECTION_TIMEOUT
        )
        mycursor = mydb.cursor()
        #print("connected to database: ")

        #print("To store the max(ID) in old_max_id.py file")
        mycursor = mydb.cursor()
        mycursor.execute("SELECT max(ID) FROM personal_intelligence where STATUS = 1 and CHANNEL_TYPE = 3")
        old_max_id = mycursor.fetchone()
        #print(old_max_id)
        old_max_id = str(old_max_id).replace('(', '').replace(')', '').replace(',', '')
        #print(old_max_id)

        if old_max_id == "None":
          print("There is no Active Social_user_id in the Personal_Intelligence table. Exiting from the process..!")
          mycursor.close()
          mydb.close()
          sys.exit("Exiting the process successfully")

        else:
          with open("/dbfs/FileStore/tables/Streaming_Process/Pre_Prod/OldMaxId_Pre_Prod.py", "w") as fw:
            fw.write('%s' % old_max_id)
    except Exception as e:
        print("Problem in getting the max(ID) from table or writing the max(ID) into the old_max_id.py file",e)

 # This polling() will be called for every 30 second
def polling():

        #print("Inside Polling Method1")
        mydb = pymysql.connect(
            host = RDS_HOSTNAME,
            user = DBUSERNAME,
            password = DBPASSWORD,
            database = DATABASE,
            connect_timeout = CONNECTION_TIMEOUT)
        mycursor = mydb.cursor()

        q1 = """SELECT max(ID) FROM personal_intelligence where STATUS = 1 and CHANNEL_TYPE = 3"""
        mycursor.execute(q1)
        new_max_id = mycursor.fetchone()
        new_max_id = str(new_max_id).replace('(', '').replace(')', '').replace(',', '')
        old_max_id = open('/dbfs/FileStore/tables/Streaming_Process/Pre_Prod/OldMaxId_Pre_Prod.py').read().strip()

        #print("New_value: ",new_max_id,type(new_max_id))
        #print("Old Value: ",old_max_id,type(old_max_id))
        if int(new_max_id) > int(old_max_id):
            #print("Inside If condition")
            #print("Stopping the Streaming process!")
            #sp = subprocess.run(["pkill", "-f", "python3 /dbfs/FileStore/tables/Streaming_Process/Pre_Prod/Streaming_Pre_Prod.py"])
            #print("sp.returncode: ", sp.returncode)
            with open("/dbfs/FileStore/tables/Streaming_Process/Pre_Prod/OldMaxId_Pre_Prod.py", 'w') as fp:
                fp.write("%s" % str(new_max_id))

            q2 = """select program_name from myprivi_live_demo.control_stream where status = 1"""
            mycursor.execute(q2)
            pgrm_name = mycursor.fetchone()
            pgrm_name = str(pgrm_name).replace('(', '').replace(')', '').replace(',', '').replace("'", '')
            mycursor.close()
            mydb.close()

            if pgrm_name == 'Streaming_Pre_Prod_1.py':
                sp = subprocess.run(["pkill", "-f", "python3 /dbfs/FileStore/tables/Streaming_Process/Pre_Prod/Streaming_Pre_Prod_1.py"])
                time.sleep(0.05)
                if sp.returncode == 0:
                    print("Stopped the streaming process-1 successfully..!")
                else:
                    print("some problem in stopping the streaming process-1..")
                #print("Starting the streaming process from polling...")
                subprocess.Popen("python3 /dbfs/FileStore/tables/Streaming_Process/Pre_Prod/Streaming_Pre_Prod_1.py", shell=True)
                print("Started the streaming process-1 successfully..!")
            else:
                sp = subprocess.run(["pkill", "-f", "python3 /dbfs/FileStore/tables/Streaming_Process/Pre_Prod/Streaming_Pre_Prod_2.py"])
                time.sleep(0.05)
                if sp.returncode == 0:
                    print("Stopped the streaming process-2 successfully..!")
                else:
                    print("some problem in stopping the streaming process-2..")
                #print("Starting the streaming process from polling...")
                subprocess.Popen("python3 /dbfs/FileStore/tables/Streaming_Process/Pre_Prod/Streaming_Pre_Prod_2.py", shell=True)
                print("Started the streaming process-2 successfully..!")
        else:
          pass

try:
  client = boto3.client('secretsmanager',region_name="us-east-2")
  response = client.get_secret_value(SecretId="Secrets_Pre_Prod")
  secret = json.loads(response['SecretString'])
  #print("My_Credentials: ",secret)
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

# calling this to store the max(ID) intially to store in old_max_id.py file
get_max_id()

# Starting the Streaming process
mydb = pymysql.connect(host = RDS_HOSTNAME,user = DBUSERNAME,password = DBPASSWORD,database = DATABASE,connect_timeout = CONNECTION_TIMEOUT)
mycursor = mydb.cursor()

q = """select program_name from myprivi_live_demo.control_stream where status = 1"""
mycursor.execute(q)
pgrm_name = mycursor.fetchone()
pgrm_name = str(pgrm_name).replace('(', '').replace(')', '').replace(',', '').replace("'", '')

if pgrm_name == 'Streaming_Pre_Prod_1.py':
    print("Twitter Streaming process-1 Initiated..")
    subprocess.Popen("python3 /dbfs/FileStore/tables/Streaming_Process/Pre_Prod/Streaming_Pre_Prod_1.py", shell=True)
    print("Started the streaming process-1 successfully..!")
else:
    print("Twitter Streaming process-2 Initiated..")
    subprocess.Popen("python3 /dbfs/FileStore/tables/Streaming_Process/Pre_Prod/Streaming_Pre_Prod_2.py", shell=True)
    print("Started the streaming process-2 successfully..!")

mycursor.close()
mydb.close()

#Scheduling the polling function to run for every 20 secs
schedule.every(20).seconds.do(polling)
while True:
    schedule.run_pending()
    time.sleep(1)