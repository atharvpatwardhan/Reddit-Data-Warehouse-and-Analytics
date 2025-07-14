import s3fs
import os
from dotenv import load_dotenv
load_dotenv(dotenv_path='../.env')

AWS_ACCESS_KEY_ID = os.getenv('aws_access_key_id')
AWS_ACCESS_KEY = os.getenv('aws_secret_access_key')
AWS_SESSION_TOKEN = os.getenv('aws_session_token')
AWS_REGION = os.getenv('aws_region')
AWS_BUCKET_NAME = os.getenv('aws_bucket_name')

def connect_to_s3():
    try:
        s3 = s3fs.S3FileSystem(anon=False,
                               key= AWS_ACCESS_KEY_ID,
                               secret=AWS_ACCESS_KEY)
        return s3
    except Exception as e:
        print(e)

def create_bucket_if_not_exist(s3: s3fs.S3FileSystem, bucket:str):
    try:
        if not s3.exists(bucket):
            s3.mkdir(bucket)
            print("Bucket created")
        else :
            print("Bucket already exists")
    except Exception as e:
        print(e)


def upload_to_s3_bucket(s3: s3fs.S3FileSystem, file_path: str, bucket:str, s3_file_name: str):
    try:
        s3.put(file_path, bucket+'/raw/'+ s3_file_name)
        print('File uploaded to s3')
    except FileNotFoundError:
        print('The file was not found')