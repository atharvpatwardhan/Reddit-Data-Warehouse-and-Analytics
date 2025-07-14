from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import os
import sys
import pandas as pd

from etls.reddit_etl import connect_reddit, extract_posts, transform_data, load_data_to_csv
from dotenv import load_dotenv
from airflow.decorators import dag, task
from etls.s3_etl import connect_to_s3, create_bucket_if_not_exist, upload_to_s3_bucket

load_dotenv(dotenv_path='../.env')


REDDIT_CLIENT_ID = os.getenv('reddit_client_id')
REDDIT_SECRET_KEY = os.getenv('reddit_secret_key')
OUTPUT_PATH = os.getenv('output_path')
AWS_BUCKET_NAME = os.getenv('aws_bucket_name')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pipelines import reddit_pipeline

# Default DAG arguments
default_args = {
    'owner': 'atharv',
    'start_date': datetime.datetime(2025, 7, 13),
}

# File postfix (for dynamic filename generation)
file_postfix = datetime.datetime.now().strftime("%Y%m%d")

# Define the DAG
# dag = DAG(
#     dag_id='etl_reddit_pipeline',
#     default_args=default_args,
#     schedule_interval='@daily',
#     catchup=False,
#     tags=['reddit', 'etl', 'pipeline']
# )


@dag(
    start_date=datetime.datetime(2025, 7, 14),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Atharv", "retries": 0},
    tags=["example"],
)
def reddit_dag():
    @task
    def extract(file_name: str, subreddit: str, time_filter: str, limit=None):
            print("Starting Reddit ETL Pipeline")
            instance = connect_reddit(REDDIT_CLIENT_ID, REDDIT_SECRET_KEY, 'hyp3rflame')
            print("Connected to Reddit!")
            # extraction
            posts = extract_posts(instance, subreddit, time_filter, limit)
            post_df = pd.DataFrame(posts)
            # transformation
            post_df = transform_data(post_df)
            # loading to csv
            file_path = f'include/data/{file_name}.csv'
            load_data_to_csv(post_df, file_path)

            return file_path
    
    @task
    def upload_to_s3(ti):
            file_path = ti.xcom_pull(task_ids='extract', key='return_value')
            s3 = connect_to_s3()
            create_bucket_if_not_exist(s3, AWS_BUCKET_NAME)
            upload_to_s3_bucket(s3, file_path, AWS_BUCKET_NAME, file_path.split('/')[-1])

    extract_task = extract(
        file_name=f'reddit_{file_postfix}',
        subreddit='dataengineering',
        time_filter='month',
        limit=100
    )

    s3_upload_task = upload_to_s3()

    extract_task >> s3_upload_task



reddit_dag()