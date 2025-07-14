import pandas as pd

from etls.reddit_etl import connect_reddit, extract_posts, transform_data, load_data_to_csv
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path='../.env')


REDDIT_CLIENT_ID = os.getenv('reddit_client_id')
REDDIT_SECRET_KEY = os.getenv('reddit_secret_key')
OUTPUT_PATH = os.getenv('output_path')

def reddit_pipeline(file_name: str, subreddit: str, time_filter: str, limit=None):
    # connecting to reddit instance
    instance = connect_reddit(REDDIT_CLIENT_ID, REDDIT_SECRET_KEY, 'hyp3rflame')
    # extraction
    posts = extract_posts(instance, subreddit, time_filter, limit)
    post_df = pd.DataFrame(posts)
    # transformation
    post_df = transform_data(post_df)
    # loading to csv
    file_path = f'{OUTPUT_PATH}/{file_name}.csv'
    load_data_to_csv(post_df, file_path)

    return file_path