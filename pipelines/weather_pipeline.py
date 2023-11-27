from utils.constants import *
from etls.reddit_etl import *
import pandas as pd


def reddit_pipeline(file_name: str, subreddit: str, time_filter = 'day', limit = None):
    
    # Connecting to reddit instance
    instance = connect_reddit(CLIENT_ID, SECRET, 'Mingggggggggggg')
    
    # Extraction
    posts = extract_posts(instance, subreddit, time_filter, limit)
    
    post_df = pd.DataFrame(posts)
    
    #Transformation
    post_df = transform_posts(post_df)
    
    #Load
    file_path = f'{OUTPUT_PATH}/{file_name}.csv'
    load_data(post_df, file_path)
    
    return file_path
    