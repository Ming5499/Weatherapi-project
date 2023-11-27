#Python Reddit API
import praw
from praw import Reddit

import numpy as np
import pandas as pd

# Connect to Reddit
def connect_reddit(client_id, client_secret, user_agent) -> Reddit:
    try:
        reddit = praw.Reddit(client_id=client_id,
                             client_secret=client_secret,
                             user_agent=user_agent)
        print('Connect successful')
        return reddit
    except Exception as e:
        print(e)
        
        
#Extract posts
def extract_posts(reddit_instance: Reddit, subreddit: str, time_filter = str, limit = None):
    # Get the subreddit instance
    subreddit = reddit_instance.subreddit(subreddit)
    #Get posts from the subreddit (no limit)
    posts = subreddit.top(time_filter=time_filter,limit=limit)
    
    POST_FIELDS = (
    'id',
    'title',
    'score',
    'num_comments',
    'author',
    'created_utc',
    'url',
    'over_18',
    'edited',
    'spoiler',
    'stickied'
    )
    
    post_lists = []
    
    for post in posts:
        post_dict = vars(post)
        
        post = {key: post_dict[key] for key in POST_FIELDS}
        post_lists.append(post)
        
    return post_lists


def transform_posts(post_df: pd.DataFrame):
    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s')
    post_df['over_18'] = np.where((post_df['over_18'] == True),True,False)
    post_df['author'] = post_df['author'].astype(str)
    
    return post_df


def load_data(data: pd.DataFrame, path: str):
    # Load data to csv
    data.to_csv(path, index=False)

    