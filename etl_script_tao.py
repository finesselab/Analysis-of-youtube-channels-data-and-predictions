import requests
import pandas as pd
import numpy as np
import time
import datetime

now = datetime.datetime.now()
print(f"Run start time: {now.strftime('%Y-%m-%d %H:%M:%S')}")

t_start = time.time()

API_KEY = 'AIzaSyDFCOnb-YXXurEO3D1zjmqbSCRkbCc7Yjo'
CHANNEL_ID = "UCvG_RH3tAekGN5rPUVUmcIg" #taaooma 6

from package.getvideos.getvideos_module import get_videos, get_no_of_pages

pages_number = get_no_of_pages(CHANNEL_ID, API_KEY)

t0 = time.time()

df = get_videos(API_KEY, CHANNEL_ID, pages_number)

t1 = time.time()
print(f"Videos data extracted: {(t1-t0)/60} mins")

#df.tail()



### Cleaning dataframe



from package.cleandf.cleandf_module import clean_title_string, remove_title_emoji

def split_title_and_datetime(df):
  
    #title_and_features = df['video_title'].str.split('|', expand = True)
    #title_and_features.columns = ['title']
    date_and_time = df['upload_datetime'].str.split('T', expand =True)
    date_and_time.columns = ['date', 'time']
    df_part = df[['video_id', 'video_title', 'view_count', 'like_count', 'dislike_count', 'comment_count', 'video_duration']]
    new_df = pd.concat([df_part, date_and_time], axis = 1)
    new_df = new_df[['video_id', 'date', 'time', 'video_title',
          'view_count', 'like_count', 'dislike_count', 'comment_count', 'video_duration']]
    
    new_df.columns = ['video_id', 'date', 'time', 'title',
          'view_count', 'like_count', 'dislike_count', 'comment_count', 'video_duration']
    
    new_df.fillna(np.nan, inplace= True)

    return new_df



df2 = split_title_and_datetime(df)
#df2.head()


#df2 = clean_title_string(df2)
#df2.head()

def clean_casts(df):
    new_list = []
    for title in df['title']:
            if title is not np.nan:
                new_list.append(remove_title_emoji(title).upper().strip())
            else:
                new_list.append(np.nan)
    df['title'] = new_list
    return df


df2 = clean_casts(df2)
#df2.head(10)


df2['cast1'] = np.nan
df2['cast2'] = np.nan
df2['cast3'] = np.nan
df2['cast4'] = np.nan
df2['cast5'] = np.nan

df2 = df2.replace({np.nan: None})
#df2.head(2)



from package.updatedatabase.update_module_tao import update_db, append_new_data_to_db

t0 = time.time()
new_data_df = update_db(df2)
t1 = time.time()
print(f"Db updated: {(t1-t0)/60} mins")

#new_data_df.shape



append_new_data_to_db(new_data_df)

t_end = time.time()

print(f"runtime: {(t_end - t_start)/60} mins")
print("\n")









