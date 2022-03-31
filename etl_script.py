import requests
import pandas as pd
import time
import datetime

now = datetime.datetime.now()
print(f"Run start time: {now.strftime('%Y-%m-%d %H:%M:%S')}")

t_start = time.time()

API_KEY = 'AIzaSyBzVOeHPZ2tMxMxx2djhU3Zj_ztgKTtM6U'
CHANNEL_ID = 'UCxAkiUiL3KnMl0BIFBv6xxA' #macaroni

page_token = ""
url = "https://www.googleapis.com/youtube/v3/search?key="+API_KEY+"&channelId="+CHANNEL_ID+"&part=snippet,id&order=date&maxResults=10000"+page_token
response = requests.get(url).json()


from firstpackage.getvideos.getvideos_module import get_videos, get_no_of_pages
pages_number = get_no_of_pages(CHANNEL_ID, API_KEY)

t0 = time.time()
df = get_videos(API_KEY, CHANNEL_ID, pages_number)
t1 = time.time()
print(f" Videos data extracted: {(t1-t0)/60} mins")


#Cleaning dataframe



from firstpackage.cleandf.cleandf_module import split_title_and_datetime, clean_title_string, clean_casts

df2 = split_title_and_datetime(df)
df2 = clean_title_string(df2)

#df2.head(2)
df2 = clean_casts(df2)

#df2.head(2)

import numpy as np

df2 = df2.replace({np.nan: None})

#df2.head(2)

#Connect Python to SQL

from firstpackage.updatedatabase.update_module_mac import update_db, append_new_data_to_db

t0 = time.time()
new_data_df = update_db(df2)
t1 = time.time()
print(f"Db update: {(t1-t0)/60} mins")

#new_data_df.shape



append_new_data_to_db(new_data_df)

t_end = time.time()

print(f"runtime: {(t_end - t_start)/60} mins")
print("\n")

