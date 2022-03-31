import pandas as pd
import numpy as np

def split_title_and_datetime(df):
    import numpy as np
    
    title_and_features = df['video_title'].str.split('|', expand = True)
    title_and_features.columns = ['title', 'cast1', 'cast2', 'cast3', 'cast4', 'cast5']
    date_and_time = df['upload_datetime'].str.split('T', expand =True)
    date_and_time.columns = ['date', 'time']
    df_part = df[['video_id', 'view_count', 'like_count', 'dislike_count', 'comment_count', 'video_duration']]
    new_df = pd.concat([df_part, title_and_features, date_and_time], axis = 1)
    new_df = new_df[['video_id', 'date', 'time', 'title', 'cast1', 'cast2', 'cast3', 'cast4', 'cast5',
          'view_count', 'like_count', 'dislike_count', 'comment_count', 'video_duration']]
    
    new_df.fillna(np.nan, inplace= True)
    
    return new_df



def remove_title_emoji(string):
    import re
    pat = r"\w+"
    return ' '.join(re.findall(pat, string))




def clean_title_string(df):
    df['title'] = [remove_title_emoji(title) for title in df['title']]
    return df





def clean_casts(df):
    import numpy as np
    casts = ['cast1', 'cast2', 'cast3', 'cast4', 'cast5']
    for cast in casts:
        new_list = []
        for title in df[cast]:
                if title is not np.nan:
                    new_list.append(remove_title_emoji(title).upper().strip())
                else:
                    new_list.append(np.nan)
        df[cast] = new_list
    return df






