import pyodbc
import pandas as pd
import numpy as np
import pickle

###QUERY NEW DATA FOR LIKES PREDICTION

conn2 = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};'
                                'Server=finesxxx.xxxx.xxxxxxx.amazonaws.com,xxxx;'
                                'Database=xxxxxxxx;'
                                'uid=xxxxx;pwd=xxxxxx;'
                                'TrustServerCertificate=yes;')


print ("Connected")
cursor = conn2.cursor()

query = '''
      SELECT *
      FROM modeling_data3
      WHERE months_since_upload <= 2
'''

df = pd.read_sql_query(query, con= conn2)

###PREPROCESSING  NEW DATA

df['year'] = df['year'].astype(str)

comedian = df['comedian'].unique()
gender = ['M', 'M', 'F', 'M']
df['gender'] = df['comedian'].map(dict(zip(comedian, gender)))

df1 = df[['comedian', 'gender', 'year', 'months_since_upload', 'video_duration',
       'view_count', 'weekend_weekday', 'monthly_videos_count','like_count']]


cat_columns = ['comedian', 'gender', 'year','weekend_weekday']

features = df1.columns.drop('like_count')
X = df1[list(features)]
y = df1['like_count']

from sklearn.metrics import mean_absolute_error as MAE

###LOAD SAVED ONEHOTENCODER
encoder =  pickle.load(open('Fitted_OneHotencoder.pkl', 'rb'))

def encode_new_data(new_data_df):
    cat_ohe_new = encoder.transform(new_data_df[cat_columns])
    ohe_new_data_df = pd.DataFrame(cat_ohe_new, columns = encoder.get_feature_names_out(input_features = cat_columns))
    
    new_data_df.reset_index(inplace= True)
    new_data_df.drop('index', axis= 1, inplace= True)
    
    new_data_df_encoded = pd.concat([new_data_df, ohe_new_data_df], axis=1).drop(columns = cat_columns, axis=1)
    return new_data_df_encoded


###LOAD SAVED RANDOMFOREST MODEL
loaded_model = pickle.load(open('RF_model.pkl', 'rb'))

y_pred = loaded_model.predict(encode_new_data(X))
y_pred = np.round(y_pred)
preds = pd.DataFrame(data=y_pred, columns= ['likes_prediction'])
preds['likes_prediction'] = preds['likes_prediction'].apply(lambda x: int(x))


mae_new_data = MAE(y, y_pred)
print(f"MAE of new data: {mae_new_data}")

final_df = df[['video_id', 'comedian', 'gender', 'year', 'year_month', 'months_since_upload',
       'video_duration', 'view_count', 'day_of_week',
       'weekend_weekday', 'monthly_videos_count', 'like_count']]

###MERGE PREDICTIONS WITH NEW DATA

final_df = pd.concat([final_df, preds], axis = 1)

###CREATE PREDICTIONS TABLE IN DATABASE

# query = '''
#     CREATE TABLE predictions (
#         video_id varchar(max) NULL,
#         comedian varchar(max) NULL,
#         gender varchar(3) NULL,
#         year varchar(max) NULL,
#         year_month varchar(max) NULL,
#         months_since_upload int NULL,
#         video_duration int NULL,
#         view_count int NULL,
#         day_of_week varchar(max) NULL,
#         weekend_weekday varchar(max) NULL, 
#         monthly_videos_count int NULL, 
#         like_count int NULL,
#         likes_prediction int NULL)
#     '''


# cursor.execute(query)
# conn2.commit()

###INSERT PREDICTIONS DATA INTO TABLE

# def insert_new_data_to_table(video_id, comedian, gender, year, year_month, months_since_upload,
#                              video_duration, view_count, day_of_week, weekend_weekday, monthly_videos_count,
#                              like_count, likes_prediction):
#     query = '''
#             INSERT INTO predictions (video_id, comedian, gender, year, year_month, months_since_upload,
#                              video_duration, view_count, day_of_week, weekend_weekday, monthly_videos_count,
#                              like_count, likes_prediction) 
#             VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
#     '''
#     row_values_to_insert = (video_id, comedian, gender, year, year_month, months_since_upload,
#                              video_duration, view_count, day_of_week, weekend_weekday, monthly_videos_count,
#                              like_count, likes_prediction)
    
#     cursor.execute(query, row_values_to_insert)

# for idx, row in final_df.iterrows():
#     insert_new_data_to_table(row['video_id'], row['comedian'], row['gender'], row['year'], row['year_month'],
#                        row['months_since_upload'], row['video_duration'], row['view_count'], row['day_of_week'],
#                        row['weekend_weekday'], row['monthly_videos_count'], row['like_count'], row['likes_prediction'])

#conn2.commit()


###UPDATE PREDICTIONS TABLE

def check_if_video_exists(video_id):
    query = '''
       SELECT *
       FROM predictions
       WHERE video_id = ?
    '''
    row = cursor.execute(query, (video_id,))
    if row:
        return row.fetchone()
    else:
        return None

def update_row(view_count, like_count, likes_prediction, video_id):
    query = '''
            UPDATE predictions
            SET 
                view_count = ?,
                like_count = ?,
                likes_prediction = ?
            WHERE video_id = ?
    '''
    vars_to_update = (view_count, like_count, likes_prediction, video_id)
    cursor.execute(query, vars_to_update)



def update_table(df):
    temp_df = pd.DataFrame(columns = ['video_id', 'comedian', 'gender', 'year', 'year_month', 'months_since_upload',
                    'video_duration', 'view_count', 'day_of_week', 'weekend_weekday', 'monthly_videos_count', 'like_count',
                    'likes_prediction'])
    for i, row in df.iterrows():
        if check_if_video_exists(row['video_id']):
            update_row(row['view_count'], row['like_count'], row['likes_prediction'], row['video_id'])
        else:
            temp_df = temp_df.append(row)
            
    return temp_df


new_data_dff = update_table(final_df)
print('Updated existing videos')

###IF VIDEOS DATA IS NOT IN PREDICTIONS TABLE, ADD THE DATA TO THE TABLE

if new_data_dff.empty:
    print('No new data to add')
    
else:
    for idx, row in new_data_dff.iterrows():
        insert_new_data_to_table(row['video_id'], row['comedian'], row['gender'], row['year'], row['year_month'],
                           row['months_since_upload'], row['video_duration'], row['view_count'], row['day_of_week'],
                           row['weekend_weekday'], row['monthly_videos_count'], row['like_count'], row['likes_prediction'])
    print('Added new data')

conn2.commit()






