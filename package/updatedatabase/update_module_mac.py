import pyodbc
import pandas as pd

conn2 = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};'
                                'Server=finesxxx.xxxx.xxxxxxx.amazonaws.com,xxxx;'
                                'Database=xxxxxxxx;'
                                'uid=xxxxx;pwd=xxxxxx;'
                                'TrustServerCertificate=yes;')

print ("Connected")
cursor = conn2.cursor()




def check_if_video_exists(video_id):
    query = '''
       SELECT *
       FROM mr_macaroni
       WHERE video_id = ?
    '''
    row = cursor.execute(query, (video_id,))
    if row:
        return row.fetchone()
    else:
        return None




def update_row(video_id, view_count, like_count):
    query = '''
            UPDATE mr_macaroni
            SET 
                view_count = ?,
                like_count = ?
            WHERE video_id = ?
    '''
    vars_to_update = (view_count, like_count, video_id)
    cursor.execute(query, vars_to_update)




def update_db(df):
    temp_df = pd.DataFrame(columns = ['video_id', 'date', 'time', 'title', 'cast1', 'cast2', 'cast3', 'cast4',
       'cast5', 'view_count', 'like_count', 'dislike_count', 'comment_count', 'video_duration'])
    for i, row in df.iterrows():
        if check_if_video_exists(row['video_id']):
            update_row(row['video_id'], row['view_count'], row['like_count'])
        else:
            temp_df = temp_df.append(row)
            
    return temp_df




def insert_new_data_to_table(video_id, date, time, title, cast1, cast2, cast3, cast4,
            cast5, view_count, like_count, dislike_count, comment_count, video_duration):
    query = '''
            INSERT INTO mr_macaroni (video_id, upload_date, upload_time, title, cast1, cast2, cast3, cast4,
            cast5, view_count, like_count, dislike_count, comment_count, duration) 
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    '''
    row_values_to_insert = (video_id, date, time, title, cast1, cast2, cast3, cast4,
            cast5, view_count, like_count, dislike_count, comment_count, video_duration)
    
    cursor.execute(query, row_values_to_insert)




def append_new_data_to_db(dff):
    for i, row in dff.iterrows():
        insert_new_data_to_table(row['video_id'], row['date'], row['time'], row['title'],
                         row['cast1'],row['cast2'], row['cast3'], row['cast4'], row['cast5'], 
                          row['view_count'], row['like_count'], row['dislike_count'], row['comment_count'],
                                row['video_duration'])
    conn2.commit()
    conn2.close()
    print('Updated database!, Connection closed')










