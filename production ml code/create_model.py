import pyodbc
import pandas as pd
import numpy as np
import pickle
pd.options.mode.chained_assignment = None

### Query modeling data from database

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
         WHERE months_since_upload > 2
       '''

#modeling_data2

df = pd.read_sql_query(query, con= conn2)
df['year'] = df['year'].astype(str)

comedian = df['comedian'].unique()
gender = ['M', 'F', 'M', 'M']
df['gender'] = df['comedian'].map(dict(zip(comedian, gender)))



new_df = df[['comedian', 'gender', 'year', 'months_since_upload', 'video_duration',
       'view_count', 'weekend_weekday', 'monthly_videos_count','like_count']]



X_new =new_df.columns.drop('like_count')
X = new_df[list(X_new)]
y = new_df['like_count']

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_squared_error as MSE, mean_absolute_error as MAE
from sklearn.model_selection import RandomizedSearchCV
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer

SEED = 1
cat_columns = ['comedian', 'gender', 'year','weekend_weekday']

def one_hot_encoding(training_df):
    
    ohe = OneHotEncoder(handle_unknown='ignore', sparse = False)
    ohe.fit(training_df[cat_columns])
    
    cat_ohe = ohe.transform(training_df[cat_columns])
    ohe_training_df = pd.DataFrame(cat_ohe, columns = ohe.get_feature_names(input_features = cat_columns))

    training_df.reset_index(inplace= True)
    training_df.drop('index', axis= 1, inplace= True)

    training_df_encoded = pd.concat([training_df, ohe_training_df], axis=1).drop(columns = cat_columns, axis=1)
    
    return ohe, training_df_encoded
    


encoder, X_train_encoded =  one_hot_encoding(X)
pickle.dump(encoder, open('Fitted_OneHotencoder.pkl', 'wb'))

def encode_new_data(new_data_df):
    cat_ohe_new = encoder.transform(new_data_df[cat_columns])
    ohe_new_data_df = pd.DataFrame(cat_ohe_new, columns = encoder.get_feature_names(input_features = cat_columns))
    
    new_data_df.reset_index(inplace= True)
    new_data_df.drop('index', axis= 1, inplace= True)
    
    new_data_df_encoded = pd.concat([new_data_df, ohe_new_data_df], axis=1).drop(columns = cat_columns, axis=1)
    return new_data_df_encoded



def random_forest_model():
    rf = RandomForestRegressor(random_state= SEED)

    params_grid = {'n_estimators': np.arange(200, 250, 10),
            'min_samples_leaf': np.arange(0, 0.01, 0.001)}

    search = RandomizedSearchCV(estimator = rf, param_distributions= params_grid, cv = 5, scoring = 'neg_mean_absolute_error',
                     n_iter = 20, verbose = 0, n_jobs = -1)

    result = search.fit(X_train_encoded, y)

    best_hyperparams = result.best_params_
    best_score = result.best_score_

    best_model = result.best_estimator_
    print(f"Best hyperparams: {best_hyperparams} \nBest score: {best_score}")
    return best_model

model = random_forest_model()

pickle.dump(model, open('RF_model.pkl', 'wb'))

print('Model created and saved')








































