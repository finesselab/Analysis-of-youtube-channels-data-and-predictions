# Analysis of youtube videos data of four popular Nigerian comedians.



## Data Collection
   ETL (Extract Transform Load) scripts were written for collecting videos data from comedians respective youtube 
   channel(s) via youtube API.
   - Pandas dataframes were built with the collected data which were in json format, the data was cleaned subsequently.
   - Finally, the dataframes were uploaded to MS-SQL database running on AWS RDS.




## Analysis
   1. Database views(code in SQL views file) were created from the tables as base reports for POWER BI visualization.
  
   #### Predictive model
   2. Also, a 'modeling data' view was created for exploring the relationship between different variables.

      The modeling data was also used for training and evaluating a 'likes_count' prediction model.
      The XGBoost regressor model predicts the number of likes of new videos with a Mean Absolute Error of 1200. The predictions
      are concatenated to the new data dataframe and uploaded to MS-SQL database running on AWS RDS.

      The production code for the prediction model is in 'production ml code' folder which runs through a 
      cronjob on an AWS Linux EC2 instance. 

