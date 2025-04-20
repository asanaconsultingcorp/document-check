#2) TEST DATAFRAME TO DICTIONARY
#import pandas as pd

# Sample DataFrame
#df = pd.DataFrame({
#    'A': [1, 2, 3],
#    'B': [4, 5, 6]
#})

# Convert DataFrame to dictionary using zip
#dict_from_columns = dict(zip(df.columns, df.T.values.tolist()))
#print(df)
#print(dict_from_columns)

#1) UUID TEST
#df_uuid = snowpark_session.sql('SELECT UUID_STRING()').collect()
#print(df_uuid)
