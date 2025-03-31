df_uuid = snowpark_session.sql('SELECT UUID_STRING()').collect()

print(df_uuid)