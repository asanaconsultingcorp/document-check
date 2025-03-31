CREATE OR REPLACE PROCEDURE create_batch(org_id VARCHAR)
RETURNS VARCHAR NOT NULL
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'create_batch'
AS
$$
import pandas as pd
from datetime import datetime

def create_batch(snowpark_session, org_id, df_details):

    df_uuid = pd.DataFrame(snowpark_session.sql('SELECT UUID_STRING()').collect())
    new_uuid = df_uuid.iloc[0, 0]
    
    query = """
        INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER(ID, ORGANIZATION_ID, BATCH_NAME, CREATED_BY, MODIFIED_BY)
        VALUES('""" + str(new_uuid) + """', '""" + str(org_id) + """', '""" + str(datetime.today().strftime('%Y%m%d_%H%M%S_%f')) + """_BATCH', CURRENT_USER(), CURRENT_USER())
    """
    
    snowpark_session.sql(query).collect()    
         
    for df_detail in df_details:
        query = """
            INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_DETAILS(BATCH_HEADER_ID, FILE_NAME, CREATED_BY, MODIFIED_BY)
            VALUES('""" + str(new_uuid) + """', '""" + str(df_detail.name) + """', '""" + str(datetime.today().strftime('%Y%m%d_%H%M%S_%f')) + """_BATCH', CURRENT_USER(), CURRENT_USER())
        """
        
    return "1"
$$;