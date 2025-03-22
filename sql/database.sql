USE ROLE ACCOUNTADMIN;

-- CREATE A DOC AI ROLE TO BE USED FOR THE QUICKSTART
CREATE ROLE DOC_AI_ROLE;
GRANT DATABASE ROLE SNOWFLAKE.DOCUMENT_INTELLIGENCE_CREATOR TO ROLE DOC_AI_ROLE;

-- CREATE A WAREHOUSE TO BE USED
CREATE WAREHOUSE DOC_AI_WH 
    WAREHOUSE_TYPE = 'STANDARD'
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_RESUME = TRUE
;

-- GIVE THE DOC_AI_ROLE ROLE ACCESS TO THE WAREHOUSE
GRANT USAGE, OPERATE, MODIFY ON WAREHOUSE DOC_AI_WH TO ROLE DOC_AI_ROLE;

-- CREATE DATABASE AND SCHEMA TO BE USED, GIVE THE DOC_AI_ROLE ACCESS
CREATE DATABASE DOC_AI_DB;
GRANT CREATE SCHEMA, MODIFY, USAGE ON DATABASE DOC_AI_DB TO ROLE DOC_AI_ROLE;

GRANT ROLE DOC_AI_ROLE TO USER ANKURCPATEL;

-- CHANGE TO THE QUICKSTART ROLE
USE ROLE DOC_AI_ROLE;

-- CREATE A SCHEMA FOR THE DOCUEMNT AI MODEL, STAGE etc
CREATE SCHEMA DOC_AI_DB.DOC_AI_SCHEMA;
CREATE SCHEMA DOC_AI_DB.SECURITY_SCHEMA;

-- EXPLICIT GRANT USAGE AND snowflake.ml.document_intelligence on the  SCHEMA
GRANT USAGE ON SCHEMA DOC_AI_DB.DOC_AI_SCHEMA TO ROLE DOC_AI_ROLE;
GRANT CREATE SNOWFLAKE.ML.DOCUMENT_INTELLIGENCE ON SCHEMA DOC_AI_DB.DOC_AI_SCHEMA TO ROLE DOC_AI_ROLE;

-- CREATE A STAGE FOR STORING DOCUMENTS
CREATE STAGE DOC_AI_DB.DOC_AI_SCHEMA.DOC_AI_STAGE
  DIRECTORY = (enable = true)
  ENCRYPTION = (type = 'snowflake_sse');

-- SCHEMA FOR THE STREAMLIT APP
CREATE SCHEMA DOC_AI_DB.STREAMLIT_SCHEMA;

-- TABLE FOR THE STREAMLIT APP
/*
CREATE OR REPLACE TABLE DOC_AI_DB.DOC_AI_SCHEMA.CO_BRANDING_AGREEMENTS_VERIFIED
(
    file_name string
    , snowflake_file_url string
    , verification_date TIMESTAMP
    , verification_user string
);
*/


USE ROLE USERADMIN;

CREATE USER DOC_AI_USER 
    PASSWORD = 'Zpzp0809!abcde'
    LOGIN_NAME = 'DOC_AI_USER'
    DISPLAY_NAME = 'DOC_AI_USER'
    DEFAULT_WAREHOUSE = 'DOC_AI_WH'
    DEFAULT_ROLE = 'DOC_AI_ROLE'
;

USE ROLE SECURITYADMIN;
GRANT ROLE DOC_AI_ROLE TO USER DOC_AI_USER;