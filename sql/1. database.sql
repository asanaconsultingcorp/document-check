USE ROLE ACCOUNTADMIN;

-- CREATE A DOC AI ROLE TO BE USED FOR THE QUICKSTART
CREATE OR REPLACE ROLE DOC_AI_ROLE;
GRANT DATABASE ROLE SNOWFLAKE.DOCUMENT_INTELLIGENCE_CREATOR TO ROLE DOC_AI_ROLE;

-- CREATE A WAREHOUSE TO BE USED
CREATE OR REPLACE WAREHOUSE DOC_AI_WH 
    WAREHOUSE_TYPE = 'STANDARD'
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_RESUME = TRUE
;

-- GIVE THE DOC_AI_ROLE ROLE ACCESS TO THE WAREHOUSE
GRANT USAGE, OPERATE, MODIFY ON WAREHOUSE DOC_AI_WH TO ROLE DOC_AI_ROLE;

-- CREATE DATABASE AND SCHEMA TO BE USED, GIVE THE DOC_AI_ROLE ACCESS
CREATE OR REPLACE DATABASE DOC_AI_DB;
GRANT CREATE SCHEMA, MODIFY, USAGE ON DATABASE DOC_AI_DB TO ROLE DOC_AI_ROLE;

-- CREATE A SCHEMA FOR THE DOCUEMNT AI MODEL, STAGE etc
CREATE OR REPLACE SCHEMA DOC_AI_DB.DOC_AI_SCHEMA;
CREATE OR REPLACE SCHEMA DOC_AI_DB.SECURITY_SCHEMA;
CREATE OR REPLACE SCHEMA DOC_AI_DB.STREAMLIT_SCHEMA;

-- EXPLICIT GRANT USAGE AND snowflake.ml.document_intelligence on the  SCHEMA
GRANT USAGE ON SCHEMA DOC_AI_DB.DOC_AI_SCHEMA TO ROLE DOC_AI_ROLE;
GRANT USAGE ON SCHEMA DOC_AI_DB.SECURITY_SCHEMA TO ROLE DOC_AI_ROLE;
GRANT USAGE ON SCHEMA DOC_AI_DB.STREAMLIT_SCHEMA TO ROLE DOC_AI_ROLE;
GRANT CREATE SNOWFLAKE.ML.DOCUMENT_INTELLIGENCE ON SCHEMA DOC_AI_DB.DOC_AI_SCHEMA TO ROLE DOC_AI_ROLE;

GRANT SELECT, DELETE, TRUNCATE, INSERT, UPDATE ON ALL TABLES IN DATABASE DOC_AI_DB TO ROLE DOC_AI_ROLE;
GRANT SELECT, DELETE, TRUNCATE, INSERT, UPDATE ON FUTURE TABLES IN DATABASE DOC_AI_DB TO ROLE DOC_AI_ROLE;
GRANT SELECT ON ALL VIEWS IN DATABASE DOC_AI_DB TO ROLE DOC_AI_ROLE;
GRANT SELECT ON FUTURE VIEWS IN DATABASE DOC_AI_DB TO ROLE DOC_AI_ROLE;

GRANT SELECT, DELETE, TRUNCATE, INSERT, UPDATE ON ALL TABLES IN DATABASE DOC_AI_DB TO ROLE ACCOUNTADMIN;
GRANT SELECT, DELETE, TRUNCATE, INSERT, UPDATE ON FUTURE TABLES IN DATABASE DOC_AI_DB TO ROLE ACCOUNTADMIN;
GRANT SELECT ON ALL VIEWS IN DATABASE DOC_AI_DB TO ROLE ACCOUNTADMIN;
GRANT SELECT ON FUTURE VIEWS IN DATABASE DOC_AI_DB TO ROLE ACCOUNTADMIN;
GRANT CREATE SNOWFLAKE.ML.DOCUMENT_INTELLIGENCE ON SCHEMA DOC_AI_DB.DOC_AI_SCHEMA TO ROLE ACCOUNTADMIN;

-- CREATE A STAGE FOR STORING DOCUMENTS
CREATE STAGE DOC_AI_DB.DOC_AI_SCHEMA.DOC_AI_STAGE
  DIRECTORY = (enable = true)
  ENCRYPTION = (type = 'snowflake_sse');

CREATE OR REPLACE USER DOC_AI_USER 
    PASSWORD = 'Zpzp0809!abcde'
    LOGIN_NAME = 'DOC_AI_USER'
    DISPLAY_NAME = 'DOC_AI_USER'
    DEFAULT_WAREHOUSE = 'DOC_AI_WH'
    DEFAULT_ROLE = 'DOC_AI_ROLE'
;

GRANT ROLE DOC_AI_ROLE TO USER DOC_AI_USER;
GRANT ROLE DOC_AI_ROLE TO USER ANKURCPATEL;