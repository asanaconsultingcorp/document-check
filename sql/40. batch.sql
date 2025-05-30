USE DATABASE DOC_AI_DB;

CREATE OR REPLACE TABLE DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_MODEL
(
    ID VARCHAR(16777216) PRIMARY KEY NOT NULL DEFAULT UUID_STRING(),
    BATCH_HEADER_MODEL_CODE VARCHAR(16777216) UNIQUE ENFORCED,
    BATCH_HEADER_MODEL_LABEL VARCHAR(16777216),
    SNOWFLAKE_STAGE_DATABASE VARCHAR(16777216),
    SNOWFLAKE_STAGE_SCHEMA VARCHAR(16777216),
    SNOWFLAKE_STAGE_NAME VARCHAR(16777216),
    SNOWFLAKE_MODEL_DATABASE VARCHAR(16777216),
    SNOWFLAKE_MODEL_SCHEMA VARCHAR(16777216),
    SNOWFLAKE_MODEL_NAME VARCHAR(16777216),
    ACTIVE_FLAG VARCHAR(16777216) DEFAULT 'N',
    CREATED_ON TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR(16777216),
    MODIFIED_ON TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    MODIFIED_BY VARCHAR(16777216)
)
;

INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_MODEL(BATCH_HEADER_MODEL_CODE, BATCH_HEADER_MODEL_LABEL, SNOWFLAKE_STAGE_DATABASE, SNOWFLAKE_STAGE_SCHEMA, SNOWFLAKE_STAGE_NAME, SNOWFLAKE_MODEL_DATABASE, SNOWFLAKE_MODEL_SCHEMA, SNOWFLAKE_MODEL_NAME, ACTIVE_FLAG, CREATED_BY, MODIFIED_BY) VALUES('CM', 'Contract Model Batch', 'DOC_AI_DB', 'DOC_AI_SCHEMA', 'DOC_AI_STAGE', 'DOC_AI_DB', 'DOC_AI_SCHEMA', 'CONTRACTS_EXT', 'Y', CURRENT_USER(), CURRENT_USER());
INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_MODEL(BATCH_HEADER_MODEL_CODE, BATCH_HEADER_MODEL_LABEL, SNOWFLAKE_STAGE_DATABASE, SNOWFLAKE_STAGE_SCHEMA, SNOWFLAKE_STAGE_NAME, SNOWFLAKE_MODEL_DATABASE, SNOWFLAKE_MODEL_SCHEMA, SNOWFLAKE_MODEL_NAME, ACTIVE_FLAG, CREATED_BY, MODIFIED_BY) VALUES('IR', 'Inspection Report Batch', 'DOC_AI_DB', 'DOC_AI_SCHEMA', 'INSPECTION_STAGE', 'DOC_AI_DB', 'DOC_AI_SCHEMA', 'INSPECTION_REPORT_EXT', 'Y', CURRENT_USER(), CURRENT_USER());
INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_MODEL(BATCH_HEADER_MODEL_CODE, BATCH_HEADER_MODEL_LABEL, SNOWFLAKE_STAGE_DATABASE, SNOWFLAKE_STAGE_SCHEMA, SNOWFLAKE_STAGE_NAME, SNOWFLAKE_MODEL_DATABASE, SNOWFLAKE_MODEL_SCHEMA, SNOWFLAKE_MODEL_NAME, ACTIVE_FLAG, CREATED_BY, MODIFIED_BY) VALUES('IR2', 'Inspection Report Batch V2', 'DOC_AI_DB', 'DOC_AI_SCHEMA', 'BLAH_STAGE', 'DOC_AI_DB', 'DOC_AI_SCHEMA', 'BLAH_MODEL', 'Y', CURRENT_USER(), CURRENT_USER());
INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_MODEL(BATCH_HEADER_MODEL_CODE, BATCH_HEADER_MODEL_LABEL, SNOWFLAKE_STAGE_DATABASE, SNOWFLAKE_STAGE_SCHEMA, SNOWFLAKE_STAGE_NAME, SNOWFLAKE_MODEL_DATABASE, SNOWFLAKE_MODEL_SCHEMA, SNOWFLAKE_MODEL_NAME, ACTIVE_FLAG, CREATED_BY, MODIFIED_BY) VALUES('ACP', 'Ankur Magic Batch', 'DOC_AI_DB', 'DOC_AI_SCHEMA', 'BLAH_STAGE', 'DOC_AI_DB', 'DOC_AI_SCHEMA', 'BLAH_MODEL', 'Y', CURRENT_USER(), CURRENT_USER());
INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_MODEL(BATCH_HEADER_MODEL_CODE, BATCH_HEADER_MODEL_LABEL, SNOWFLAKE_STAGE_DATABASE, SNOWFLAKE_STAGE_SCHEMA, SNOWFLAKE_STAGE_NAME, SNOWFLAKE_MODEL_DATABASE, SNOWFLAKE_MODEL_SCHEMA, SNOWFLAKE_MODEL_NAME, ACTIVE_FLAG, CREATED_BY, MODIFIED_BY) VALUES('ACPN', 'Ankur Inactive Batch', 'DOC_AI_DB', 'DOC_AI_SCHEMA', 'BLAH_STAGE', 'DOC_AI_DB', 'DOC_AI_SCHEMA', 'BLAH_MODEL', 'N', CURRENT_USER(), CURRENT_USER());


CREATE OR REPLACE TABLE DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_STATUS
(
    ID VARCHAR(16777216) PRIMARY KEY NOT NULL DEFAULT UUID_STRING(),
    BATCH_HEADER_STATUS_CODE VARCHAR(16777216) UNIQUE ENFORCED,
    BATCH_HEADER_STATUS VARCHAR(16777216),
    APP_USER_ID_CREATED_BY VARCHAR(16777216),
    APP_USER_ID_MODIFIED_BY VARCHAR(16777216),
    CREATED_ON TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR(16777216),
    MODIFIED_ON TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    MODIFIED_BY VARCHAR(16777216)
)
;

INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_STATUS(BATCH_HEADER_STATUS_CODE, BATCH_HEADER_STATUS, CREATED_BY, MODIFIED_BY) VALUES('U', 'Unprocessed', CURRENT_USER(), CURRENT_USER());
INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_STATUS(BATCH_HEADER_STATUS_CODE, BATCH_HEADER_STATUS, CREATED_BY, MODIFIED_BY) VALUES('I', 'In Progress', CURRENT_USER(), CURRENT_USER());
INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_STATUS(BATCH_HEADER_STATUS_CODE, BATCH_HEADER_STATUS, CREATED_BY, MODIFIED_BY) VALUES('P', 'Processed', CURRENT_USER(), CURRENT_USER());
INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_STATUS(BATCH_HEADER_STATUS_CODE, BATCH_HEADER_STATUS, CREATED_BY, MODIFIED_BY) VALUES('UC', 'Unprocessed Cancelled', CURRENT_USER(), CURRENT_USER());
INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_STATUS(BATCH_HEADER_STATUS_CODE, BATCH_HEADER_STATUS, CREATED_BY, MODIFIED_BY) VALUES('PC', 'Processed Cancelled', CURRENT_USER(), CURRENT_USER());


CREATE OR REPLACE TABLE DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_DETAIL_STATUS
(
    ID VARCHAR(16777216) PRIMARY KEY NOT NULL DEFAULT UUID_STRING(),
    BATCH_DETAIL_STATUS_CODE VARCHAR(16777216) UNIQUE ENFORCED,
    BATCH_DETAIL_STATUS VARCHAR(16777216),
    APP_USER_ID_CREATED_BY VARCHAR(16777216),
    APP_USER_ID_MODIFIED_BY VARCHAR(16777216),
    CREATED_ON TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR(16777216),
    MODIFIED_ON TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    MODIFIED_BY VARCHAR(16777216)
)
;

INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_DETAIL_STATUS(BATCH_DETAIL_STATUS_CODE, BATCH_DETAIL_STATUS, CREATED_BY, MODIFIED_BY) VALUES('U', 'Unprocessed', CURRENT_USER(), CURRENT_USER());
INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_DETAIL_STATUS(BATCH_DETAIL_STATUS_CODE, BATCH_DETAIL_STATUS, CREATED_BY, MODIFIED_BY) VALUES('I', 'In Progress', CURRENT_USER(), CURRENT_USER());
INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_DETAIL_STATUS(BATCH_DETAIL_STATUS_CODE, BATCH_DETAIL_STATUS, CREATED_BY, MODIFIED_BY) VALUES('P', 'Processed', CURRENT_USER(), CURRENT_USER());
INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_DETAIL_STATUS(BATCH_DETAIL_STATUS_CODE, BATCH_DETAIL_STATUS, CREATED_BY, MODIFIED_BY) VALUES('UC', 'Unprocessed Cancelled', CURRENT_USER(), CURRENT_USER());
INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_DETAIL_STATUS(BATCH_DETAIL_STATUS_CODE, BATCH_DETAIL_STATUS, CREATED_BY, MODIFIED_BY) VALUES('PC', 'Processed Cancelled', CURRENT_USER(), CURRENT_USER());


CREATE OR REPLACE TABLE DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER
(
    ID VARCHAR(16777216) PRIMARY KEY NOT NULL DEFAULT UUID_STRING(),
    ORGANIZATION_ID VARCHAR(16777216),
    USER_BATCH_NAME VARCHAR(16777216),
    SYSTEM_BATCH_NAME VARCHAR(16777216),
    BATCH_PATH VARCHAR(16777216),
    BATCH_HEADER_STATUS_CODE VARCHAR(16777216),
    BATCH_HEADER_MODEL_CODE VARCHAR(16777216),
    CANCELLED_DATE TIMESTAMP,
    PROCESSED_START_DATE TIMESTAMP,
    PROCESSED_END_DATE TIMESTAMP,
    APP_USER_ID_CREATED_BY VARCHAR(16777216),
    APP_USER_ID_MODIFIED_BY VARCHAR(16777216),
    CREATED_ON TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR(16777216),
    MODIFIED_ON TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    MODIFIED_BY VARCHAR(16777216)
)
;

CREATE OR REPLACE TABLE DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_DETAIL
(
    ID VARCHAR(16777216) PRIMARY KEY NOT NULL DEFAULT UUID_STRING(),
    BATCH_HEADER_ID VARCHAR(16777216),
    FILE_NAME VARCHAR(16777216),
    FILE_TYPE VARCHAR(16777216),
    FILE_SIZE INTEGER,
    BATCH_DETAIL_STATUS_CODE VARCHAR(16777216),
    CANCELLED_DATE TIMESTAMP,
    PROCESSED_START_DATE TIMESTAMP,
    PROCESSED_END_DATE TIMESTAMP,
    APP_USER_ID_CREATED_BY VARCHAR(16777216),
    APP_USER_ID_MODIFIED_BY VARCHAR(16777216),
    CREATED_ON TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR(16777216),
    MODIFIED_ON TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    MODIFIED_BY VARCHAR(16777216)
)
;


CREATE OR REPLACE VIEW DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_DETAIL_ORGANIZATION_VIEW AS
SELECT BH.ID AS BATCH_HEADER_ID, BH.BATCH_HEADER_STATUS_CODE,  BHS.BATCH_HEADER_STATUS,
    BH.BATCH_HEADER_MODEL_CODE, BHM.BATCH_HEADER_MODEL_LABEL,
    BHM.SNOWFLAKE_STAGE_DATABASE, BHM.SNOWFLAKE_STAGE_SCHEMA, BHM.SNOWFLAKE_STAGE_NAME,
    BHM.SNOWFLAKE_MODEL_DATABASE, BHM.SNOWFLAKE_MODEL_SCHEMA, BHM.SNOWFLAKE_MODEL_NAME,
    BH.ORGANIZATION_ID, O.ORGANIZATION_NAME, 
    BH.USER_BATCH_NAME, BH.SYSTEM_BATCH_NAME, BH.BATCH_PATH, 
    BD.ID AS BATCH_DETAIL_ID, BD.FILE_NAME, BD.FILE_TYPE, BD.FILE_SIZE, 
    BD.BATCH_DETAIL_STATUS_CODE, BDS.BATCH_DETAIL_STATUS,
    BH.CANCELLED_DATE, BH.PROCESSED_START_DATE, BH.PROCESSED_END_DATE, BH.CREATED_ON AS BATCH_CREATED_ON
FROM DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER BH
LEFT JOIN DOC_AI_DB.SECURITY_SCHEMA.ORGANIZATIONS O ON BH.ORGANIZATION_ID=O.ID
LEFT JOIN DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_MODEL BHM ON BH.BATCH_HEADER_MODEL_CODE=BHM.BATCH_HEADER_MODEL_CODE
LEFT JOIN DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_STATUS BHS ON BH.BATCH_HEADER_STATUS_CODE=BHS.BATCH_HEADER_STATUS_CODE
LEFT JOIN DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_DETAIL BD ON BH.ID=BD.BATCH_HEADER_ID
LEFT JOIN DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_DETAIL_STATUS BDS ON BD.BATCH_DETAIL_STATUS_CODE=BDS.BATCH_DETAIL_STATUS_CODE
;

CREATE OR REPLACE VIEW DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_ORGANIZATION_VIEW AS
SELECT BATCH_HEADER_ID, BATCH_HEADER_STATUS_CODE, BATCH_HEADER_STATUS,
    BATCH_HEADER_MODEL_CODE, BATCH_HEADER_MODEL_LABEL, 
    SNOWFLAKE_STAGE_DATABASE, SNOWFLAKE_STAGE_SCHEMA, SNOWFLAKE_STAGE_NAME,
    SNOWFLAKE_MODEL_DATABASE, SNOWFLAKE_MODEL_SCHEMA, SNOWFLAKE_MODEL_NAME,
    ORGANIZATION_ID, ORGANIZATION_NAME, 
    USER_BATCH_NAME, SYSTEM_BATCH_NAME, BATCH_PATH, 
    CANCELLED_DATE, PROCESSED_START_DATE, PROCESSED_END_DATE, BATCH_CREATED_ON, 
    COUNT(DISTINCT BATCH_DETAIL_ID) AS BATCH_COUNT
FROM DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_DETAIL_ORGANIZATION_VIEW
GROUP BY BATCH_HEADER_ID, BATCH_HEADER_STATUS_CODE, BATCH_HEADER_STATUS,
    BATCH_HEADER_MODEL_CODE, BATCH_HEADER_MODEL_LABEL, 
    SNOWFLAKE_STAGE_DATABASE, SNOWFLAKE_STAGE_SCHEMA, SNOWFLAKE_STAGE_NAME,
    SNOWFLAKE_MODEL_DATABASE, SNOWFLAKE_MODEL_SCHEMA, SNOWFLAKE_MODEL_NAME,
    ORGANIZATION_ID, ORGANIZATION_NAME, 
    USER_BATCH_NAME, SYSTEM_BATCH_NAME, BATCH_PATH, 
    CANCELLED_DATE, PROCESSED_START_DATE, PROCESSED_END_DATE, BATCH_CREATED_ON
;

/*
SELECT * FROM DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER;
SELECT * FROM DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_STATUS;
SELECT * FROM DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_MODEL;
SELECT * FROM DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_DETAIL;
SELECT * FROM DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_DETAIL_ORGANIZATION_VIEW
SELECT * FROM DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_ORGANIZATION_VIEW
*/