USE DATABASE DOC_AI_DB;
USE SCHEMA SCHEDULED_TASKS;

CREATE OR REPLACE TABLE DOC_AI_DB.SCHEDULED_TASKS.SCHEDULED_TASK_MANIFEST
(
    ID VARCHAR(16777216) PRIMARY KEY NOT NULL DEFAULT UUID_STRING(),
    JOB_NAME VARCHAR(16777216),
    START_DATE_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    END_DATE_TIME TIMESTAMP,
    FILES_PROCESSED INTEGER,
    FILES_SUCCESSFUL INTEGER,
    FILES_FAILED INTEGER,
    CREATED_ON TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR(16777216),
    MODIFIED_ON TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    MODIFIED_BY VARCHAR(16777216)
)
;


CREATE OR REPLACE TASK DOC_AI_DB.SCHEDULED_TASKS.PROCESS_BATCHES
    WAREHOUSE = 'DOC_AI_WH'
    SCHEDULE = 'USING CRON */2 * * * * UTC'
AS
DECLARE
    job_uuid STRING;
    sql_query STRING;

/*
BATCH_HEADER_ID, BATCH_HEADER_STATUS_CODE, BATCH_HEADER_STATUS, ORGANIZATION_ID, ORGANIZATION_NAME, 
    BATCH_NAME, BATCH_PATH, CANCELLED_DATE, PROCESSED_DATE, BATCH_CREATED_ON, COUNT(DISTINCT BATCH_DETAIL_ID) AS BATCH_COUNT
*/
    /*
    c1 CURSOR FOR SELECT BATCH_HEADER_ID, BATCH_DETAIL_ID, BATCH_PATH, FILE_NAME 
        FROM DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_DETAIL_ORGANIZATION_VIEW
        WHERE BATCH_HEADER_STATUS_CODE = 'U'
        ORDER BY BATCH_HEADER_ID, BATCH_PATH, FILE_NAME;
    */
    
    c1 CURSOR FOR SELECT BATCH_HEADER_ID, ORGANIZATION_ID, BATCH_PATH
        FROM DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_ORGANIZATION_VIEW
        WHERE BATCH_HEADER_STATUS_CODE = 'U'
        ORDER BY BATCH_HEADER_ID, ORGANIZATION_ID, BATCH_PATH;

    c2 CURSOR FOR SELECT BATCH_HEADER_ID, BATCH_DETAIL_ID, BATCH_PATH, FILE_NAME 
        FROM DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_DETAIL_ORGANIZATION_VIEW
        WHERE BATCH_HEADER_STATUS_CODE = 'U'
        AND BATCH_HEADER_ID = ?
        ORDER BY BATCH_HEADER_ID, BATCH_PATH, FILE_NAME;
    
    current_batch_header_id STRING;
    current_organization_id STRING;
    current_batch_path STRING;
    current_batch_detail_id STRING;
    current_batch_file_name STRING;

    files_processed INT;
    files_successful INT;
    files_failed INT;
BEGIN
    /* These two lines seem like the wrong way to handle insert and return value */
    --CALL DOC_AI_DB.SCHEDULED_TASKS.INSERT_SCHEDULED_TASK_MANIFEST();
    --select $1 into :job_uuid from table(result_scan(last_query_id()));

    --INSERT INTO SCHEDULED_TASK_MANIFEST
    job_uuid := UUID_STRING();
    sql_query := 'INSERT INTO DOC_AI_DB.SCHEDULED_TASKS.SCHEDULED_TASK_MANIFEST(ID, START_DATE_TIME, JOB_NAME, CREATED_BY, MODIFIED_BY) 
        VALUES (''' || job_uuid || ''', CURRENT_TIMESTAMP(), CONCAT(''PROCESS_BATCHES_'',TO_CHAR(CURRENT_TIMESTAMP(), ''YYYYMMDD_HH24MISS_FF3'')), 
        CURRENT_USER(), CURRENT_USER())';
    EXECUTE IMMEDIATE :sql_query;

    files_processed := 0;
    files_successful := 0;
    files_failed := 0;

    FOR record_loop1 in c1 DO
        current_batch_header_id := record_loop1.BATCH_HEADER_ID;
        current_organization_id := record_loop1.ORGANIZATION_ID;
        current_batch_path := record_loop1.BATCH_PATH;

        sql_query := 'UPDATE DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER
            SET PROCESSED_START_DATE = CURRENT_TIMESTAMP(), BATCH_HEADER_STATUS_CODE=''I''
            WHERE ID = ''' || current_batch_header_id || ''';';
        EXECUTE IMMEDIATE :sql_query;

        OPEN c2 using (:current_batch_header_id);        
        FOR record_loop2 in c2 DO
            current_batch_detail_id := record_loop2.BATCH_DETAIL_ID;
            current_batch_file_name := record_loop2.FILE_NAME;

            sql_query := 'UPDATE DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_DETAIL 
                SET PROCESSED_START_DATE = CURRENT_TIMESTAMP(), BATCH_DETAIL_STATUS_CODE=''I''
                WHERE ID = ''' || current_batch_detail_id || ''';';
            EXECUTE IMMEDIATE :sql_query;

            


            sql_query := 'UPDATE DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_DETAIL 
                SET PROCESSED_END_DATE = CURRENT_TIMESTAMP(), BATCH_DETAIL_STATUS_CODE=''P''
                WHERE ID = ''' || current_batch_detail_id || ''';';
            EXECUTE IMMEDIATE :sql_query;
        END FOR;

        sql_query := 'UPDATE DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER
            SET PROCESSED_END_DATE = CURRENT_TIMESTAMP(), BATCH_HEADER_STATUS_CODE=''P''
            WHERE ID = ''' || current_batch_header_id || ''';';
        EXECUTE IMMEDIATE :sql_query;
    
    END FOR;


    /*
    WITH CTE_STAGED AS
    (
        SELECT 
            TRIM(SPLIT(RELATIVE_PATH, '/')[0], '"') AS BATCH_NAME,
            TRIM(SPLIT(RELATIVE_PATH, '/')[1], '"') AS FILE_NAME,
            RELATIVE_PATH AS FULL_FILE_NAME_PATH,
            SIZE AS FILE_SIZE,
            LAST_MODIFIED,
            FILE_URL AS SNOWFLAKE_FILE_URL,
            DOC_AI_DB.DOC_AI_SCHEMA.DOC_AI_CO_BRANDING!PREDICT(GET_PRESIGNED_URL('@DOC_AI_DB.DOC_AI_SCHEMA.DOC_AI_STAGE', RELATIVE_PATH), 1) AS PREDICT
        FROM DIRECTORY(@DOC_AI_DB.DOC_AI_SCHEMA.DOC_AI_STAGE)
    )
    SELECT
        BATCH_NAME,
        FILE_NAME,
        FULL_FILE_NAME_PATH,
        FILE_SIZE,
        LAST_MODIFIED,
        SNOWFLAKE_FILE_URL,
        predict:"duration"[0].value::varchar as DURATION_VALUE,
        predict:"duration"[0].score::float as DURATION_SCORE,   
        predict:"effective_date"[0].value::varchar as EFFECTIVE_DATE_VALUE,
        predict:"effective_date"[0].score::float as EFFECTIVE_DATE_SCORE,
        predict:"force_majeure"[0].value::varchar as FORCE_MAJEURE_VALUE,
        predict:"force_majeure"[0].score::float as FORCE_MAJEURE_SCORE,
        predict:"indemnification_clause"[0].value::varchar as INDEMNIFICATION_CLAUSE_VALUE,
        predict:"indemnification_clause"[0].score::float as INDEMNIFICATION_CLAUSE_SCORE,
        predict:"notice_period"[0].value::varchar as NOTICE_PERIOD_VALUE,
        predict:"notice_period"[0].score::float as NOTICE_PERIOD_SCORE,
        predict:"parties"[0].value::varchar as PARTY1_VALUE,
        predict:"parties"[0].score::float as PARTY1_SCORE,
        predict:"parties"[1].value::varchar as PARTY2_VALUE,
        predict:"parties"[1].score::float as PARTY2_SCORE,
        predict:"payment_terms"[0].value::varchar as PAYMENT_TERMS_VALUE,
        predict:"payment_terms"[0].score::float as PAYMENT_TERMS_SCORE,
        predict:"renewal_options"[0].value::varchar as RENEWAL_OPTIONS_VALUE,
        predict:"renewal_options"[0].score::float as RENEWAL_OPTIONS_SCORE
    FROM CTE_STAGED
    */

    sql_query := 'UPDATE DOC_AI_DB.SCHEDULED_TASKS.SCHEDULED_TASK_MANIFEST SET END_DATE_TIME=CURRENT_TIMESTAMP() WHERE ID = ''' || job_uuid || '''';
    EXECUTE IMMEDIATE :sql_query;
END;

ALTER TASK DOC_AI_DB.SCHEDULED_TASKS.PROCESS_BATCHES RESUME;
ALTER TASK DOC_AI_DB.SCHEDULED_TASKS.PROCESS_BATCHES SUSPEND;