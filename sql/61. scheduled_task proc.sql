USE DATABASE DOC_AI_DB;
USE SCHEMA SCHEDULED_TASKS;

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

CREATE OR REPLACE TASK DOC_AI_DB.SCHEDULED_TASKS.PROCESS_BATCHES_TASK
    WAREHOUSE = 'DOC_AI_WH'
    SCHEDULE = 'USING CRON */2 * * * * UTC'
AS
DECLARE
    scheduled_task_header_uuid STRING;
    scheduled_task_detail_uuid STRING;
    sql_query STRING;
    sql_query2 STRING;
BEGIN
    --INSERT new SCHEDULED_TASK_HEADER record
    scheduled_task_header_uuid := UUID_STRING();
    sql_query := 'INSERT INTO DOC_AI_DB.SCHEDULED_TASKS.SCHEDULED_TASK_HEADER(ID, START_DATE_TIME, JOB_NAME, CREATED_BY, MODIFIED_BY) 
        VALUES (''' || scheduled_task_header_uuid || ''', CURRENT_TIMESTAMP(), CONCAT(''PROCESS_BATCHES_'',TO_CHAR(CURRENT_TIMESTAMP(), ''YYYYMMDD_HH24MISS_FF3'')), 
        CURRENT_USER(), CURRENT_USER());';
    sql_query2 := 'INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.SQL_HISTORY(SQL_QUERY) VALUES(''' || REPLACE(sql_query,'''', '''''') || ''')';
    EXECUTE IMMEDIATE :sql_query2;


    --INSERT new SCHEDULED_TASK_DETAIL record
    scheduled_task_detail_uuid := UUID_STRING();
    sql_query := 'INSERT INTO DOC_AI_DB.SCHEDULED_TASKS.SCHEDULED_TASK_DETAIL(ID, SCHEDULED_TASK_HEADER_ID, BATCH_HEADER_ID, CREATED_BY, MODIFIED_BY) 
        VALUES (''' || scheduled_task_detail_uuid || ''', ''' || scheduled_task_header_uuid || ''', ''' || current_batch_header_id || ''', CURRENT_USER(), CURRENT_USER())';
    sql_query2 := 'INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.SQL_HISTORY(SQL_QUERY) VALUES(''' || REPLACE(sql_query,'''', '''''') || ''')';
    EXECUTE IMMEDIATE :sql_query2;

    
    sql_query := 'UPDATE DOC_AI_DB.SCHEDULED_TASKS.SCHEDULED_TASK_HEADER SET END_DATE_TIME=CURRENT_TIMESTAMP() WHERE ID = ''' || scheduled_task_header_uuid || '''';
    sql_query2 := 'INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.SQL_HISTORY(SQL_QUERY) VALUES(''' || REPLACE(sql_query,'''', '''''') || ''')';
    EXECUTE IMMEDIATE :sql_query2;
END;

CREATE OR REPLACE PROCEDURE DOC_AI_DB.SCHEDULED_TASKS.PROCESS_BATCHES()
RETURNS STRING
LANGUAGE SQL
AS
DECLARE
    analysis_uuid STRING;
    sql_query STRING;
    sql_query2 STRING;

    c1 CURSOR FOR SELECT BATCH_HEADER_ID, ORGANIZATION_ID, BATCH_PATH
        FROM DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_ORGANIZATION_VIEW
        WHERE BATCH_HEADER_STATUS_CODE = 'U'
        ORDER BY BATCH_HEADER_ID, ORGANIZATION_ID, BATCH_PATH;

    c2 CURSOR FOR SELECT BATCH_HEADER_ID, BATCH_DETAIL_ID, BATCH_PATH, FILE_NAME, 
            SNOWFLAKE_STAGE_NAME, SNOWFLAKE_MODEL_DATABASE, 
            SNOWFLAKE_MODEL_SCHEMA, SNOWFLAKE_MODEL_NAME
        FROM DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_DETAIL_ORGANIZATION_VIEW
        WHERE BATCH_HEADER_STATUS_CODE = 'U'
        AND BATCH_HEADER_ID = ?
        ORDER BY BATCH_HEADER_ID, BATCH_PATH, FILE_NAME;
    
    current_batch_header_id STRING;
    current_organization_id STRING;
    current_batch_path STRING;

    current_batch_detail_id STRING;
    current_file_name STRING;
    current_snowflake_stage_name STRING;
    current_snowflake_model_database STRING;
    current_snowflake_model_schema STRING;
    current_snowflake_model_database_schema STRING;
    current_snowflake_model_name STRING;

    current_full_path_file_name STRING;
    current_full_snowflake_stage_name STRING;
    current_full_snowflake_model_name STRING;

    files_processed INT;
    files_successful INT;
    files_failed INT;
BEGIN
    /* These two lines seem like the wrong way to handle insert and return value */
    --CALL DOC_AI_DB.SCHEDULED_TASKS.INSERT_SCHEDULED_TASK_HEADER();
    --select $1 into :scheduled_task_header_uuid from table(result_scan(last_query_id()));

    EXECUTE IMMEDIATE 'TRUNCATE TABLE DOC_AI_DB.STREAMLIT_SCHEMA.SQL_HISTORY;';
    
    files_processed := 0;
    files_successful := 0;
    files_failed := 0;

    FOR record_loop1 in c1 DO
        current_batch_header_id := record_loop1.BATCH_HEADER_ID;
        current_organization_id := record_loop1.ORGANIZATION_ID;
        current_batch_path := record_loop1.BATCH_PATH;

        --UPDATE BATCH_HEADER to In Progress
        sql_query := 'UPDATE DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER
            SET PROCESSED_START_DATE = CURRENT_TIMESTAMP(), BATCH_HEADER_STATUS_CODE=''I''
            WHERE ID = ''' || current_batch_header_id || ''';';
        sql_query2 := 'INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.SQL_HISTORY(SQL_QUERY) VALUES(''' || REPLACE(sql_query,'''', '''''') || ''')';
        EXECUTE IMMEDIATE :sql_query2;        

        OPEN c2 using (:current_batch_header_id);     
        FOR record_loop2 in c2 DO
            current_batch_detail_id := record_loop2.BATCH_DETAIL_ID;

            sql_query := 'UPDATE DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_DETAIL 
                SET PROCESSED_START_DATE = CURRENT_TIMESTAMP(), BATCH_DETAIL_STATUS_CODE=''I''
                WHERE ID = ''' || current_batch_detail_id || ''';';
            sql_query2 := 'INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.SQL_HISTORY(SQL_QUERY) VALUES(''' || REPLACE(sql_query,'''', '''''') || ''')';
            EXECUTE IMMEDIATE :sql_query2;
            
            current_batch_path := record_loop2.BATCH_PATH;
            current_file_name := record_loop2.FILE_NAME;

            current_snowflake_stage_name := record_loop2.SNOWFLAKE_STAGE_NAME;
            current_snowflake_model_database := record_loop2.SNOWFLAKE_MODEL_DATABASE;
            current_snowflake_model_schema := record_loop2.SNOWFLAKE_MODEL_SCHEMA;
            current_snowflake_model_database_schema := current_snowflake_model_database || '.' || current_snowflake_model_schema;
            current_snowflake_model_name := record_loop2.SNOWFLAKE_MODEL_NAME;

            current_full_path_file_name := current_batch_path || '/' || current_file_name;
            current_full_snowflake_stage_name := '@' || current_snowflake_stage_name;
            current_full_snowflake_model_name := current_snowflake_model_database_schema || '.' || current_snowflake_model_name;

            analysis_uuid := UUID_STRING();

            sql_query := 'USE SCHEMA ' || current_snowflake_model_database_schema || ';';
            sql_query2 := 'INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.SQL_HISTORY(SQL_QUERY) VALUES(''' || REPLACE(sql_query,'''', '''''') || ''')';
            EXECUTE IMMEDIATE :sql_query2;

            sql_query := '
                INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.ANALYSIS(ID, FILE_NAME, FILE_SIZE, LAST_MODIFIED, SNOWFLAKE_FILE_URL, PREDICT, CREATED_BY, MODIFIED_BY)
                with CTE_BASELINE AS
                (
                    SELECT ''' || analysis_uuid || ''' AS ID,
                        RELATIVE_PATH AS FILE_NAME,
                        SIZE AS FILE_SIZE,
                        LAST_MODIFIED,
                        FILE_URL AS SNOWFLAKE_FILE_URL
                    FROM DIRECTORY(' || current_full_snowflake_stage_name || ')
                    WHERE RELATIVE_PATH=''' || current_full_path_file_name || '''
                )
                SELECT ID, FILE_NAME, FILE_SIZE, LAST_MODIFIED, SNOWFLAKE_FILE_URL, ' ||
                    current_full_snowflake_model_name || '!PREDICT(GET_PRESIGNED_URL(' || current_full_snowflake_stage_name || ', FILE_NAME), 1) AS PREDICT,
                    CURRENT_USER(), CURRENT_USER()
                FROM CTE_BASELINE;';
            sql_query2 := 'INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.SQL_HISTORY(SQL_QUERY) VALUES(''' || REPLACE(sql_query,'''', '''''') || ''')';
            EXECUTE IMMEDIATE :sql_query2;

            sql_query := 'UPDATE DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_DETAIL 
                SET PROCESSED_END_DATE = CURRENT_TIMESTAMP(), BATCH_DETAIL_STATUS_CODE=''P''
                WHERE ID = ''' || current_batch_detail_id || ''';';
            sql_query2 := 'INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.SQL_HISTORY(SQL_QUERY) VALUES(''' || REPLACE(sql_query,'''', '''''') || ''')';
            EXECUTE IMMEDIATE :sql_query2;
        END FOR;
        CLOSE C2;

        sql_query := 'UPDATE DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER
            SET PROCESSED_END_DATE = CURRENT_TIMESTAMP(), BATCH_HEADER_STATUS_CODE=''P''
            WHERE ID = ''' || current_batch_header_id || ''';';
        sql_query2 := 'INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.SQL_HISTORY(SQL_QUERY) VALUES(''' || REPLACE(sql_query,'''', '''''') || ''')';
        EXECUTE IMMEDIATE :sql_query2;
    
    END FOR;
    CLOSE C1;
    
    RETURN '1';
END;

ALTER TASK DOC_AI_DB.SCHEDULED_TASKS.PROCESS_BATCHES_TASK RESUME;
ALTER TASK DOC_AI_DB.SCHEDULED_TASKS.PROCESS_BATCHES_TASK SUSPEND;
--CALL DOC_AI_DB.SCHEDULED_TASKS.PROCESS_BATCHES()
--SELECT * FROM DOC_AI_DB.STREAMLIT_SCHEMA.SQL_HISTORY ORDER BY CREATED_ON;