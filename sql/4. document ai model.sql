--CREATE MODEL MANUALLY

DROP TABLE IF EXISTS DOC_AI_DB.DOC_AI_SCHEMA.STAGED_FILES;

CREATE TABLE DOC_AI_DB.DOC_AI_SCHEMA.STAGED_FILES AS
SELECT 
    TRIM(SPLIT(RELATIVE_PATH, '/')[0], '"') AS BATCH_NAME,
    TRIM(SPLIT(RELATIVE_PATH, '/')[1], '"') AS FILE_NAME,
    RELATIVE_PATH AS FULL_FILE_NAME_PATH,
    SIZE AS FILE_SIZE,
    LAST_MODIFIED,
    FILE_URL AS SNOWFLAKE_FILE_URL,
    DOC_AI_DB.DOC_AI_SCHEMA.DOC_AI_CO_BRANDING!PREDICT(GET_PRESIGNED_URL('@DOC_AI_STAGE', RELATIVE_PATH), 1) AS PREDICT
FROM DIRECTORY(@DOC_AI_STAGE)
;
    

SELECT *
FROM DOC_AI_DB.DOC_AI_SCHEMA.STAGED_FILES
;

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
FROM DOC_AI_DB.DOC_AI_SCHEMA.STAGED_FILES
;