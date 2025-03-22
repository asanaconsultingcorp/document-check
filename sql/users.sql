USE DATABASE DOC_AI_DB;

CREATE OR REPLACE TABLE DOC_AI_DB.SECURITY_SCHEMA.ORGANIZATIONS
(
    id VARCHAR(16777216) PRIMARY KEY NOT NULL DEFAULT UUID_STRING(),
    organization_name VARCHAR
)
;

CREATE OR REPLACE TABLE DOC_AI_DB.SECURITY_SCHEMA.USERS
(
    id VARCHAR(16777216) PRIMARY KEY NOT NULL DEFAULT UUID_STRING(),
    organization_id VARCHAR(16777216),
    user_name VARCHAR,
    password VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    email_address VARCHAR
)
;


INSERT INTO DOC_AI_DB.SECURITY_SCHEMA.ORGANIZATIONS(organization_name) VALUES('Alpha Corporation');
INSERT INTO DOC_AI_DB.SECURITY_SCHEMA.ORGANIZATIONS(organization_name) VALUES('Beta Organization');

/*
SELECT *
FROM DOC_AI_DB.SECURITY_SCHEMA.ORGANIZATIONS
;
*/

INSERT INTO DOC_AI_DB.SECURITY_SCHEMA.USERS(organization_id, user_name, password, first_name, last_name, email_address)
SELECT o.id, 'apatel', 'password', 'Ankur', 'Patel', 'ankurcp@gmail.com'
FROM DOC_AI_DB.SECURITY_SCHEMA.ORGANIZATIONS o
WHERE o.organization_name = 'Alpha Corporation'
;

INSERT INTO DOC_AI_DB.SECURITY_SCHEMA.USERS(organization_id, user_name, password, first_name, last_name, email_address)
SELECT o.id, 'spatel', 'password', 'Sinal', 'Patel', 'sinal_patel@hotmail.com'
FROM DOC_AI_DB.SECURITY_SCHEMA.ORGANIZATIONS o
WHERE o.organization_name = 'Beta Organization'
;

/*
SELECT *
FROM DOC_AI_DB.SECURITY_SCHEMA.USERS
;
*/


CREATE OR REPLACE VIEW DOC_AI_DB.SECURITY_SCHEMA.USERS_ORGANIZATIONS AS
SELECT u.id as user_id, u.user_name, u.password, u.first_name, u.last_name, u.email_address, o.organization_name 
FROM DOC_AI_DB.SECURITY_SCHEMA.USERS u
JOIN DOC_AI_DB.SECURITY_SCHEMA.ORGANIZATIONS o on u.organization_id=o.id
;

/*
SELECT * 
FROM DOC_AI_DB.SECURITY_SCHEMA.USERS_ORGANIZATIONS
*/