--GIT SETUP

use role sysadmin;
CREATE DATABASE IF NOT EXISTS AI_EXTRACT_DEMOS;
USE DATABASE AI_EXTRACT_DEMOS;
CREATE SCHEMA IF NOT EXISTS EXTRACT_ANYTHING;
USE SCHEMA AI_EXTRACT_DEMOS.EXTRACT_ANYTHING;

use role accountadmin;
  
CREATE OR REPLACE API INTEGRATION git_sfc_gh_nrinard_api_integration
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/sfc-gh-nrinard')
    ENABLED = TRUE;

use role sysadmin;
-- Create Git repository integration for the public demo repository
CREATE OR REPLACE GIT REPOSITORY AI_EXTRACT_PUBLIC
    API_INTEGRATION = git_sfc_gh_nrinard_api_integration
    ORIGIN = 'https://github.com/sfc-gh-nrinard/AI_EXTRACT-public.git';

ALTER GIT REPOSITORY AI_EXTRACT_PUBLIC FETCH;

-- Create internal stage for copied data files
CREATE OR REPLACE STAGE STREAMLIT_STAGE
    COMMENT = 'Internal stage for Streamlit app files'
    DIRECTORY = ( ENABLE = TRUE)
    ENCRYPTION = (   TYPE = 'SNOWFLAKE_FULL');

CREATE OR REPLACE STAGE DOCS_ROUTER_STAGE
COMMENT = 'Internal stage for data files'
  DIRECTORY = (ENABLE = TRUE)
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

 CREATE OR REPLACE STREAM DOCS_ROUTER_STREAM ON STAGE DOCS_ROUTER_STAGE;

-- Single RAW table capturing classification and extracted JSON
CREATE OR REPLACE TABLE RAW (
  file_name          VARCHAR,
  file_url           VARCHAR,
  document_type      VARCHAR,       -- dynamic, from DOC_TYPES or 'NO_MATCH'
  extract_json       VARIANT,       -- raw AI_EXTRACT result
  validation_json    VARIANT,       -- tier-1 AI_COMPLETE validation result
  approved           BOOLEAN DEFAULT FALSE,
  approved_at        TIMESTAMP_NTZ,
  created_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Dynamic document types registry
CREATE OR REPLACE TABLE DOC_TYPES (
  document_type   VARCHAR,
  description     VARCHAR,
  created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Prompts per document type (drives AI_EXTRACT responseFormat)
CREATE OR REPLACE TABLE DOC_TYPE_PROMPTS (
  document_type     VARCHAR,
  field_name        VARCHAR,
  retrieval_prompt  VARCHAR,
  sort_order        NUMBER(38,0) DEFAULT 0,
  created_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);




CREATE OR REPLACE PROCEDURE PROCESS_RAW()
RETURNS STRING
LANGUAGE SQL
AS $$
DECLARE
  v_count NUMBER := 0;
  v_file_name STRING;
  c CURSOR FOR
    SELECT RELATIVE_PATH AS file_name
    FROM DOCS_ROUTER_STREAM
    WHERE METADATA$ACTION = 'INSERT';
BEGIN
  FOR rec IN c DO
    v_file_name := rec.file_name;
    CALL PROCESS_ONE_FILE(:v_file_name);
    v_count := v_count + 1;
  END FOR;

  RETURN CONCAT('OK (processed ', v_count, ' file(s))');
END;
$$;

CREATE WAREHOUSE IF NOT EXISTS AI_EXTRACT_XS_WH
  WAREHOUSE_SIZE = 'XSMALL'
  COMMENT = 'Snowpark warehouse for ingestion';

-- (Optional) A task can be added later to automate processing when new files arrive

-- ========================================================================
-- COPY DATA FROM GIT TO INTERNAL STAGE
-- ========================================================================
COPY FILES
INTO @STREAMLIT_STAGE/
FROM @AI_EXTRACT_DEMOS.EXTRACT_ANYTHING.AI_EXTRACT_PUBLIC/branches/main/app/;

COPY FILES
INTO @DOCS_ROUTER_STAGE
FROM @AI_EXTRACT_DEMOS.EXTRACT_ANYTHING.AI_EXTRACT_PUBLIC/branches/main/extraction_documents/pre-load;

CREATE OR REPLACE STREAMLIT AI_EXTRACT_ANYTHING
ROOT_LOCATION = '@AI_EXTRACT_DEMOS.EXTRACT_ANYTHING.STREAMLIT_STAGE'
MAIN_FILE = 'streamlit_main.py'
QUERY_WAREHOUSE = AI_EXTRACT_XS_WH;

ALTER STAGE DOCS_ROUTER_STAGE REFRESH;

-- ========================================================================
-- Seed dynamic doc types and prompts for existing PERMIT and CONTRACTOR
-- ========================================================================
INSERT INTO DOC_TYPES (document_type, description)
SELECT 'PERMIT', 'Building Permit Request' UNION ALL
SELECT 'CONTRACTOR', 'Contractor License Application';

-- ========================================================================
-- CRUD Procedures for App (Types, Prompts, Single-file processing, Approve)
-- ========================================================================

CREATE OR REPLACE PROCEDURE UPSERT_DOC_TYPE(p_doc_type VARCHAR, p_description VARCHAR)
RETURNS STRING
LANGUAGE SQL
AS $$
BEGIN
  MERGE INTO DOC_TYPES t
  USING (SELECT :p_doc_type AS document_type, :p_description AS description) s
  ON t.document_type = s.document_type
  WHEN NOT MATCHED THEN INSERT (document_type, description) VALUES (s.document_type, s.description);
  RETURN 'OK';
END;
$$;

CREATE OR REPLACE PROCEDURE REPLACE_PROMPTS(p_doc_type VARCHAR, p_prompts VARIANT)
RETURNS STRING
LANGUAGE SQL
AS $$
BEGIN
  DELETE FROM DOC_TYPE_PROMPTS WHERE document_type = :p_doc_type;
  INSERT INTO DOC_TYPE_PROMPTS (document_type, field_name, retrieval_prompt, sort_order)
  SELECT :p_doc_type,
         TRIM((value:field_name)::STRING),
         (value:retrieval_prompt)::STRING,
         TRY_TO_NUMBER((value:sort_order)::STRING)
  FROM TABLE(FLATTEN(input => :p_prompts))
  WHERE TRIM((value:field_name)::STRING) IS NOT NULL
    AND TRIM((value:field_name)::STRING) <> ''
    AND (value:retrieval_prompt) IS NOT NULL;
  RETURN 'OK';
END;
$$;

CREATE OR REPLACE PROCEDURE PROCESS_ONE_FILE(p_file_name VARCHAR)
RETURNS STRING
LANGUAGE SQL
AS $$
BEGIN
  INSERT INTO RAW (file_name, file_url, document_type, extract_json)
  WITH input AS (
    SELECT :p_file_name AS file_name
  ),
  types_list AS (
    SELECT LISTAGG(document_type, ', ') WITHIN GROUP (ORDER BY document_type) AS type_list
    FROM DOC_TYPES
  ),
  classified AS (
    SELECT i.file_name,
           GET_PRESIGNED_URL('@DOCS_ROUTER_STAGE', i.file_name) AS file_url,
           UPPER(
             AI_EXTRACT(
               file => TO_FILE('@DOCS_ROUTER_STAGE', i.file_name),
               responseFormat => [[
                 'document_type',
                 'Select the best matching document type from this list: ' || COALESCE((SELECT type_list FROM types_list), '') || '. If none match, return NO_MATCH. Return only the label.'
               ]]
             ):response.document_type::VARCHAR
           ) AS document_type
    FROM input i
  ),
  prompts AS (
    SELECT document_type,
           ARRAY_AGG(ARRAY_CONSTRUCT(field_name, retrieval_prompt)) WITHIN GROUP (ORDER BY sort_order, field_name) AS rf,
           ARRAY_AGG(field_name) WITHIN GROUP (ORDER BY sort_order, field_name) AS fields
    FROM DOC_TYPE_PROMPTS
    GROUP BY document_type
  )
  SELECT c.file_name,
         c.file_url,
         c.document_type,
         IFF(
           c.document_type = 'NO_MATCH',
           OBJECT_CONSTRUCT(),
           IFF(
             p.rf IS NULL OR ARRAY_SIZE(p.rf) = 0,
             OBJECT_CONSTRUCT('warning','NO_PROMPTS_CONFIGURED'),
             AI_EXTRACT(
               file => TO_FILE('@DOCS_ROUTER_STAGE', c.file_name),
               responseFormat => p.rf
             )
           )
         ) AS extract_json
  FROM classified c
  LEFT JOIN prompts p
    ON p.document_type = c.document_type;

  -- Tier-1 validation
  UPDATE RAW t
  SET validation_json = v.vjson
  FROM (
    SELECT s.file_name,
           IFF(
             s.document_type = 'NO_MATCH' OR s.fields IS NULL OR ARRAY_SIZE(s.fields) = 0,
             OBJECT_CONSTRUCT('status','skipped','reason','no document type or prompts'),
             AI_COMPLETE(
               model => 'mistral-large',
               prompt => CONCAT(
                 'You are a strict validator for extracted document data. Given a document type description, the list of fields requested, and the extracted JSON, answer ONLY with a compact JSON object with keys \"valid\" (boolean) and \"notes\" (string). Be conservative when fields conflict with the description or the prompts. Do not add extra keys.\n\nInput as JSON:\n',
                 TO_JSON(OBJECT_CONSTRUCT(
                   'document_type', s.document_type,
                   'description', COALESCE(s.description, ''),
                   'fields', s.fields,
                   'extracted', s.extract_json
                 ))
               ),
               model_parameters => OBJECT_CONSTRUCT('temperature', 0),
               response_format => OBJECT_CONSTRUCT(
                 'type','json',
                 'schema', PARSE_JSON('{"type":"object","properties":{"valid":{"type":"boolean"},"notes":{"type":"string"}},"required":["valid","notes"]}')
               )
             )
           ) AS vjson
    FROM (
      SELECT r.file_name, r.document_type, r.extract_json, d.description, p.fields
      FROM RAW r
      LEFT JOIN DOC_TYPES d ON d.document_type = r.document_type
      LEFT JOIN (
        SELECT document_type,
               ARRAY_AGG(field_name) WITHIN GROUP (ORDER BY sort_order, field_name) AS fields
        FROM DOC_TYPE_PROMPTS
        GROUP BY document_type
      ) p ON p.document_type = r.document_type
      WHERE r.file_name = :p_file_name
    ) s
  ) v
  WHERE t.file_name = v.file_name;

  RETURN 'OK';
END;
$$;

CREATE OR REPLACE PROCEDURE APPROVE_RECORD(p_file_name VARCHAR, p_approved_json VARIANT)
RETURNS STRING
LANGUAGE SQL
AS $$
BEGIN
  UPDATE RAW
  SET extract_json = :p_approved_json,
      approved = TRUE,
      approved_at = CURRENT_TIMESTAMP()
  WHERE file_name = :p_file_name;
  RETURN 'OK';
END;
$$;

-- Seed PERMIT prompts
INSERT INTO DOC_TYPE_PROMPTS (document_type, field_name, retrieval_prompt, sort_order)
SELECT 'PERMIT','applicant_name','Applicant full name',1 UNION ALL
SELECT 'PERMIT','company_name','Company name',2 UNION ALL
SELECT 'PERMIT','phone','Phone number',3 UNION ALL
SELECT 'PERMIT','email','Email address',4 UNION ALL
SELECT 'PERMIT','project_address','Project address',5 UNION ALL
SELECT 'PERMIT','parcel_number','Parcel number',6 UNION ALL
SELECT 'PERMIT','permit_types','Which license types have check marks next to them? List the permit types as an array of strings (e.g., building, electrical, plumbing, mechanical)',7 UNION ALL
SELECT 'PERMIT','work_description','What is the work description?',8 UNION ALL
SELECT 'PERMIT','estimated_cost','Estimated cost as a number',9 UNION ALL
SELECT 'PERMIT','contractor_license','Contractor license number',10 UNION ALL
SELECT 'PERMIT','owner_name','Owner name',11 UNION ALL
SELECT 'PERMIT','affirmation','Does the applicant affirm that the information provided is correct (true/false)',12 UNION ALL
SELECT 'PERMIT','has_signature','Has signature (true/false)',13 UNION ALL
SELECT 'PERMIT','signed_date','What date was the document signed?',14 UNION ALL
SELECT 'PERMIT','approved_by','Who approved this building permit?',15 UNION ALL
SELECT 'PERMIT','has_seal','Is there a snowflake shaped seal present on the permit (YES/NO)?',16;

-- Seed CONTRACTOR prompts
INSERT INTO DOC_TYPE_PROMPTS (document_type, field_name, retrieval_prompt, sort_order)
SELECT 'CONTRACTOR','applicant_name','Applicant full name',1 UNION ALL
SELECT 'CONTRACTOR','business_name','Business name (DBA)',2 UNION ALL
SELECT 'CONTRACTOR','business_address','Business address',3 UNION ALL
SELECT 'CONTRACTOR','city','City',4 UNION ALL
SELECT 'CONTRACTOR','state','State',5 UNION ALL
SELECT 'CONTRACTOR','zip','ZIP code',6 UNION ALL
SELECT 'CONTRACTOR','phone','Phone number',7 UNION ALL
SELECT 'CONTRACTOR','email','Email address',8 UNION ALL
SELECT 'CONTRACTOR','license_types','List the license types as an array of strings (e.g., general, electrical, plumbing, hvac, roofing)',9 UNION ALL
SELECT 'CONTRACTOR','fein_ssn','FEIN or SSN',10 UNION ALL
SELECT 'CONTRACTOR','insurance_provider','Insurance provider',11 UNION ALL
SELECT 'CONTRACTOR','policy_number','Policy number',12 UNION ALL
SELECT 'CONTRACTOR','wc_yes','Workers compensation coverage (true/false)',13 UNION ALL
SELECT 'CONTRACTOR','bonded','Bonded (true/false)',14 UNION ALL
SELECT 'CONTRACTOR','years_experience','Years of experience as a number',15 UNION ALL
SELECT 'CONTRACTOR','reference_1','Reference 1 (Name / Phone)',16 UNION ALL
SELECT 'CONTRACTOR','reference_2','Reference 2 (Name / Phone)',17 UNION ALL
SELECT 'CONTRACTOR','reference_3','Reference 3 (Name / Phone)',18 UNION ALL
SELECT 'CONTRACTOR','affirmation','Does the applicant affirm that the information provided is correct (true/false)',19 UNION ALL
SELECT 'CONTRACTOR','has_signature','Has signature (true/false)',20 UNION ALL
SELECT 'CONTRACTOR','signed_date','What date was the document signed?',21 UNION ALL
SELECT 'CONTRACTOR','approved_by','Who approved this application?',22 UNION ALL
SELECT 'CONTRACTOR','approved_date','What is the approved date, it will be found on a label/stamp/sticker. Likely towards the bottom of the page. It has a blue background.',23;

CALL PROCESS_RAW();