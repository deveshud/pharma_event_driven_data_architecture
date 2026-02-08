/* =====================================================================
   PHARMA EVENT-DRIVEN DATA PLATFORM — SNOWFLAKE SETUP (COPY/PASTE READY)
   Notes on sensitive info:
   - Replace placeholders like <AWS_ROLE_ARN> and <S3_RAW_URL>.
   - Do NOT commit real ARNs/account IDs to Git. Use a template file.
   ===================================================================== */

/* =========================
   0) (Optional) Account TZ
   =========================
   Only run if you truly want to set account timezone.
   Requires ACCOUNTADMIN.
*/
-- USE ROLE ACCOUNTADMIN;
-- ALTER ACCOUNT SET TIMEZONE = 'Asia/Kolkata';
-- USE ROLE SYSADMIN;


/* =========================
   1) Database + Schemas
   ========================= */
CREATE OR REPLACE DATABASE PHARMA_PLATFORM;

CREATE OR REPLACE SCHEMA PHARMA_PLATFORM.RAW;
CREATE OR REPLACE SCHEMA PHARMA_PLATFORM.BRONZE;
CREATE OR REPLACE SCHEMA PHARMA_PLATFORM.SILVER;
CREATE OR REPLACE SCHEMA PHARMA_PLATFORM.GOLD;


/* =========================
   2) Warehouse
   ========================= */
CREATE OR REPLACE WAREHOUSE WH_PHARMA
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;


/* =========================
   3) AWS ↔ Snowflake Access (Storage Integration)
   =========================
   SENSITIVE: STORAGE_AWS_ROLE_ARN contains your AWS account/role.
   Replace with placeholder before sharing or committing.
*/
CREATE OR REPLACE STORAGE INTEGRATION PHARMA_S3_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<AWS_ROLE_ARN>'              -- e.g., arn:aws:iam::<acct>:role/<role>
  STORAGE_ALLOWED_LOCATIONS = ('<S3_RAW_URL>');        -- e.g., s3://pharma-platform-data-project/raw/

-- Useful for initial setup only (returns external_id + snowflake ARN for trust policy):
-- DESC INTEGRATION PHARMA_S3_INT;


/* =========================
   4) RAW Schema Context
   ========================= */
USE DATABASE PHARMA_PLATFORM;
USE SCHEMA RAW;


/* =========================
   5) File Format + External Stage
   =========================
   SENSITIVE: stage URL can reveal bucket name; use placeholder if sharing.
*/
CREATE OR REPLACE FILE FORMAT CSV_FF
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL', 'null')
  DATE_FORMAT = 'YYYY-MM-DD'
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

CREATE OR REPLACE STAGE RAW_S3_STAGE
  STORAGE_INTEGRATION = PHARMA_S3_INT
  URL = '<S3_RAW_URL>'                                 -- e.g., s3://pharma-platform-data-project/raw/
  FILE_FORMAT = CSV_FF;

-- Smoke test (should work once integration trust is set):
-- LIST @RAW_S3_STAGE;


/* =========================
   6) RAW Tables
   ========================= */
CREATE OR REPLACE TABLE HCP_MASTER_RAW (
  hcp_id           STRING,
  specialty        STRING,
  territory        STRING,
  engagement_tier  STRING,
  src_file         STRING,
  load_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE PRODUCT_MASTER_RAW (
  product_code     STRING,
  brand_name       STRING,
  therapy_area     STRING,
  launch_date      DATE,
  lifecycle_stage  STRING,
  src_file         STRING,
  load_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RX_EVENTS_RAW (
  rx_id        STRING,
  rx_date      DATE,
  product_code STRING,
  hcp_id       STRING,
  quantity     NUMBER(38,0),
  days_supply  NUMBER(38,0),
  region       STRING,
  payer_type   STRING,
  channel      STRING,
  src_file     STRING,
  load_ts      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


/* =========================
   7) Snowpipes (Auto-ingest)
   =========================
   After creating each pipe:
   - Run DESC PIPE <pipe_name>;
   - Copy notification_channel;
   - Configure S3 Event Notification to that queue for the matching prefix.
*/

-- 7A) RX Events
CREATE OR REPLACE PIPE PIPE_RX_EVENTS
  AUTO_INGEST = TRUE
AS
COPY INTO RX_EVENTS_RAW (
  rx_id, rx_date, product_code, hcp_id,
  quantity, days_supply, region, payer_type, channel,
  src_file
)
FROM (
  SELECT
    $1,
    TO_DATE($2, 'YYYY-MM-DD'),
    $3, $4,
    TO_NUMBER($5), TO_NUMBER($6),
    $7, $8, $9,
    METADATA$FILENAME
  FROM @RAW_S3_STAGE/rx_events/
)
FILE_FORMAT = (FORMAT_NAME = CSV_FF);

-- 7B) HCP Master
CREATE OR REPLACE PIPE PIPE_HCP_MASTER
  AUTO_INGEST = TRUE
AS
COPY INTO HCP_MASTER_RAW (
  hcp_id, specialty, territory, engagement_tier,
  src_file
)
FROM (
  SELECT
    $1, $2, $3, $4,
    METADATA$FILENAME
  FROM @RAW_S3_STAGE/hcp_master/
)
FILE_FORMAT = (FORMAT_NAME = CSV_FF);

-- 7C) Product Master
CREATE OR REPLACE PIPE PIPE_PRODUCT_MASTER
  AUTO_INGEST = TRUE
AS
COPY INTO PRODUCT_MASTER_RAW (
  product_code, brand_name, therapy_area, launch_date, lifecycle_stage,
  src_file
)
FROM (
  SELECT
    $1, $2, $3, TO_DATE($4, 'YYYY-MM-DD'), $5,
    METADATA$FILENAME
  FROM @RAW_S3_STAGE/product_master/
)
FILE_FORMAT = (FORMAT_NAME = CSV_FF);


/* =========================
   8) Introspection / Debug
   ========================= */

-- Get Snowpipe SQS queue targets (notification_channel)
-- DESC PIPE PIPE_RX_EVENTS;
-- DESC PIPE PIPE_HCP_MASTER;
-- DESC PIPE PIPE_PRODUCT_MASTER;

-- Pipe listing
-- SHOW PIPES LIKE 'PIPE_%';

-- Verify tables
-- SELECT * FROM HCP_MASTER_RAW;
-- SELECT * FROM PRODUCT_MASTER_RAW;
-- SELECT * FROM RX_EVENTS_RAW ORDER BY load_ts DESC;

-- Current timestamp
-- SELECT CURRENT_TIMESTAMP();

-- Copy history (example: last 2 hours for product table)
-- SELECT *
-- FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
--   TABLE_NAME=>'PRODUCT_MASTER_RAW',
--   START_TIME=>DATEADD('hour', -2, CURRENT_TIMESTAMP())
-- ))
-- ORDER BY LAST_LOAD_TIME DESC;

-- General copy history (can be noisy)
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY);