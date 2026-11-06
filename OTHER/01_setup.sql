-- =============================================================================
-- Phase 3 - Snowflake setup
-- Chaos E-commerce Pipeline
--
-- HOW TO RUN
--   1. Log in to Snowflake in your browser.
--   2. Projects -> Worksheets -> + (new SQL worksheet).
--   3. Paste this whole file in.
--   4. Run statements top to bottom. In Snowsight, Ctrl+Enter runs the
--      statement your cursor is in; the "Run All" button runs the file.
--   5. Section 8 verifies everything and prints the values for your .env.
--
-- Safe to re-run. Every object uses IF NOT EXISTS, so running this twice
-- changes nothing and destroys nothing. Same idempotency principle the rest
-- of the pipeline is built on.
--
-- ONE SIMPLIFICATION TO KNOW ABOUT
--   Everything here runs as ACCOUNTADMIN because this is your own trial
--   account. In a real organisation you would split this up: SYSADMIN creates
--   warehouses and databases, SECURITYADMIN creates roles and users, and only
--   ACCOUNTADMIN touches resource monitors. Worth saying out loud in an
--   interview, because running everything as ACCOUNTADMIN is a habit that
--   does not survive contact with a real company.
-- =============================================================================

use role accountadmin;


-- =============================================================================
-- 3.7  RESOURCE MONITOR  (first, deliberately)
-- =============================================================================
-- This is item 3.7 on the checklist, but it goes first. A monitor created
-- after the warehouse protects you from nothing in the window between the two.
--
-- A trial account has a fixed credit allowance. The usual way people lose it
-- is a warehouse left running, or a runaway query on an oversized warehouse.
-- This caps the damage at 50 credits a month, which is far more than this
-- project needs.
--
-- Triggers fire in order: notify, notify, then stop accepting new queries,
-- then kill what is already running.
-- =============================================================================

create resource monitor if not exists ecommerce_monitor
with
    credit_quota    = 50
    frequency       = monthly
    start_timestamp = immediately
    triggers
        on  75 percent do notify
        on  90 percent do notify
        on 100 percent do suspend
        on 110 percent do suspend_immediate;


-- =============================================================================
-- 3.1  WAREHOUSE  (compute)
-- =============================================================================
-- A warehouse is NOT where data lives. It is a compute cluster that executes
-- queries. Your tables sit in the database and cost storage; the warehouse
-- costs credits, billed by the second, for every second it is running.
--
--   warehouse_size = XSMALL     smallest tier, 1 credit/hour. Each size up
--                               doubles both speed and cost.
--   auto_suspend   = 60         shut down after 60 seconds idle. THE most
--                               important setting here. Without it, an idle
--                               warehouse bills all night for doing nothing.
--   auto_resume    = TRUE       wake automatically when a query arrives.
--                               Without it, suspended means your queries fail.
--   initially_suspended         do not start billing the moment it is created.
-- =============================================================================

create warehouse if not exists ecommerce_wh
with
    warehouse_size      = 'XSMALL'
    auto_suspend        = 60
    auto_resume         = true
    initially_suspended = true
    resource_monitor    = ecommerce_monitor
    comment             = 'Dedicated compute for the chaos e-commerce pipeline';

-- Attach the monitor explicitly too, so a re-run of this script on a
-- pre-existing warehouse still ends up protected.
alter warehouse ecommerce_wh set resource_monitor = ecommerce_monitor;


-- =============================================================================
-- 3.2  DATABASE AND SCHEMAS
-- =============================================================================
-- Four schemas, each a layer of the pipeline:
--
--   RAW        Data exactly as it arrived, plus load metadata. Never cleaned,
--              never deduplicated. If RAW is wrong, you lost the evidence.
--   STAGING    Cleaned, typed, deduplicated. Rebuildable from RAW at any time.
--   MARTS      The star schema. Rebuildable from STAGING at any time.
--   SNAPSHOTS  SCD Type 2 history, built by dbt snapshot.
--
-- SNAPSHOTS is not on the Phase 3 checklist and is worth the extra line.
-- Snapshot tables are the one thing in this pipeline that CANNOT be rebuilt:
-- they accumulate history one run at a time, so dropping the schema destroys
-- every customer version permanently. Keeping it apart from the two
-- rebuild-at-will layers makes that difference visible instead of a trap.
-- =============================================================================

create database if not exists ecommerce
    comment = 'Chaos e-commerce pipeline';

create schema if not exists ecommerce.raw
    comment = 'Landing zone. Data as received, plus _loaded_at / _source_file / _batch_date';

create schema if not exists ecommerce.staging
    comment = 'Cleaned, typed, deduplicated. Fully rebuildable from RAW';

create schema if not exists ecommerce.marts
    comment = 'Star schema: fct_order_items, dim_customer, dim_product, dim_date';

create schema if not exists ecommerce.snapshots
    comment = 'SCD2 history. NOT rebuildable - accumulates one run at a time';

-- Snowflake creates a PUBLIC schema in every new database. Nothing uses it,
-- and an unused writable schema is just somewhere for stray objects to hide.
drop schema if exists ecommerce.public;


-- =============================================================================
-- 3.3  ROLE, USER AND GRANTS
-- =============================================================================
-- The pipeline gets its own role and its own login. Two reasons that matter
-- beyond tidiness:
--
--   1. If the credentials leak, the blast radius is this one database.
--   2. Phase 12 can attribute credit usage to this role specifically, instead
--      of mixing it with everything else you do in the account.
-- =============================================================================

create role if not exists ecommerce_role
    comment = 'Least-privilege role for the pipeline: ingestion, dbt, Airflow';

-- Granting the role to SYSADMIN means ACCOUNTADMIN inherits it, so you can
-- switch into it yourself for the smoke test in section 7 without needing to
-- know your own username.
grant role ecommerce_role to role sysadmin;

-- --- Compute -----------------------------------------------------------------
-- USAGE lets the role run queries on the warehouse.
-- OPERATE lets it resume a suspended warehouse, which it must be able to do,
-- because auto_suspend means the warehouse is usually off when a DAG starts.
grant usage, operate on warehouse ecommerce_wh to role ecommerce_role;

-- --- Database and schemas ----------------------------------------------------
grant usage on database ecommerce to role ecommerce_role;

-- dbt needs to create schemas of its own. By default it names them
-- <target_schema>_<custom_schema>, so without this you hit a permission error
-- in Phase 5 that is genuinely annoying to diagnose.
grant create schema on database ecommerce to role ecommerce_role;

grant usage, create table, create view, create stage, create file format
    on schema ecommerce.raw to role ecommerce_role;

grant usage, create table, create view
    on schema ecommerce.staging to role ecommerce_role;

grant usage, create table, create view
    on schema ecommerce.marts to role ecommerce_role;

grant usage, create table, create view
    on schema ecommerce.snapshots to role ecommerce_role;

-- --- Reading objects it did not create ---------------------------------------
-- The role owns whatever it creates, so these cover the case where something
-- was created by another role. FUTURE grants apply to objects that do not
-- exist yet, which saves re-granting every time dbt adds a model.
grant select on all    tables in schema ecommerce.raw to role ecommerce_role;
grant select on future tables in schema ecommerce.raw to role ecommerce_role;
grant select on all    views  in schema ecommerce.raw to role ecommerce_role;
grant select on future views  in schema ecommerce.raw to role ecommerce_role;

grant select on all    tables in schema ecommerce.staging to role ecommerce_role;
grant select on future tables in schema ecommerce.staging to role ecommerce_role;
grant select on all    views  in schema ecommerce.staging to role ecommerce_role;
grant select on future views  in schema ecommerce.staging to role ecommerce_role;

grant select on all    tables in schema ecommerce.marts to role ecommerce_role;
grant select on future tables in schema ecommerce.marts to role ecommerce_role;

grant select on all    tables in schema ecommerce.snapshots to role ecommerce_role;
grant select on future tables in schema ecommerce.snapshots to role ecommerce_role;

-- --- Reading Snowflake's own metadata ----------------------------------------
-- Phase 12 queries query history and warehouse metering to find expensive
-- models. This grant is what makes that possible.
grant imported privileges on database snowflake to role ecommerce_role;

-- --- The user ----------------------------------------------------------------
-- >>> CHANGE THE PASSWORD BELOW BEFORE RUNNING THIS STATEMENT. <<<
-- Use something long and random. It goes in .env and nowhere else.
--
-- TYPE = SERVICE marks this as a non-human login, which exempts it from the
-- MFA requirements that apply to people. If your account rejects TYPE or
-- rejects password authentication for service users, use the key-pair block
-- at the bottom of this file instead.

create user if not exists ecommerce_user
    password          = 'REPLACE_ME_WITH_A_LONG_RANDOM_STRING'
    type              = service
    default_role      = ecommerce_role
    default_warehouse = ecommerce_wh
    default_namespace = ecommerce.raw
    comment           = 'Pipeline service account: ingestion, dbt, Airflow';

grant role ecommerce_role to user ecommerce_user;


-- =============================================================================
-- 3.4  FILE FORMATS
-- =============================================================================
-- A file format tells COPY INTO how to parse what you uploaded. Two of them,
-- because the simulator can emit either: Parquet for the real pipeline, CSV
-- for when you want to open a file and look at it.
--
-- Parquet carries its own schema and types, which is why it is the default.
-- CSV needs every one of these options spelled out, and getting them wrong is
-- a classic source of silently mangled data.
-- =============================================================================

create file format if not exists ecommerce.raw.ff_parquet
    type        = parquet
    compression = auto
    comment     = 'Default format. Parquet carries its own schema and types';

create file format if not exists ecommerce.raw.ff_csv
    type                         = csv
    field_delimiter              = ','
    skip_header                  = 1
    field_optionally_enclosed_by = '"'
    -- Without this, an empty cell loads as the literal string '' rather
    -- than NULL, and your null tests pass while the data is wrong.
    null_if                      = ('', 'NULL', 'null', 'None', 'nan')
    empty_field_as_null          = true
    trim_space                   = true
    date_format                  = 'YYYY-MM-DD'
    timestamp_format             = 'YYYY-MM-DD HH24:MI:SS'
    -- Fail the whole load rather than silently dropping a malformed row.
    -- You want chaos detected, not absorbed.
    error_on_column_count_mismatch = true
    comment = 'For --format csv runs of the simulator';


-- =============================================================================
-- 3.5  INTERNAL STAGE
-- =============================================================================
-- A stage is the landing area between your laptop and your tables. Phase 4
-- uploads to it with PUT, then loads into RAW tables with COPY INTO.
--
-- "Internal" means Snowflake stores the files for you. An external stage
-- would point at your own S3 or Azure bucket. Internal is right here: no
-- cloud account needed, and the files are small.
--
-- Snowflake records which files it has already loaded into which table, and
-- that load history is one of the two things making your ingestion
-- idempotent in Phase 4.
-- =============================================================================

create stage if not exists ecommerce.raw.stg_landing
    file_format = ecommerce.raw.ff_parquet
    comment     = 'PUT uploads land here, then COPY INTO moves them to RAW tables';


-- =============================================================================
-- 7.  SMOKE TEST
-- =============================================================================
-- Stop assuming the grants worked and prove it. This switches into the
-- pipeline role and does what the pipeline will do: resume the warehouse,
-- create a table, write to it, read it back, clean up.
--
-- If any statement here fails, a grant above is missing, and it is far better
-- to find that now than halfway through Phase 4.
-- =============================================================================

use role ecommerce_role;
use warehouse ecommerce_wh;
use database ecommerce;
use schema raw;

create or replace table _setup_check (
    id      number,
    note    string,
    checked timestamp_ltz default current_timestamp()
);

insert into _setup_check (id, note) values (1, 'Phase 3 grants verified');

select * from _setup_check;

-- Prove the stage is reachable. An empty result is the correct answer here;
-- an error is not.
list @ecommerce.raw.stg_landing;

drop table _setup_check;


-- =============================================================================
-- 8.  VERIFY, AND COLLECT YOUR .env VALUES
-- =============================================================================

use role accountadmin;

-- This is the value for SNOWFLAKE_ACCOUNT in your .env file.
select current_organization_name() || '-' || current_account_name() as snowflake_account;

-- If the line above errors on an older account, use this instead. It returns
-- the legacy account locator, which also works for connecting.
-- select current_account() as snowflake_account_locator;

show warehouses        like 'ECOMMERCE_WH';
show databases         like 'ECOMMERCE';
show schemas           in database ecommerce;
show file formats      in schema ecommerce.raw;
show stages            in schema ecommerce.raw;
show grants to role ecommerce_role;

-- Confirm the cap is in place and nothing has been spent yet.
show resource monitors like 'ECOMMERCE_MONITOR';


-- =============================================================================
-- OPTIONAL: key-pair authentication
-- =============================================================================
-- Use this if your account refuses password authentication for service users,
-- or if you would simply rather not keep a password in .env. Key-pair is the
-- better practice and is what you would use at work.
--
-- On your machine, in PowerShell (needs OpenSSL; Git for Windows ships it):
--
--   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snowflake_key.p8 -nocrypt
--   openssl rsa -in snowflake_key.p8 -pubout -out snowflake_key.pub
--
-- Open snowflake_key.pub, remove the BEGIN and END lines and all newlines,
-- and paste the remaining single line below. Keep snowflake_key.p8 out of git.
--
-- alter user ecommerce_user set rsa_public_key = 'PASTE_THE_PUBLIC_KEY_HERE';
-- alter user ecommerce_user unset password;
--
-- Then in .env, drop SNOWFLAKE_PASSWORD and set:
--   SNOWFLAKE_PRIVATE_KEY_PATH=./snowflake_key.p8


-- =============================================================================
-- TEARDOWN (do not run unless you mean it)
-- =============================================================================
-- Keep this at the bottom so rebuilding from scratch is one copy-paste away.
-- Dropping ECOMMERCE destroys your SNAPSHOTS schema, and SCD2 history cannot
-- be regenerated. Snowflake's Time Travel can undrop within the retention
-- window, but do not rely on that.
--
-- drop user             if exists ecommerce_user;
-- drop role             if exists ecommerce_role;
-- drop database         if exists ecommerce;
-- drop warehouse        if exists ecommerce_wh;
-- drop resource monitor if exists ecommerce_monitor;
