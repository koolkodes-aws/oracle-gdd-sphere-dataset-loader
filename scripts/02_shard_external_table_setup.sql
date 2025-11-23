-- =====================================================
-- 02_shard_external_table_setup.sql
-- TARGET: EACH Shard PDB (Run locally)
-- USER: sphere_user
-- PURPOSE: Map local JSONL file to External Table
-- =====================================================

-- !!! CONFIGURATION !!!
-- Update this path to the actual mount point on the shard
DEFINE local_data_path = '/sphere'

SET ECHO ON
SET SERVEROUTPUT ON

PROMPT =====================================================
PROMPT Configuring Local External Table
PROMPT Path: &local_data_path
PROMPT =====================================================

-- 1. Disable Shard DDL 
-- Critical: We are creating a LOCAL object specific to this shard's filesystem
ALTER SESSION DISABLE SHARD DDL;

-- 2. Create Local Directory
CREATE OR REPLACE DIRECTORY sphere_data_dir AS '&local_data_path';

-- 3. Clean up existing external table (if any)
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE SPHERE900M_EXT';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

-- 4. Create Local External Table
-- Maps the JSONL file to a single CLOB column for processing
CREATE TABLE SPHERE900M_EXT (
    json_doc CLOB
)
ORGANIZATION EXTERNAL (
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY sphere_data_dir
    ACCESS PARAMETERS (
        RECORDS DELIMITED BY NEWLINE 
        FIELDS (
            json_doc CHAR(2000000) -- 2MB Buffer for large lines
        )
    )
    LOCATION ('sphere.jsonl')
)
REJECT LIMIT UNLIMITED;

PROMPT Local External Table SPHERE900M_EXT created successfully.
EXIT;