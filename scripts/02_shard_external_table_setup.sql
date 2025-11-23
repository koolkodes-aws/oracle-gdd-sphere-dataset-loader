-- =====================================================
-- 02_shard_external_table_setup.sql
-- TARGET: EACH Shard PDB (Execute on each shard individually)
-- USER: sphere_user
-- PURPOSE: Create external table for local JSONL file access
-- =====================================================

-- !!! CONFIGURATION !!!
-- Update this path to match the actual file system mount point on each shard
DEFINE local_data_path = '/sphere'

SET ECHO ON
SET SERVEROUTPUT ON

PROMPT =====================================================
PROMPT Creating External Table Definition
PROMPT Path: &local_data_path
PROMPT =====================================================

-- 1. Disable Shard DDL Propagation
-- This DDL creates a local object with host-specific file system paths
-- Must not be propagated via Global Data Services to other shards
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

-- 4. Create External Table with ORGANIZATION EXTERNAL
-- Each row in the external file is mapped to a single CLOB column
-- ORACLE_LOADER access driver reads newline-delimited records
CREATE TABLE SPHERE900M_EXT (
    json_doc CLOB
)
ORGANIZATION EXTERNAL (
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY sphere_data_dir
    ACCESS PARAMETERS (
        RECORDS DELIMITED BY NEWLINE 
        FIELDS (
            json_doc CHAR(2000000) -- 2MB field buffer for large JSON lines
        )
    )
    LOCATION ('sphere.jsonl')
)
REJECT LIMIT UNLIMITED;

PROMPT Local External Table SPHERE900M_EXT created successfully.
EXIT;