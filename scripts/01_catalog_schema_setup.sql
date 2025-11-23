-- =====================================================
-- 01_catalog_schema_setup.sql
-- TARGET: Catalog Database
-- USER: SYSDBA
-- PURPOSE: Create Global Schema for Oracle GDD 26ai
-- =====================================================

SET ECHO ON
SET SERVEROUTPUT ON
SET TIMING ON

PROMPT =====================================================
PROMPT Creating Sphere Dataset Schema (Catalog)
PROMPT =====================================================

-- 1. Enable Shard DDL propagation
ALTER SESSION ENABLE SHARD DDL;

-- 2. Create Global User
CREATE USER sphere_user IDENTIFIED BY "<secure_password>";

GRANT CONNECT, RESOURCE, DBA TO sphere_user;
GRANT CREATE TABLE, CREATE VIEW, CREATE SEQUENCE TO sphere_user;
GRANT UNLIMITED TABLESPACE TO sphere_user;
GRANT CREATE ANY DIRECTORY TO sphere_user;
GRANT SELECT_CATALOG_ROLE TO sphere_user; -- Helpful for GDD monitoring

-- 3. Create Tablespace Set (Automatically created on all shards)
CREATE TABLESPACE SET sphere_ts1;

-- 4. Create Sharded Table with VECTOR Data Type
-- Partitioned by Consistent Hash for even distribution
CREATE SHARDED TABLE sphere_user.sphere_documents (
    id           VARCHAR2(100) NOT NULL,
    url          VARCHAR2(4000),
    title        VARCHAR2(1000),
    sha          VARCHAR2(100),
    raw_text     CLOB,
    vector       VECTOR(768, FLOAT32),
    created_date TIMESTAMP DEFAULT SYSTIMESTAMP,
    updated_date TIMESTAMP DEFAULT SYSTIMESTAMP,
    CONSTRAINT sphere_documents_pk PRIMARY KEY (id)
)
PARTITION BY CONSISTENT HASH (id)
PARTITIONS AUTO
TABLESPACE SET sphere_ts1;

-- 5. Create Metadata Table (Sharded by Reference)
-- Co-locates metadata with the parent document on the same shard
CREATE SHARDED TABLE sphere_user.sphere_load_metadata (
    batch_id       NUMBER NOT NULL,
    doc_id         VARCHAR2(100) NOT NULL,
    load_date      TIMESTAMP DEFAULT SYSTIMESTAMP,
    records_loaded NUMBER,
    load_status    VARCHAR2(50),
    error_message  CLOB,
    shard_name     VARCHAR2(100),
    CONSTRAINT sphere_load_meta_pk PRIMARY KEY (doc_id, batch_id),
    CONSTRAINT sphere_load_meta_fk FOREIGN KEY (doc_id) 
        REFERENCES sphere_user.sphere_documents(id)
)
PARTITION BY REFERENCE (sphere_load_meta_fk)
TABLESPACE SET sphere_ts1;

COMMIT;

PROMPT Global Schema Creation Complete.
EXIT;