-- =====================================================
-- 03_data_load_script.sql
-- TARGET: EACH Shard PDB
-- USER: sphere_user
-- STRATEGY: Parallel Query with Serial Direct-Path INSERT
-- =====================================================  

SET TIMING ON
SET ECHO ON

-- ==================================================
-- SESSION CONFIGURATION
-- ==================================================

-- 1. Work Area Memory Management
ALTER SESSION SET WORKAREA_SIZE_POLICY=AUTO;
ALTER SESSION SET SORT_AREA_SIZE=1073741824; -- 1GB sort area for JSON operations

-- 2. Commit Optimization
-- BATCH mode reduces log file sync waits by deferring physical write confirmation
ALTER SESSION SET COMMIT_WRITE = 'BATCH, NOWAIT';
ALTER SESSION SET COMMIT_LOGGING = 'BATCH';

-- 3. Parallel Execution Configuration
-- Parallel DML is NOT enabled to avoid redo allocation latch contention.
-- Only parallel query is used on the external table SELECT operation.

-- ==================================================
-- LOAD EXECUTION
-- ==================================================

PROMPT Starting Data Load...

INSERT /*+ APPEND */ INTO sphere_documents (
    id,
    url,
    title,
    sha,
    raw_text,
    vector,
    created_date
)
SELECT /*+ PARALLEL(t, 16) */ -- Parallel query servers for JSON parsing and vector deserialization
    jt.id,
    jt.url,
    jt.title,
    jt.sha,
    jt.raw_text,
    TO_VECTOR(jt.vector_json),
    SYSTIMESTAMP
FROM
    SPHERE900M_EXT t,
    JSON_TABLE(t.json_doc, '$'
        COLUMNS (
            id          VARCHAR2(100)       PATH '$.id',
            url         VARCHAR2(4000)      PATH '$.url',
            title       VARCHAR2(1000)      PATH '$.title',
            sha         VARCHAR2(100)       PATH '$.sha',
            raw_text    CLOB                PATH '$.raw',
            vector_json CLOB FORMAT JSON    PATH '$.vector'
        )
    ) jt
WHERE
    t.json_doc IS NOT NULL
    -- Chunk-aware filtering for sharded tables
    -- SHARD_CHUNK_ID computes chunk ownership based on sharding key hash
    -- Returns NULL for chunks not owned by this shard, filtering rows from result set
    -- Prevents ORA-02502 (REMOTE MAPPING ERROR) during INSERT operations
    AND SHARD_CHUNK_ID('SPHERE_USER.SPHERE_DOCUMENTS', jt.id) IS NOT NULL;

COMMIT;

PROMPT Load Complete.
EXIT;