-- =====================================================
-- 03_data_load_script.sql
-- TARGET: EACH Shard PDB
-- USER: sphere_user
-- STRATEGY: Hybrid Load (Parallel Read / Serial Write)
-- =====================================================

SET TIMING ON
SET ECHO ON

-- ==================================================
-- SESSION OPTIMIZATIONS
-- ==================================================

-- 1. Memory Optimization for JSON Parsing
ALTER SESSION SET WORKAREA_SIZE_POLICY=AUTO;
ALTER SESSION SET SORT_AREA_SIZE=1073741824; -- 1GB Sort Area

-- 2. Redo Log Optimization
-- "Batch, Nowait" allows the loader to proceed without waiting for physical disk sync
ALTER SESSION SET COMMIT_WRITE = 'BATCH, NOWAIT';
ALTER SESSION SET COMMIT_LOGGING = 'BATCH';

-- 3. Parallelism Strategy
-- We DO NOT enable PARALLEL DML on the insert side to protect the Redo Log Buffer.
-- We ONLY use parallel query on the select side.

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
SELECT /*+ PARALLEL(t, 16) */ -- 16 threads to parse JSON and convert Vectors
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
    -- CRITICAL FOR GDD: SHARD PRUNING
    -- This function returns the Chunk ID for the given sharding key (id).
    -- If the Chunk ID is not mapped to this local shard, the function returns NULL (or filters out).
    -- This ensures this shard ONLY inserts rows it owns.
    AND SHARD_CHUNK_ID('SPHERE_USER.SPHERE_DOCUMENTS', jt.id) IS NOT NULL;

COMMIT;

PROMPT Load Complete.
EXIT;