# Loading Sphere Dataset into Oracle Globally Distributed Database (26ai)

The Meta Sphere dataset represents one of the largest publicly available corpora of web-scale text embeddings, containing approximately 900 million documents from Common Crawl with pre-computed 768-dimensional dense vector representations. Each document includes the original text, metadata, and a FLOAT32 vector embedding generated using the Sphere DPR (Dense Passage Retrieval) model. This dataset serves as a critical benchmark for evaluating vector search systems at scale, particularly for retrieval-augmented generation (RAG) and semantic search applications where query latency and accuracy must remain consistent across billions of vectors.

This document details the procedure for ingesting the complete Sphere dataset into Oracle Globally Distributed Database 26ai. The loading strategy employs external tables, enabling file-based data ingestion on each shard. We combile parallel query on the external table with direct-path insert operations to optimize throughput while minimizing redo log contention during high-volume ingestion.

## Why Oracle Globally Distributed Database for Large Vector Datasets?

For large-scale vector search deployments with datasets exceeding 500 million records, Oracle Globally Distributed Database (GDD) provides a unique advantage by enabling **horizontally scaled, in-memory HNSW vector indexes** across multiple database shards.

### The Physics of Large Vector Indexes

**Memory Constraints**:

- In-memory HNSW vector indexes deliver significantly faster performance compared to disk-based IVF indexes
- HNSW indexes utilize the Vector Memory Pool, a dedicated memory area within the System Global Area (SGA)
- Single database instance memory is bounded by physical hardware constraints
- Large vector datasets (500M+ vectors with high dimensionality) can exceed single-instance Vector Memory Pool capacity

**Disk-Based Limitations**:

- IVF vector indexes support datasets limited only by available tablespace storage
- Storage costs are lower compared to memory requirements
- Query performance degrades significantly compared to in-memory HNSW due to physical I/O overhead
- Query latency may exceed acceptable thresholds for latency-sensitive workloads

### Oracle GDD Solution: Sharded In-Memory HNSW

Oracle Globally Distributed Database 23ai/26ai enables horizontal scaling for in-memory HNSW indexes:

**Scalability**:

- Oracle Globally Distributed Database supports up to 1,000 shards per sharded database configuration
- Data distribution uses Consistent Hash partitioning, ensuring approximately 1/n of dataset rows per shard
- Horizontal scaling increases aggregate Vector Memory Pool capacity linearly with shard count
- Production deployments demonstrate scalability with multi-shard vector index configurations

**Performance Benefits**:

- **Query Latency**: Sub-second vector similarity search at scale
- **Index Build Time**: Parallel DDL execution across shards reduces index creation time
  - Single-instance: Hours for large-scale datasets
  - Sharded database: Linear reduction in index build time proportional to shard count
- Each shard executes DDL operations independently, achieving parallel execution across the database cluster

### Use Case: Large-Scale Vector Search Benchmarks

This loader was developed to support large-scale vector search benchmarks for datasets that cannot fit in single-instance memory. The Meta Sphere dataset (100K-900M records) serves as a standardized benchmark for evaluating vector database performance at scale.

## Architecture Overview

In a distributed database environment, centralized loading patterns—where a single client pushes data through a coordinator node—often result in network saturation and serialization bottlenecks. To optimize ingestion performance for large datasets, this procedure decentralizes the loading process.

The architecture relies on three core mechanisms:

### Global Schema Propagation

The database schema is defined centrally on the Catalog Database. The GDD infrastructure utilizes the Global Data Services (GDS) framework to asynchronously propagate DDL statements (tables, users, privileges) to all shard nodes. This ensures schema consistency across the topology without requiring manual DDL execution on individual nodes.

### Data Locality via Direct-Path INSERT

Source data files reside on local storage attached to each shard node, eliminating network transfer overhead. Each shard's Pluggable Database (PDB) accesses data through external tables. The INSERT operation uses the `APPEND` hint to perform direct-path inserts, writing formatted blocks directly to data files and bypassing the Database Buffer Cache. This minimizes logical I/O, reduces CPU consumption, and eliminates buffer cache contention.

### Chunk-Aware Data Filtering

The source file `sphere.jsonl` is replicated identically across all shard nodes. Without filtering, loading the complete file on each shard would cause ORA-02502 (REMOTE MAPPING ERROR) due to sharding key violations. The load operation uses the `SHARD_CHUNK_ID` function in the WHERE predicate to implement chunk-aware filtering.

- **Hash Computation**: The function computes the chunk identifier by hashing the sharding key value for each row.
- **Chunk Ownership**: Returns NULL if the computed chunk is not owned by the local shard, causing the row to be filtered from the result set.
- **Optimization**: Filtered rows are discarded during query execution without generating redo records or triggering constraint violations.

## Prerequisites

Ensure the following infrastructure and configuration requirements are met before initiating the load.

### Source Data Distribution

The `sphere.jsonl` source file must exist on the local file system of every shard node.

- **Path**: Recommended path is `/sphere/sphere.jsonl`. Verify that the operating system user `oracle` has read permissions on this file.
- **I/O Considerations**: Store the source file on high-performance local NVMe or SSD storage. Avoid Network Attached Storage (NFS) for the source file, as network latency during the read phase can throttle the ingest rate.

### Storage Provisioning

- **Tablespace**: The `sphere_ts1` tablespace must be configured with `AUTOEXTEND ON` or pre-allocated to accommodate the target data volume. Vector embeddings (FLOAT32 arrays) are dense; ensure underlying storage supports high sustained write IOPS.
- **Fast Recovery Area (FRA)**: Increase `db_recovery_file_dest_size` (e.g., to 2TB) to accommodate the volume of archive logs generated during the load. If the FRA fills, the database instance will suspend operations.

### System Tuning (Redo Log Configuration)

**Log File Size**: Configuring appropriate Redo Log sizing is critical for bulk loads involving Primary Keys.

- **Recommendation**: Set Redo Log group size to a minimum of 4GB.
- **Technical Context**: Small log files trigger frequent log switches. Each log switch forces a checkpoint, requiring the Database Writer (DBWR) to flush dirty blocks to disk. Frequent checkpoints interrupt the direct path load stream, degrading performance. Larger logs reduce switch frequency, allowing for sustained throughput.

## Step 1: Global Schema Setup (Catalog)

**Target**: Catalog Database  
**User**: SYS (or user with SYSDBA/GSMADMIN_ROLE)

This step defines the global data model. Executing the script on the Catalog propagates the schema definitions to all registered shards.

**Technical Actions**:

- Creates the global user `sphere_user`.
- Defines the sharded table `sphere_documents` using Consistent Hash Partitioning. This distribution method hashes the `id` column to assign rows to specific chunks, ensuring uniform data distribution across the shard topology.
- Instantiates the `VECTOR(768, FLOAT32)` column for storing embeddings.

**Execution**:  
Execute once on the Catalog.

```bash
sqlplus sys/<password>@<catalog_host>:<port>/<service_name> as sysdba @01_catalog_schema_setup.sql
```

## Step 2: Local External Table Setup (All Shards)

**Target**: Each Shard PDB  
**User**: sphere_user

This step establishes the link between the database instance and the local file system. Directory objects and external table location definitions are instance-specific and require local configuration.

**Shard DDL Disable**:  
The script executes `ALTER SESSION DISABLE SHARD DDL`. This prevents the GDD coordination mechanism from replicating the DDL to other shards, allowing the creation of a Directory Object pointing to the unique local mount point of the specific host.

**Execution**:  
Connect to each shard PDB individually and execute the setup script.

```bash
sqlplus sphere_user/<password>@<shard_host>:<port>/<pdb_service_name> @02_shard_external_table_setup.sql
```

**Note**: Update the `local_data_path` variable in the SQL file if the source file location differs from `/sphere`.

## Step 3: Data Load Execution (All Shards)

**Target**: Each Shard PDB  
**User**: sphere_user  
**Method**: Background Execution (nohup)

This step executes data ingestion using parallel query on the external table combined with serial direct-path insert.

**Parallelism Configuration**:

### Parallel Query on External Table

The SELECT statement uses the `PARALLEL` hint to instantiate parallel query server processes. These processes read the external file, parse JSON structures using `JSON_TABLE`, and materialize VECTOR objects from array literals. Parallel query reduces elapsed time for CPU-intensive JSON parsing and vector deserialization operations.

### Serial Direct-Path INSERT

Parallel DML is not enabled for the INSERT operation.

- **Redo Log Considerations**: Parallel DML with unique constraints (Primary Key) can cause contention on redo allocation latches and redo copy latches, particularly when multiple parallel execution servers attempt simultaneous writes to the redo log buffer. The `log buffer space` wait event may become prevalent under high concurrency.
- **Implementation**: The parallel query servers pass rows through the query coordinator to a single direct-path INSERT operation. This serializes redo generation, maintaining throughput within Log Writer (LGWR) capacity while leveraging parallelism for the read and transformation phases.

**Execution**:  
Execute in background on each shard server.

```bash
nohup sqlplus sphere_user/<password>@<shard_host>:<port>/<pdb_service_name> @03_data_load_script.sql > load_shard_$(date +%Y%m%d).log 2>&1 &
```

## Monitoring

Monitor load operations using dynamic performance views.

### Parallel Execution Server Status

Query `V$PX_SESSION` to verify active parallel execution servers and coordinator processes.

```sql
SELECT sid, serial#, qcsid, degree, req_degree, server_group, server_set
FROM v$px_session 
WHERE qcsid = (SELECT sid FROM v$session WHERE username = 'SPHERE_USER');
```

### Long Operations Monitoring

Query `V$SESSION_LONGOPS` to track full table scan progress and estimate completion time.

```sql
SELECT opname, target, sofar, totalwork, units, time_remaining, elapsed_seconds
FROM v$session_longops
WHERE time_remaining > 0 AND username = 'SPHERE_USER';
```

## Step 4: Post-Load Vector Index Creation (Catalog)

**Target**: Catalog Database  
**User**: sphere_user

Vector index creation constructs an in-memory Hierarchical Navigable Small World (HNSW) graph structure. Building vector indexes during concurrent DML operations causes index fragmentation and increased maintenance overhead. Best practice defers index creation until bulk loading completes.

**Index Configuration**:

- **Organization**: `ORGANIZATION NEIGHBOR GRAPH` specifies in-memory HNSW vector index type.
- **Distance Function**: `DISTANCE COSINE` configures the similarity metric for vector comparisons.
- **Parallel DDL**: The `PARALLEL` clause enables parallel index build operations on each shard, utilizing available CPU resources.
- **DDL Propagation**: Executing `CREATE VECTOR INDEX` on the catalog database with `ENABLE SHARD DDL` propagates the DDL statement to all shards via Global Data Services (GDS), triggering parallel index builds across the sharded database.

**Execution**:  
Execute only after Step 3 has successfully completed on all shards.

```sql
-- Connect to Catalog
ALTER SESSION ENABLE SHARD DDL;
ALTER SESSION ENABLE PARALLEL DDL;

CREATE VECTOR INDEX sphere_doc_vector_idx ON sphere_documents(vector)
ORGANIZATION NEIGHBOR GRAPH
DISTANCE METRIC COSINE
WITH TARGET ACCURACY 90
PARALLEL 32;
```

## Appendix A: Oracle Autonomous Database Deployment

Oracle Autonomous Database (Serverless and Dedicated) deployments do not provide direct file system access. Data loading uses Object Storage integration via the `DBMS_CLOUD` package.

### Functional Differences

- **Data Source**: OCI Object Storage, Amazon S3, or Azure Blob Storage replaces local file system access.
- **API**: `DBMS_CLOUD.CREATE_EXTERNAL_TABLE` procedure replaces DDL-based `ORGANIZATION EXTERNAL` syntax.
- **Authentication**: Cloud credentials (Auth Token, Resource Principal, or API Key) required for Object Storage access.

### Setup Procedure

#### A. Data Staging

Upload the `sphere.jsonl` file to an OCI Object Storage bucket within the target region.

#### B. Credential Configuration

Oracle Autonomous Database on OCI supports Resource Principal authentication, eliminating credential management. For private buckets or cross-tenant access, create credential objects using `DBMS_CLOUD.CREATE_CREDENTIAL`:

```sql
BEGIN
  DBMS_CLOUD.CREATE_CREDENTIAL(
    credential_name => 'OCI_API_CRED',
    user_ocid       => '<user_ocid>',
    tenancy_ocid    => '<tenancy_ocid>',
    private_key     => '<private_key_content>',
    fingerprint     => '<fingerprint>'
  );
END;
/
```

#### C. External Table Creation via DBMS_CLOUD

Replace Step 2 with `DBMS_CLOUD.CREATE_EXTERNAL_TABLE` procedure. This creates an external table definition referencing Object Storage URIs.

```sql
BEGIN
    DBMS_CLOUD.CREATE_EXTERNAL_TABLE(
        table_name      => 'SPHERE900M_EXT',
        credential_name => 'OCI_API_CRED', -- Or 'OCI$RESOURCE_PRINCIPAL'
        file_uri_list   => 'https://objectstorage.<region>.oraclecloud.com/n/<namespace>/b/<bucket>/o/sphere.jsonl',
        column_list     => 'json_doc CLOB',
        field_list      => 'json_doc CHAR(5000000)', -- Buffer size for large JSON lines
        format          => JSON_OBJECT(
            'recorddelimiter' value 'newline',
            'delimiter'       value CHR(31) -- Unit Separator ensures line is treated as single column
        )
    );
END;
/
```

#### D. Load Execution

Proceed with Step 3 using the same SQL script. The `DBMS_CLOUD` package handles parallel reads from Object Storage, with the access driver managing streaming data transfer to the database instance.

## References

### Meta Sphere Dataset

- [Official GitHub Repository (Facebook Research)](https://github.com/facebookresearch/sphere)
- [Sphere: Meta AI's web-scale corpus](https://ai.facebook.com/blog/sphere-ai-web-scale-corpus/)

### Oracle Documentation

- [Oracle Globally Distributed Database Guide](https://docs.oracle.com/en/database/oracle/oracle-database/)
- [Oracle AI Database 26ai Documentation Library](https://docs.oracle.com/en/database/oracle/oracle-database/)
