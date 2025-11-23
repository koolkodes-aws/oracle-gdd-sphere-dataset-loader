# Loading Sphere Dataset into Oracle Globally Distributed Database (26ai)

This document details the procedure for ingesting the Meta Sphere dataset (JSONL format) into an Oracle Globally Distributed Database (GDD) 26ai environment. The dataset consists of approximately 900 million records containing high-dimensional vector embeddings.

The loading strategy employs Sharded External Tables mapped to local file systems on individual shards, combined with a Hybrid Parallel Load approach. This architecture optimizes I/O throughput and mitigates contention in the database redo log buffer during high-volume ingestion.

## Architecture Overview

In a distributed database environment, centralized loading patterns—where a single client pushes data through a coordinator node—often result in network saturation and serialization bottlenecks. To optimize ingestion performance for large datasets, this procedure decentralizes the loading process.

The architecture relies on three core mechanisms:

### Global Schema Propagation

The database schema is defined centrally on the Catalog Database. The GDD infrastructure utilizes the Global Data Services (GDS) framework to asynchronously propagate DDL statements (tables, users, privileges) to all shard nodes. This ensures schema consistency across the topology without requiring manual DDL execution on individual nodes.

### Data Locality via Direct Path Load

Rather than transmitting data across the network, source data is staged locally on each shard's file system. Each Shard Pluggable Database (PDB) utilizes a local External Table configuration that references the local sphere.jsonl file. This configuration enables the database engine to execute a "Direct Path Load" (`INSERT /*+ APPEND */`), which writes data blocks directly to data files, bypassing the System Global Area (SGA) buffer cache. This approach significantly reduces CPU overhead and memory contention.

### Shard-Pruned Ingestion

The source file `sphere.jsonl` is identical on every shard and contains the complete dataset. Loading the full file on every node would result in redundant processing and "Shard Miss" errors (ORA-02502). To filter data efficiently, the load scripts implement the `SHARD_CHUNK_ID` function within the WHERE clause.

- **Mechanism**: The function computes the sharding key hash for each row at runtime.
- **Logic**: It verifies if the computed chunk ID maps to the local shard processing the row.
- **Result**: The database inserts only the rows owned by the local shard and discards non-local rows without generating redo entries or error logs for the skipped data.

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

## Step 3: Hybrid Parallel Load (All Shards)

**Target**: Each Shard PDB  
**User**: sphere_user  
**Method**: Background Execution (nohup)

This step executes the data ingestion using a Hybrid Load Strategy that differentiates the parallelism degree between the Producer (Read) and Consumer (Write) operations.

**Hybrid Strategy Mechanics**:

### Parallel Read (Producer)

The SELECT statement utilizes `/*+ PARALLEL(t, 16) */`. This instantiates 16 parallel query slave processes to read the JSONL file, parse the JSON structure, and convert text arrays into binary VECTOR format. This phase is CPU-intensive and benefits from high concurrency.

### Serial Write (Consumer)

Parallel DML is intentionally disabled for the INSERT operation.

- **Rationale**: Enabling parallel writers for high-volume loads with Primary Key constraints often creates contention on the Redo Log Buffer latches. If multiple writers attempt to write to the redo log buffer simultaneously, the `log buffer space` wait event can throttle the entire system.
- **Outcome**: The 16 reader threads funnel processed data to a single writer process. This acts as a flow control mechanism, maintaining high throughput while keeping redo generation within the capacity limits of the Log Writer (LGWR) process.

**Execution**:  
Execute continuously in the background on every shard server.

```bash
nohup sqlplus sphere_user/<password>@<shard_host>:<port>/<pdb_service_name> @03_hybrid_load_script.sql > load_shard_$(date +%Y%m%d).log 2>&1 &
```

## Monitoring

Verify load progress by inspecting database performance views.

### Active Session Check

Verify the presence of the coordinator and parallel slave processes.

```sql
SELECT status, count(*) FROM v$px_session WHERE username = 'SPHERE_USER' GROUP BY status;
```

### Progress Estimation

Query `V$SESSION_LONGOPS` to view the full table scan progress and estimated completion time.

```sql
SELECT OPNAME, TARGET, SOFAR, TOTALWORK, UNITS, TIME_REMAINING
FROM V$SESSION_LONGOPS
WHERE TIME_REMAINING > 0;
```

## Step 4: Post-Load Vector Indexing (Catalog)

**Target**: Catalog Database  
**User**: sphere_user

Vector Index creation involves building a Hierarchical Navigable Small World (HNSW) graph, which is computationally expensive. Building this index during the initial data load results in significant index fragmentation and overhead. The optimal approach is to defer index creation until the dataset is fully populated.

**Indexing Configuration**:

- **Algorithm**: Creates an `ORGANIZATION NEIGHBOR GRAPH` index using the HNSW algorithm.
- **Distance Metric**: Configures `DISTANCE METRIC COSINE` for semantic similarity calculations.
- **Parallelism**: Uses `PARALLEL 32` to leverage available CPU cores on each shard, accelerating the in-memory graph construction.
- **Propagation**: Executing the DDL on the Catalog propagates the command to all shards, triggering parallel local index builds.

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

## Appendix A: Loading on Autonomous Database (ADB)

For Oracle Autonomous Database (Serverless or Dedicated) deployments, direct access to the local operating system file system is restricted. Data ingestion must utilize Object Storage.

### 1. Functional Differences

- **Data Source**: OCI Object Storage (or S3/Azure Blob) replaces the local file system.
- **Interface**: `DBMS_CLOUD` package replaces standard `ORGANIZATION EXTERNAL` syntax.
- **Authentication**: Requires cloud credentials (API Key or Resource Principal).

### Setup Procedure

#### A. Data Staging

Upload the `sphere.jsonl` file to an OCI Object Storage bucket within the target region.

#### B. Credential Configuration

For OCI-native deployments, utilize the Resource Principal (`OCI$RESOURCE_PRINCIPAL`) to avoid managing static API keys. If using a private bucket without Resource Principals, create a credential object:

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

#### C. External Table Definition

Replace Step 2 with the following `DBMS_CLOUD` configuration. This maps the object storage URI to the external table definition.

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

Proceed with Step 3 (Hybrid Load Script). The script logic remains unchanged; `DBMS_CLOUD` manages the parallel data stream from Object Storage to the database engine.

## References

### Meta Sphere Dataset

- [Official GitHub Repository (Facebook Research)](https://github.com/facebookresearch/sphere)
- [Sphere: Meta AI's web-scale corpus](https://ai.facebook.com/blog/sphere-ai-web-scale-corpus/)

### Oracle Documentation

- [Oracle Globally Distributed Database Guide](https://docs.oracle.com/en/database/oracle/oracle-database/)
- [Oracle AI Database 26ai Documentation Library](https://docs.oracle.com/en/database/oracle/oracle-database/)
