# Tarchia

Tarchia is an Active Data Catalog.

Tarchia actively manages and catalogs data in real-time. Unlike traditional catalogs that serve merely as passive records, our Active Data Catalog is essential to the operational workflow, ensuring meta data is always up-to-date and readily accessible for system processes.

Key Features:
- Real-time updates and synchronization with the system.
- Active role in data management and operations.
- Interactive interface for seamless data access and manipulation.

The Active Data Catalog is designed to enhance data accessibility, reliability, and operational efficiency, making it a cornerstone of our data management strategy.

It is not yet-another-system-to-update, it is a vital component.

**Terminology**

- **Catalog** - A collection of tables.
- **Commit** - The state of the table at a specific point in time.
- **Data File** - Files that contain the rows of the table.
- **Manifest** - A list which describes the data files in the table.
- **Metadata** - Information used to manage and describe tables.
- **Owner** - Namespace for table.
- **Schema** - The structure defining the columns of the table.
- **Table** - A dataset stored in a structured and managed way.

**Physical Structure**

~~~
table/
 |- metadata
 |   |- commits/
 |   |   +- commit-00000000.avro
 |   |- manifests/
 |   |   +- manifest-00000000.avro
 |   +- history/
 |       +- history-00000000.json
 +- data/
     +- year=2000/
         +- month=01/
             +- day=01/
                 +- data-0000-0000.parquet
~~~

**Logical Structure**

~~~mermaid
flowchart TD
    CATALOG  --> COMMITS(Commit History)
    CATALOG[(Catalog)] --> |Current| COMMIT(Commit)
    CATALOG  --> |Current| SCHEMA(Schema)
    subgraph  
        COMMITS  -..-> |Historical| COMMIT
        COMMIT --> SCHEMA
        COMMIT --> MAN_LIST(Manifest/List)
    end
    MAN_LIST --> DATA(Data Files)
~~~

## Design

### Catalog Overview

- Hosted in a persistent datastore such as FireStore (Document Store Opinionated).
- Contains entries for Owners and Tables.
- Table entries contains Metadata, schema and permissions, and a pointer to the latest Commit.

### Table Creation and Updates

- **Creation**:

  - Created with a schema but no Commits (no data initially).

- **Writing to a Table**:
  - Obtain the latest Schema (may differ from the latest Commit if updated since).
  - Write the data and save the Commit.
  - Use a transaction system to ensure complete datasets/updates before Catalog updates.

### Handling Clashes

- Ensure the dataset version (latest Commit) at the start of the transaction matches the version at the end.
- If versions don't match, another change has occurred so the transaction should fail, or require a hard override.

### Streaming Datasets

- Use a transaction to add new files to the dataset, this will update the Manifest and create a new Commit.

### Scheme Evolution

- Schema changes are limited to:
  - Adding and removing columns.
  - Limited type conversions (e.g., int to float).
- Larger changes must be treated treated as different datasets.

### Table Reading

- Retrieve the Commit, this contains the schema, permissions and encryption information and the root manifest.

### Catalog References

- The latest Schema (for writing).
- The location of the Data Files (for writing).
- The partitioning of Data Files (for writing).
- The latest Commit.
- The Commit History File.

### Manifests

- Manifests are immutable, adding a new file creates a new Manifest.
- First split at 4097, (11 years of daily writes, 61 million records if 15k/file, 200 million if 50k/file)
- Otherwise, limited to 2048 rows (aiming for most files to be <2MB to support caching).
- If a manifest exceeds this number, it is split, and a Manifest List is created.
- Use B-Tree style management for efficient pruning:

  - **Read and Write Overheads**:
    - 1 million row dataset: single manifest.
    - 1 billion row dataset: 33 manifests (1 root and 1 layer with 32 children).
    - 1 trillion row dataset: 32568 manifests in three layers.
    - Parallel/Async access reduces read and process time.
    - Pruning can quickly reduce reads.

### Data Files

- Manifest and Commit files do not need to be colocated with the data files.
- Data Files do not need to be colocated with each other, but must be on the same storage platform (e.g. all on GCS, all on S3)

### ACIDity

- Updates are Atomic, effective only when the Catalog is updated.
- Updates are Consistent, commits are checked if they match the schema.
- Updates are Isolated, via Commit version checking, this is brutish but effective.
- Updated are Durable, use of a database like FireStore and Cloud Storage ensure writes are persistent.
- Failed updates may leave artifacts (e.g., orphan files), but the update itself is either successful or not.
- Storage cost for failed commits (orphaned files) is considered acceptable.

### Pruning

- Effective for columns that are sorted, nearly sorted, or have values appearing for limited periods.
- Ineffective for columns with very few, highly recurrent values (e.g., Gender).
- Effective for columns like dates when working with log entries.
- Eliminates Data Files, not rows.
- Various rules around pruning:
    - Can prune on `=`, `<`, `>`, `>=`, and `<=` conditions.
    - Cannot prune on complex columns (ARRAY, STRUCT).
    - VARCHAR and BLOB are truncated to 8 characters for pruning.

### Indexes

- Intended to operate at a leaf manifest level.
- Aim to balance between:
  - Too many indexes (one per blob).
  - Too few indexes (one per dataset).
- Index strategy is still being developed.

## Compatibility

Tarchia alpha currently only supports:

**Datafiles**: `parquet`
**Catalogs**: FireStore and internal
**Blob Stores**: local and google cloud storage

## Git-Like Management

Git      | Function                               | Tarchia
-------- | -------------------------------------- | -------
`init`   | Initialize a new dataset               | [POST] /v1/tables/{owner} 
`add`    | Stage changes to be included in commit | [POST] /v1/tables/{owner}/{table}/stage
`commit` | Save the staged changes to the dataset | [POST] /v1/transactions/commit 
<!--`branch` | Create a new branch of the dataset     | [POST] /v1/transactions/start -->
<!--`fork`   | Create a copy of a dataset             | [POST] /v1/tables/{owner}/{table}/fork-->

## API Definition

### Owner Management

End Point            | GET | POST | PATCH | DELETE
-------------------- | --- | ---- | ----- | ------
/v1/owners           | -   | Create Owner | - | -
/v1/owners/_{owner}_ | Read Owner | - | - | Delete Owner
/v1/owners/_{owner}_/_{attribute}_ | - | - | Update Attribute | -

### Table Management

End Point            | GET | POST | PATCH | DELETE
-------------------- | --- | ---- | ----- | ------
/v1/tables/_{owner}_ | List Tables | Create Table | - | -
/v1/tables/_{owner}_/_{table}_ | Table Exists | - | - | Delete Table
/v1/tables/_{owner}_/_{table}_/_{attribute}_ | - | - | Update Attribute | -

### Data Management

End Point                | GET | POST | PATCH | DELETE
------------------------ | --- | ---- | ----- | ------
/v1/transaction/start    | - | Start Transaction | - | -
/v1/transaction/commit   | - | Commit Transaction | - | -
/v1/transaction/stage    | - | Add file to Transaction | - | -
/v1/transaction/truncate | - | Truncate table | - | -

### Commit Management

End Point                | GET | POST | PATCH | DELETE
------------------------ | --- | ---- | ----- | ------
/v1/tables/_{owner}_/_{table}_/commits/_{commit}_ | Read Commit | - | - | -
/v1/tables/_{owner}_/_{table}_/commits | List Commits | - | - | -

## Request Fulfillment

**I want to retrive the current instance of a dataset**

    [GET]       /v1/tables/{owner}/{table}/commits/latest

**I want to create a new dataset**

    [POST]      /v1/tables/{owner}

**I want to retrieve a dataset as at a specific date in the past**

    [GET]       /v1/tables/{owner}/{table}/commits?before={timestamp}&limit=1
    [GET]       /v1/tables/{owner}/{table}/commits/{commit}

**I want to update the schema for a dataset**

    [POST]       /v1/views/{owner}/{table}/schemas

**I want to update the metadata for a dataset**

    [POST]      /v1/tables/{owner}/{table}/metadata

**I want to add another file to a dataset (add file to streaming dataset)**

    [POST]      /v1/transactions/start
    [POST]      /v1/transactions/stage
    [POST]      /v1/transactions/commit

**I want to write a new instance of a dataset (rewrite a snapshot dataset)**

    [POST]      /v1/transactions/start
    [POST]      /v1/transactions/truncate
    [POST]      /v1/transactions/stage
    [POST]      /v1/transactions/commit

**I want to know what datasets an owner has**

    [GET]       /v1/tables/{owner}
