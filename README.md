# Tarchia

Tarchia is a table format for huge analytic tables.

Resource      | Location
------------- | -------------
Source Code   | https://github.com/mabel-dev/tarchia
Documentation | https://github.com/mabel-dev/tarchia
Download      | https://github.com/mabel-dev/tarchia

**Terminology**

- **Catalog** - A collection of tables.
- **Data File** - Files that contain the rows of the table.
- **Manifest** - Files that list and describe data files in the table.
- **Metadata** - Information used to manage and describe tables.
- **Owner** - Namespace for table.
- **Schema** - The structure defining the columns of the table.
- **Snapshot** - The state of the table at a specific point in time.
- **Table** - A dataset stored in a structured and managed way.

**Physical Structure**

~~~python
table/
 |- metadata
 |   |- manifests/
 |   |   +- manifest-00000000.avro
 |   +- snapshots/
 |       +- as_at-00000000.json
 +- data/
     +- year=2000/
         +- month=01/
             +- day=01/
                 +- as_at_0000/
                     +- data-0000-0000.parquet
~~~

## Metadata Catalog

~~~mermaid
flowchart TD
    CATALOG[(Catalog)] --> SNAPSHOT(Snapshot)
    CATALOG  --> SCHEMA(Schema)
    subgraph  
        MAN_LIST --> MANIFEST(Manifest)
        SNAPSHOT --> SCHEMA
        SNAPSHOT --> MAN_LIST(Manifest List)
    end
    MANIFEST --> DATA(Data Files)
~~~

## Design

### Catalog Overview
- Hosted in a datastore like FireStore.
- Contains references to Schemas and Snapshots.

### Table Creation and Updates
- **Creation**:
  - Created with a schema but no snapshot (no data initially).
- **Writing to a Table**:
  - Obtain the latest schema (may differ from the latest snapshot if updated).
  - Write the data and save the snapshot.
  - Use a transaction system to ensure complete datasets/updates before catalog updates.

### Handling Clashes
- Ensure the dataset version (latest snapshot) at the start of the transaction matches the version at the end.
- If versions don't match, another change has occurred:
  - The transaction should fail or require a hard override.

### Streaming Datasets
- Add new files to the dataset and create a new manifest.
- Schema changes are limited to:
  - Adding and removing columns.
  - Limited type conversions (e.g., int to float).
- Larger changes are treated as different datasets.

### Table Reading
- Retrieve the schema and the manifest.
- Each snapshot can only have one schema.

### Catalog References
- The latest schema.
- The latest snapshot.
- Key information about the table.

### Manifests
- First split at 4097, (11 years of daily writes, 61m if 15k/file, 200m if 50k/file)
- Otherwise, limited to 2048 rows (aiming for most files to be <2MB to fit in remote cache).
- If a manifest exceeds this number, it is split, and a Manifest List is created.
- Use B-Tree style management for efficient pruning:
  - **Read and Write Overheads**:
    - 1 million row dataset: single manifest.
    - 1 billion row dataset: 33 manifests (1 root and 1 layer with 32 children).
    - 1 trillion row dataset: 32568 manifests in three layers.
  - **Parallel Access**:
    - 1 trillion rows, 32568 manifests: about 16GB of data.
    - Parallel access reduces read and process time.
    - Pruning can quickly reduce reads.

### Data Files
- Manifest and snapshot files do not need to be colocated with the data files.
- Data files do not need to be colocated with each other.

### Updates and Atomicity
- Updates are atomic, effective when the catalog is updated.
- Failed updates may leave artifacts (e.g., orphan files), but the update itself is either successful or not.
- Storage cost for failed commits (orphaned tables) is considered acceptable.

### Pruning
- Effective for columns that are sorted, nearly sorted, or have values appearing for limited periods.
- Ineffective for columns with very few, highly recurrent values (e.g., Gender).
- Effective for columns like dates when working with log entries.

### Indexes
- Intended to operate at a leaf manifest level.
- Aim to balance between:
  - Too many indexes (one per blob).
  - Too few indexes (one per dataset).
- Index strategy is still being developed.

## Compatibility

Tarchia currently only supports:

**Datafiles**: `parquet`
**Catalogs**: FireStore and internal
**Blob Stores**: local

## Git-Like Management

Git      | Function                               | Tarchia
-------- | -------------------------------------- | -------
`init`   | Initialize a new dataset               | [POST] /v1/tables/{owner} 
`add`    | Stage changes to be included in commit | [POST] /v1/tables/{owner}/{table}/stage
`commit` | Save the staged changes to the dataset | [POST] /v1/transactions/commit 
`branch` | Create a new branch of the dataset     | [POST] /v1/transactions/start
<!--`fork`   | Create a copy of a dataset             | [POST] /v1/tables/{owner}/{table}/fork-->

## API Definition

### Owner Management

End Point            | GET | POST | PATCH | DELETE
-------------------- | --- | ---- | ----- | ------
/v1/owners           | - | Create Owner | - | -
/v1/owners/_{owner}_ | Read Owner | - | Update Owner | Delete Owner

### Schema Management

End Point            | GET | POST | PATCH | DELETE
-------------------- | --- | ---- | ----- | ------
/v1/tables/_{owner}_/_{table}_/schemas | Read Schema | - | Update Schema | -

---

[POST]      /v1/tables/{owner}/{table}/stage  
[POST]      /v1/tables/{owner}/{table}/truncate  
[POST]      /v1/transactions/start  
[POST]      /v1/transactions/commit   

[POST]      /v1/tables/{owner}/{table}/push/{snapshot}  
[POST]      /v1/tables/{owner}/{table}/fork

**Table Management**

**[POST]**      /v1/tables/{owner}  
**[GET]**       /v1/tables/{owner}  
**[PATCH]**     /v1/tables/{owner}/{table}  
**[DELETE]**    /v1/tables/{owner}/{table}  
**[GET]**       /v1/tables/{owner}/{table}?as_at={timestamp}&filter={filter}  
**[GET]**       /v1/tables/{owner}/{table}/snaphots/{snapshot}?filter={filter}  
<!---
    

    [POST]      /v1/tables/{owner}/{table}/permissions
    [GET]       /v1/tables/{owner}/{table}/permissions/check
    [POST]      /v1/tables/{owner}/{table}/maintenance/compact
    [POST]      /v1/tables/{owner}/{table}/maintenance/refresh_metadata

    [POST]      /v1/views/{owner}
    [GET]       /v1/views/{owner}
    [GET]       /v1/views/{owner}/{view}
    [DELETE]    /v1/views/{owner}/{view}

    [GET]       /v1/search?query=searchTerm

    [GET]       /v1/tables/{owner}/{table}/lineage
    [GET]       /v1/tables/{owner}/{table}/audit-logs
    [GET]       /v1/views/{owner}/{view}/audit-logs

    [POST]      /v1/tables/{owner}/{table}/actions
    [GET]       /v1/tables/{owner}/{table}/actions
    [DELETE]    /v1/tables/{owner}/{table}/actions/{action}

--->

## Request Fulfillment

**I want to know what datasets there are**

    [GET]       /v1/tables

**I want to retrive the current instance of a dataset**

    [GET]       /v1/tables/{owner}/{table}?

**I want to create a new dataset**

    [POST]      /v1/tables

**I want to retrieve a dataset as at a date in the past**

    [GET]       /v1/tables/{owner}/{table}?

**I want to update the schema for a dataset**

    [POST]       /v1/views/{viewIdentifier}/schemas

**I want to update the metadata for a dataset**

    [POST]      /v1/tables/{owner}/{table}/metadata

**I want to add another file to a dataset**

    [POST]      /v1/transactions/start
    [POST]      /v1/tables/{owner}/{table}/files
    [POST]      /v1/transactions/commit

**I want to write a new instance of a dataset**

    [POST]      /v1/transactions/start
    [POST]      /v1/tables/{owner}/{table}/files/truncate
    [POST]      /v1/tables/{owner}/{table}/files
    [POST]      /v1/transactions/commit

**I want to copy a dataset**

    [POST]      /v1/tables/{owner}/{table}/clone

**I want to make a new version the latest**

    [POST]      /v1/tables/{owner}/{table}/promote

