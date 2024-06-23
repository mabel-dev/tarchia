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
 |       +- snapshot-00000000.json
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

The Catalog, hosted in a datastore like FireStore, contains references to Schemas and Snapshots. 

When a table is created, it is created with a schema but no snapshot (it doesn't have any data yet).

When a table is written to, the writer should obtain the latest schema (this may not be the same as the latest snapshot, if the schema was updated since), write the data, and saves the snapshot.

Writers use a transaction system to ensure datasets/updates are complete before updating the catalog.

Clashes are managed by ensuring the version (latest snapshot) of the dataset at the start of the transaction matches the version of the dataset at the end of the transaction - if these don't match someone else has made a change and the transaction should fail or require a hard override.

Streaming datasets add new files to the dataset, create a new manifest streaming data cannot change schemas beyond - adding and removing columns, and limited type converstions (e.g. int->float). Larger changes are different datasets.

When a table is read, we get the schema and the manifest. Each snapshot can only have one schema.

The catalog references the latest schema, latest snapshot and key information about the table.

Manifests are limited to 2048 rows (aiming for most files to be <2Mb files to fit in cache), when a manifest exceeds this number it is split and a Manifest list created. Manifest/Manifest Lists use B-Tree style management to help ensure efficient pruning. 

B-Tree manifests will create read and write overheads when accessing and updating, assuming about 15k rows per file; 1 million row dataset would be in a single manifest, a 1 billion row dataset in 33 manifests (1 root and 1 layer with 32 children) and a 1 trillion row dataset in 32568 manifests in three layers. 

1 trillion row's 32568 manifests would be about 16Gb of data, this data could be accessed in parallel reducing the time to read and process all of this data. Pruning would very quickly reduce the reads - eliminating just one row from layer one would avoid reading thousands of manifests. (Data files with 50k rows would only have 9770 Manifests)

The manifest and snapshot files do not need to be colocated with the data files.

The data files don't need to be colocated with each other.

Updates are atomic due to them being effected when the catalog is updated. Failed updates may leave artifacts (like orphan files), but the update was either successful or not, there is no partial success. As storage is cheap, if the cost of a failed commit is orphaned tables, that should be acceptable.

Pruning is only effective for columns that are sorted, or nearly sorted, or columns with values that appear for limited periods of time. Attempting to prune on a column like Gender which has very few, highly recurrant values, is likely to be a waste of effort, pruning on dates when working with log entries, is likely to be quite effective.

It's intended that indexes will operate at a leaf manifest level, providing a balance between too many indexes (one per blob) and too few indexes (one per dataset). This is still to be worked through.

## Git-Like Management

Git      | Function                               | Tarchia
-------- | -------------------------------------- | -------
`init`   | Initialize a new dataset               | [POST] /v1/tables/{owner} 
`add`    | Stage changes to be included in commit | [POST] /v1/tables/{owner}/{table}/stage
`commit` | Save the staged changes to the dataset | [POST] /v1/transactions/commit 
`branch` | Create a new branch of the dataset     | [POST] /v1/transactions/start
`push`   | Send committed changes to dataset      | [POST] /v1/tables/{owner}/{table}/push/{snapshot}
`fork`   | Create a copy of a dataset             | [POST] /v1/tables/{owner}/{table}/fork

## Backing DB Structure

~~~mermaid
erDiagram
    OWNER {
        string name
        string user
    }
    
    CATALOG {
        string name
        string owner

    }

    OWNER ||--o{ CATALOG : "owns"
~~~

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

