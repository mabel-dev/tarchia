# tarchia
Opteryx Metastore

Terminology

- **Catalog** - A collection of tables.
- **Data File** - Files that contain the rows of the table.
- **Manifest** - Files that list and describe data files in the table.
- **Metadata** - Information used to manage and describe tables.
- **Schema** - The structure defining the columns of the table.
- **Snapshot** - The state of the table at a specific point in time.
- **Table** - A dataset stored in a structured and managed way.


~~~python
table/
 |- metadata
 |   |- manifests/
 |   |   +- manifest-0000-0000.avro
 |   +- snapshots/
 |       +- snapshot-0000-0000.json
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

Clashes are managed by ensuring the version (latest snapshot) of the dataset at the start of the transaction matches the version of the dataset at the end of the transaction - if these don't match someone else has made a change and the transaction should fail.

Streaming datasets add new files to the dataset, create a new manifest streaming data cannot change schemas beyond - adding and removing columns, and limited type converstions (e.g. int->float). Larger changes are different datasets.

When a table is read, we get the schema and the manifest. Each snapshot can only have one schema.

The catalog references the latest schema, latest snapshot and key information about the table.

Manifests are limited to 1000 rows (1000 = 1.6Mb, aiming for <2Mb files to fit in cache), when a manifest exceeds this number it is split and a Manifest list created. Manifest/Manifest Lists use B-Tree style management to help ensure efficient pruning. 

B-Tree manifests will create read and write overheads when accessing and updating, assuming about 15k rows per file; 1 million row dataset would be in a single manifest, a 1 billion row dataset in 67 manifests (1 root and 1 layer with 66 children) and a 1 trillion row dataset in 66733 manifests in three layers. 

1 trillion row's 66733 manifests would be about 16Gb of data, just manifests, this data could be accessed in parallel reducing the time to read and process all of this data. Pruning would very quickly reduce the reads - eliminating just one row from layer one would avoid reading about 4000 manifests.

The manifest and snapshot files do not need to be colocated with the data files.

The data files don't need to be colocated with each other.

Updates are atomic due to them being effected when the catalog is updated. Failed updates may leave artifacts (like orphan files), but the update was either successful or not, there is no partial success. As storage is cheap, if the cost of a failed commit is orphaned tables, that should be acceptable.

Pruning is only effective for columns that are sorted, or nearly sorted, or columns with values that appear for limited periods of time. Attempting to prune on a column like Gender which has very few, highly recurrant values, is likely to be a waste of effort, pruning on dates when working with log entries, is likely to be quite effective.

It's intended that indexes will operate at a leaf manifest level, providing a balance between too many indexes (one per blob) and too few indexes (one per dataset). This is still to be worked through.

## API Definition

### Overview

    [POST]      /v1/tables
    [GET]       /v1/tables
    [DELETE]    /v1/tables/{tableIdentifier}
    [GET]       /v1/tables/{tableIdentifier}?filter={filter}&as_at={timestamp}
    [GET]       /v1/tables/{tableIdentifier}/{snapshotIdentifier}
    [POST]      /v1/views/{viewIdentifier}/schemas
    [GET]       /v1/views/{viewIdentifier}/schemas
    [POST]      /v1/tables/{tableIdentifier}/files
    [POST]      /v1/tables/{tableIdentifier}/files/truncate
    [POST]      /v1/transactions/start
    [POST]      /v1/transactions/commit
    [POST]      /v1/tables/{tableIdentifier}/metadata
    [POST]      /v1/tables/{tableIdentifier}/clone

<!---
    [POST]      /v1/tables/{tableIdentifier}/permissions
    [GET]       /v1/tables/{tableIdentifier}/permissions/check
    [POST]      /v1/tables/{tableIdentifier}/maintenance/compact
    [POST]      /v1/tables/{tableIdentifier}/maintenance/refresh_metadata

    [POST]      /v1/views
    [GET]       /v1/views
    [GET]       /v1/views/{viewIdentifier}
    [DELETE]    /v1/views/{viewIdentifier}
    [GET]       /v1/views/{viewIdentifier}/metadata
    [POST]      /v1/views/{viewIdentifier}/metadata

    [GET]       /v1/search?query=searchTerm

    [POST]      /v1/tables/{tableIdentifier}/quality-rules
    [GET]       /v1/tables/{tableIdentifier}/quality-rules
    [DELETE]    /v1/tables/{tableIdentifier}/quality-rules/{ruleIdentifier}
    [POST]      /v1/tables/{tableIdentifier}/quality-rules/{ruleIdentifier}/validate

    [GET]       /v1/tables/{tableIdentifier}/lineage
    [GET]       /v1/tables/{tableIdentifier}/audit-logs
    [GET]       /v1/views/{viewIdentifier}/audit-logs

    [POST]      /v1/tables/{tableIdentifier}/triggers
    [GET]       /v1/tables/{tableIdentifier}/triggers
    [DELETE]    /v1/tables/{tableIdentifier}/triggers/{triggerIdentifier}

    INDEX APIs
--->

## Request Fulfillment

**I want to know what datasets there are**

    [GET]       /v1/tables

**I want to retrive the current instance of a dataset**

    [GET]       /v1/tables/{tableIdentifier}?

**I want to create a new dataset**

    [POST]      /v1/tables

**I want to retrieve a dataset as at a date in the past**

    [GET]       /v1/tables/{tableIdentifier}?

**I want to update the schema for a dataset**

    [POST]       /v1/views/{viewIdentifier}/schemas

**I want to update the metadata for a dataset**

    [POST]      /v1/tables/{tableIdentifier}/metadata

**I want to add another file to a dataset**

    [POST]      /v1/transactions/start
    [POST]      /v1/tables/{tableIdentifier}/files
    [POST]      /v1/transactions/commit

**I want to write a new instance of a dataset**

    [POST]      /v1/transactions/start
    [POST]      /v1/tables/{tableIdentifier}/files/truncate
    [POST]      /v1/tables/{tableIdentifier}/files
    [POST]      /v1/transactions/commit

**I want to copy a dataset**

    [POST]      /v1/tables/{tableIdentifier}/clone
