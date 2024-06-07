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

Streaming datasets add new files to the dataset, create a new manifest streaming data cannot change schemas beyond - adding and removing columns, and limited type converstions (e.g. int->float). Larger changes are different datasets.

When a table is read, we get the schema and the manifest. Each snapshot can only have one schema.

The catalog references the latest schema, latest snapshot and key information about the table.

Manifests are limited to 1000 rows (1000 = 1.6Mb, aiming for <2Mb files to fit in cache), when a manifest exceeds this number it is split and a Manifest list created. Manifest/Manifest Lists use B-Tree style management to help ensure efficient pruning. 

Small and medium sized tables (roughly fewer than 10 million records [depends on entries per file]) are likely to only ever need one layer on manifests, larger tables (usually those which are streams) will have many manifests, each entry in a manifest can be another manifest or a blob. Creating a tree-like structure.

The manifest and snapshot files do not need to be colocated with the data files.

The data files don't need to be colocated with each other.

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
