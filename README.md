# tarchia
Opteryx Metastore

Terminology

- **Catalog** - A collection of tables.
- **Data File** - Files that contain the rows of the table.
- **Index** - A structure that improves data retrieval speed.
- **Manifest** - Files that list and describe data files in the table.
- **Metadata** - Information used to manage and describe tables.
- **Schema** - The structure defining the columns of the table.
- **Snapshot** - The state of the table at a specific point in time.
- **Statistics** - Summary information about the data in columns.
- **Table** - A dataset stored in a structured and managed way.


~~~python
table/
 |- metadata
# |   |- indexes/
# |   |   +- index-0000-0000.index
 |   |- manifests/
 |   |   +- manifest-0000-0000.avro
 |   |- snapshots/
 |   |   +- snapshot-0000-0000.json
# |   +- statistics/
# |       +- statistics-0000-0000.json
 +- data/
     +- year=2000
         +- month=01
             +- day=01
                 +- hour=00
                     +- data-0000-0000.parquet
~~~

## Metadata Catalog

~~~mermaid
flowchart TD
    CATALOG[(Catalog)] --> SNAPSHOT(Snapshot)
    CATALOG  --> SCHEMA(Schema)
    SNAPSHOT --> SCHEMA
    SNAPSHOT --> MANIFEST(Manifest)
    SNAPSHOT --> INDEX(Indexes)
    SNAPSHOT --> STATS(Statistics)
    MANIFEST --> DATA(Data Files)
~~~

The Catalog contains references to Schemas and Snapshots. 


## API Definition

### Overview

    [POST]      /v1/tables
    [GET]       /v1/tables
    [GET]       /v1/tables/{tableIdentifier}
    [DELETE]    /v1/tables/{tableIdentifier}

<!---
    [POST]      /v1/tables/{tableIdentifier}/permissions
    [GET]       /v1/tables/{tableIdentifier}/permissions/check
    [POST]      /v1/transactions/start
    [POST]      /v1/transactions/commit
    [DELETE]    /v1/tables/{tableIdentifier}/files
    [GET]       /v1/tables/{tableIdentifier}/files
    [POST]      /v1/tables/{tableIdentifier}/files
    [GET]       /v1/tables/{tableIdentifier}/snapshots
    [POST]      /v1/tables/{tableIdentifier}/snapshots
    [POST]      /v1/tables/{tableIdentifier}/maintenance/compact
    [POST]      /v1/tables/{tableIdentifier}/maintenance/refresh_metadata
    [POST]      /v1/tables/{tableIdentifier}/metadata
    [GET]       /v1/tables/{tableIdentifier}/metadata
    [POST]      /v1/tables/{tableIdentifier}/clone

    [POST]      /v1/views
    [GET]       /v1/views
    [GET]       /v1/views/{viewIdentifier}
    [DELETE]    /v1/views/{viewIdentifier}
    [GET]       /v1/views/{viewIdentifier}/schemas
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