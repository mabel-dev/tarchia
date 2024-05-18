# tarchia
Opteryx Metastore

## API Definition

### Overview

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
    [POST]      /v1/tables
    [GET]       /v1/tables
    [GET]       /v1/tables/{tableIdentifier}
    [DELETE]    /v1/tables/{tableIdentifier}
    [GET]       /v1/tables/{tableIdentifier}/schemas
    [POST]      /v1/tables/{tableIdentifier}/schemas
    [POST]      /v1/tables/{tableIdentifier}/clone