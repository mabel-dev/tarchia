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
