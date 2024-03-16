# tarchia
Mabel &amp; Opteryx Metastore

## API Definition

### Overview

 Resource	                 | GET              | POST            | PUT               | DELETE
:--------------------------- | :--------------- | :-------------- | :---------------- | :----- 
/v1/databases                | List Databases   | Create Database |  -                |  - 		
/v1/databases/{id}           | Database Details |  -              | Update Database   | Delete Database
/v1/tables                   | List Tables      | Create Table    |  -                |  -		
/v1/tables/{id}              | Table Details    |  -              | Update Table      | Delete Table
/v1/tables/{id}/morsels      | List Morsels     |  -              |  -                |  -
/v1/tables/{id}/morsels/{id} | Morsel Details   |  -              | Add/Update Morsel | Delete Morsel 
/v1/tables/{id}/lineage      | Get Lineage      |  -              |  -                |  -					
/v1/search                   | Search Metadata  |  -              |  - 			      |  -

### Dataset API

**List Databases**

Retrieves a list of all databases.

~~~
[GET] /v1/databases
~~~

Request: No request body needed.
Response: A list of databases with details (e.g., ID, name).

**Create Database**

Creates a new database.

~~~
[POST] /v1/databases
~~~

Request: JSON body with the details of the database to be created (e.g., name).
Response: Details of the created database, including its ID.

**Database Details**

Retrieves details of a specific database by ID.

~~~
[GET] /v1/databases/{databaseId}
~~~

Request: No request body needed; the database ID is specified in the URL path.
Response: Detailed information about the database (e.g., name, tables, creation date).

**Update Database**

Updates the details of an existing database.

~~~
[PUT] /v1/databases/{databaseId}
~~~

Request: JSON body with the updated details of the database (e.g., name).
Response: Updated details of the database.

**Delete Database**

Deletes a specific database by ID.

~~~
[DELETE] /v1/databases/{databaseId}
~~~

Request: No request body needed; the database ID is specified in the URL path.
Response: Confirmation of deletion.

**List Tables**

Retrieves a list of all tables across databases.

~~~
[GET] /v1/tables
~~~

Request: No request body needed.
Response: A list of tables with details (e.g., ID, name, database association).

**Create Table**

Creates a new table in a database.

~~~
[POST] /v1/tables
~~~

Request: JSON body with the details of the table to be created (e.g., name, columns).
Response: Details of the created table, including its ID.

**Table Details**

Retrieves details of a specific table by ID.

~~~
[GET] /v1/tables/{tableId}
~~~

Request: No request body needed; the table ID is specified in the URL path.
Response: Detailed information about the table (e.g., columns, data types, database ID).

**Update Table**

Updates the details of an existing table.

~~~
[PUT] /v1/tables/{tableId}
~~~

Request: JSON body with the updated details of the table (e.g., name, columns).
Response: Updated details of the table.

**Delete Table**

Deletes a specific table by ID.

~~~
[DELETE] /v1/tables/{tableId}
~~~

Request: No request body needed; the table ID is specified in the URL path.
Response: Confirmation of deletion.

**List Morsels in Table Manifest**

Retrieves a list of morsel manifest entries associated with a specific table.

~~~
[GET] /v1/tables/{tableId}/morsels
~~~

**Get Morsel Details**

Retrieves details of a specific morsel manifest entry by its ID.

~~~
[GET] /v1/tables/{tableId}/morsels/{morselId}
~~~

**Update (or Add) Morsel in Table Manifest**

Updates the details of an existing morsel manifest entry or adds a new morsel entry if it does not exist. This approach assumes idempotency, where the PUT method can be used to update existing entries or create new ones if the specified morselId does not exist within the table's manifest.

~~~
[PUT] /v1/tables/{tableId}/morsels/{morselId}
~~~

**Delete Morsel from Table Manifest**

Deletes a specific morsel manifest entry from a table's manifest by its ID.

~~~
[DELETE] /v1/tables/{tableId}/morsels/{morselId}
~~~

**Get Lineage**

Retrieves the data lineage of a specific table by ID.

~~~
[GET] /v1/tables/{tableId}/lineage
~~~

Request: No request body needed; the table ID is specified in the URL path.
Response: Lineage information, showing data sources, transformations, and destinations.

**Search Metadata**

Searches metadata across databases and tables.

~~~
[GET] /v1/search
~~~

Request: Query parameters to specify the search criteria (e.g., name, type).
Response: List of search results matching the criteria, including relevant databases and tables.


**_Query Parameters_**

Parameter | Type    | Required | Description
:-------- | :------ | :------- | :-----------
filter    | string  | No       | Regular Expression filter for dataset names (preferred and aliases).
describe  | boolean | No       | Return extended details, including schema information.

**_Example_**

_Request_

~~~bash
curl -i -X GET \
-H "Accept: application/json" \
-H "Content-type: application/json" \
"https://{host}:8080/v1.0/datasets/?filter=.*opteryx.*"
~~~

_Response_

~~~
~~~

#### **Record Blob**

**_End Point_**

~~~
[POST] /v1.0/datasets/{dataset}/blobs
~~~

**_Query Parameters_**

Parameter | Type    | Required | Description
:-------- | :------ | :------- | :-----------
dataset

**_Body Parameters_** (json)

Parameter | Type    | Required | Description
:-------- | :------ | :------- | :-----------
location

**_Response_** (raw)

~~~
{blob_identifier}
~~~

Parameter       | Type    | Description
:-------------- | :------ | :-----------
blob_identifier | string  | 

The blob_identifier is created using this approach to avoid collisions overwriting data:

~~~
identifier = CityHash64(blob location)
while collision:
    indentifier = CityHash64(identifier + blob location)
~~~

**_Example_**

~~~bash
curl -X 'POST' \
  'http://{host}:8080/v1.0/datasets/12345/blobs' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "location": "sample/space_missions.parquet"
}'
~~~
