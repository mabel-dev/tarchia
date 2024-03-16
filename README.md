# tarchia
Mabel &amp; Opteryx Metastore

## API Definition

### Overview

 Resource	            | GET	           | POST            | PUT             | DELETE
:---------------------- | :--------------- | :-------------- | :-------------- | :----- 
/v1/databases           | List Databases   | Create Database |  -              |  - 		
/v1/databases/{id}      | Database Details |  -              | Update Database | Delete Database
/v1/tables              | List Tables      | Create Table    |  -              |  -		
/v1/tables/{id}         | Table Details    |  -              | Update Table    | Delete Table
/v1/tables/{id}/lineage | Get Lineage      |  -              |  -              |  -					
/v1/search              | Search Metadata  |  -              |  - 			   | -

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
[GET] /v1/databases/{id}
~~~

Request: No request body needed; the database ID is specified in the URL path.
Response: Detailed information about the database (e.g., name, tables, creation date).

**Update Database**

Updates the details of an existing database.

~~~
[PUT] /v1/databases/{id}
~~~

Request: JSON body with the updated details of the database (e.g., name).
Response: Updated details of the database.

**Delete Database**

Deletes a specific database by ID.

~~~
[DELETE] /v1/databases/{id}
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
[GET] /v1/tables/{id}
~~~

Request: No request body needed; the table ID is specified in the URL path.
Response: Detailed information about the table (e.g., columns, data types, database ID).

**Update Table**

Updates the details of an existing table.

~~~
[PUT] /v1/tables/{id}
~~~

Request: JSON body with the updated details of the table (e.g., name, columns).
Response: Updated details of the table.

**Delete Table**

Deletes a specific table by ID.

~~~
[DELETE] /v1/tables/{id}
~~~

Request: No request body needed; the table ID is specified in the URL path.
Response: Confirmation of deletion.

**Get Lineage**

Retrieves the data lineage of a specific table by ID.

~~~
[GET] /v1/tables/{id}/lineage
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

-----

Testing -> POST Dataset
Mabel -> GET Dataset
Mabel -> POST blob


- Get Dataset
    - fetches schema
- Add Blob to Datase
    - include partitioning
    - perform profiling

Dataset format:

{
    "canonical_name": "gcs://" ... the path to the blobs,
    "preferred_name": the name as it should appear,
    "aliases": [],
    "schema": [
        {"name", "type", "default", "aliases", "description"}, ...
    ]
}
