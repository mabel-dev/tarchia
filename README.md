# tarchia
Mabel &amp; Opteryx Metastore

## API Definition

### Overview

 Resource	                 | GET              | POST            | PUT               | DELETE
:--------------------------- | :--------------- | :-------------- | :---------------- | :----- 
/v1/stores                 | List Stores      | Create Store.   |  -                |  - 		
/v1/stores/{id}            | Store Details    |  -               | Update Store      | Delete Store
/v1/datasets               | List Datasets    | Create Dataset  |  -                |  -		
/v1/datasets/{id}          | Dataset Details  |  -               | Update Table      | Delete Table
/v1/datasets/{id}/morsels  | List Morsels     |  -              |  -                |  -
/v1/datasets/{id}/morsels/{id} | Morsel Details   |  -              | Add/Update Morsel | Delete Morsel 
/v1/datasets/{id}/lineage  | Get Lineage      |  -              |  -                |  -					
/v1/search                   | Search Metadata  |  -              |  - 			      |  -

### End Point Details

#### List Stores

Retrieves a list of all data stores.

~~~
[GET] /v1/stores
~~~

Request: No request body needed.
Response: A list of databases with details (e.g., ID, name).

#### Create Store

Creates a new data store.

~~~
[POST] /v1/stores
~~~

Request: JSON body with the details of the database to be created (e.g., name).
Response: Details of the created database, including its ID.

#### Store Details

Retrieves details of a specific store by ID.

~~~
[GET] /v1/stores/{storeId}
~~~

Request: No request body needed; the database ID is specified in the URL path.
Response: Detailed information about the database (e.g., name, tables, creation date).

#### Update Store

Updates the details of an existing data store.

~~~
[PUT] /v1/stores/{storeId}
~~~

Request: JSON body with the updated details of the database (e.g., name).
Response: Updated details of the database.

#### Delete Store

Deletes a specific store by ID.

~~~
[DELETE] /v1/stores/{storeId}
~~~

Request: No request body needed; the database ID is specified in the URL path.
Response: Confirmation of deletion.

#### List datasets 

Retrieves a list of all tables across databases.

~~~
[GET] /v1/datasets 
~~~

Request: No request body needed.
Response: A list of tables with details (e.g., ID, name, database association).

#### Create Datasets 

Creates a new dataset in a database.

~~~
[POST] /v1/tables
~~~

Request: JSON body with the details of the table to be created (e.g., name, columns).
Response: Details of the created table, including its ID.

#### Table Details

Retrieves details of a specific table by ID.

~~~
[GET] /v1/tables/{tableId}
~~~

Request: No request body needed; the table ID is specified in the URL path.
Response: Detailed information about the table (e.g., columns, data types, database ID).

#### Update Table

Updates the details of an existing table.

~~~
[PUT] /v1/tables/{tableId}
~~~

Request: JSON body with the updated details of the table (e.g., name, columns).
Response: Updated details of the table.

#### Delete Table

Deletes a specific table by ID.

~~~
[DELETE] /v1/tables/{tableId}
~~~

Request: No request body needed; the table ID is specified in the URL path.
Response: Confirmation of deletion.

#### List Morsels in Table Manifest

Retrieves a list of morsel manifest entries associated with a specific table.

~~~
[GET] /v1/tables/{tableId}/morsels
~~~

#### Get Morsel Details

Retrieves details of a specific morsel manifest entry by its ID.

~~~
[GET] /v1/tables/{tableId}/morsels/{morselId}
~~~

#### Update (or Add) Morsel in Table Manifest

Updates the details of an existing morsel manifest entry or adds a new morsel entry if it does not exist. This approach assumes idempotency, where the PUT method can be used to update existing entries or create new ones if the specified morselId does not exist within the table's manifest.

~~~
[PUT] /v1/tables/{tableId}/morsels/{morselId}
~~~

#### Delete Morsel from Table Manifest

Deletes a specific morsel manifest entry from a table's manifest by its ID.

~~~
[DELETE] /v1/tables/{tableId}/morsels/{morselId}
~~~

#### Get Lineage

Retrieves the data lineage of a specific table by ID.

~~~
[GET] /v1/tables/{tableId}/lineage
~~~

Request: No request body needed; the table ID is specified in the URL path.
Response: Lineage information, showing data sources, transformations, and destinations.

#### Search Metadata

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


**_Query Parameters_**

Parameter | Type    | Required | Description
:-------- | :------ | :------- | :-----------
dataset

**_Body Parameters_** (json)

Parameter | Type    | Required | Description
:-------- | :------ | :------- | :-----------
location

**_Response_** (raw)
