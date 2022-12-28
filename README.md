# tarchia
Mabel &amp; Opteryx Metastore



## API Definition

### Overview


Resource       | POST | GET | PUT | DELETE
:------------- | :--- | :-- | :-- | :-----
datasets       | New dataset |     |     |
datasets/blobs | Add Blob to dataset |     |     |

### Dataset API

fuller description

#### **Search for dataset**

**_End Point_**

~~~
[GET] /v1.0/datasets/?filter={filter}&describe={describe}
~~~

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
"https://<domain>/v1.0/datasets/?filter=.*opteryx.*"
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
