# tarchia
Mabel &amp; Opteryx Metastore

## API Definition

### Overview


Resource    | POST | GET | PUT | DELETE
:---------- | :--- | :-- | :-- | :-----
datasets       | New dataset |     |     |
columns     |      |     |     |
datasets/blobs       |      |     |     |

### Sataset API

fuller description

#### **Search for dataset**

**_End Point_**

~~~
[GET] /v1.0/datasets/?filter=<filter>&describe=<describe>
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
