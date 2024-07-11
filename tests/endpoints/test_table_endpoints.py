"""

- cannot create a column with an invalid name
- cannot create an alias with an invalid name
"""

import sys
import os
import orjson

os.environ["CATALOG_NAME"] = "test_catalog.json"
os.environ["TARCHIA_DEBUG"] = "TRUE"

try:
    os.remove(os.environ["CATALOG_NAME"])
except FileNotFoundError:
    pass

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from fastapi.testclient import TestClient
from tarchia.models import CreateTableRequest
from tarchia.models import Column
from tarchia.models import Schema
from main import application
from tests.common import ensure_owner

TEST_OWNER = "tester"

def test_create_read_update_delete_table():

    ensure_owner()

    client = TestClient(application)

    new_table = CreateTableRequest(
        name="test_dataset",
        location="gs://dataset/",
        steward="bob",
        table_schema=Schema(columns=[Column(name="column")]),
    )

    # can't create the table for non-existent owner
    response = client.post(url=f"/v1/tables/s{TEST_OWNER}", content=new_table.serialize())
    assert response.status_code == 404, f"{response.status_code} - {response.content}"

    # create the table
    response = client.post(url=f"/v1/tables/{TEST_OWNER}", content=new_table.serialize())
    assert response.status_code == 200, f"{response.status_code} - {response.content}"

    # can we retrieve this table?
    response = client.get(url=f"/v1/tables/{TEST_OWNER}/test_dataset")
    assert response.status_code == 200, f"{response.status_code} - {response.content}"
    assert response.json()["visibility"] == "PRIVATE"

    # we shouldn't be able to update the table_id
    response = client.patch(url=f"/v1/tables/{TEST_OWNER}/test_dataset/table_id", content="1234")
    assert response.status_code == 422, f"{response.status_code} - {response.content}"

    # can we update this table
    response = client.patch(
        url=f"/v1/tables/{TEST_OWNER}/test_dataset/visibility", content='{"value":"INTERNAL"}'
    )
    assert response.status_code == 200, f"{response.status_code} - {response.content}"

    response = client.get(url=f"/v1/tables/{TEST_OWNER}/test_dataset")
    assert response.status_code == 200, f"{response.status_code} - {response.content}"
    assert response.json()["visibility"] == "INTERNAL", response.json()["visibility"]

    # delete the table
    response = client.delete(url=f"/v1/tables/{TEST_OWNER}/test_dataset")
    assert response.status_code == 200, f"{response.status_code} - {response.content}"

    # table shouldn't exist anymore
    response = client.get(url=f"/v1/tables/{TEST_OWNER}/test_dataset")
    assert response.status_code == 404, f"{response.status_code} - {response.content}"


def test_maintain_table_metadata():

    ensure_owner()

    client = TestClient(application)

    new_table = CreateTableRequest(
        name="test_dataset_metadata_test",
        location="gs://dataset/",
        steward="bob",
        table_schema=Schema(columns=[Column(name="column")]),
    )

    # create the table
    response = client.post(url=f"/v1/tables/{TEST_OWNER}", content=new_table.serialize())
    assert response.status_code == 200, f"{response.status_code} - {response.content}"

    # confirm we know the metadata value before we start
    response = client.get(url=f"/v1/tables/{TEST_OWNER}/test_dataset_metadata_test")
    assert response.status_code == 200, f"{response.status_code} - {response.content}"
    assert response.json()["metadata"] == {}, response.json()["metadata"]

    # update the metadata
    response = client.patch(
        url=f"/v1/tables/{TEST_OWNER}/test_dataset_metadata_test/metadata",
        content=orjson.dumps({"metadata": {"set": True}}),
    )
    assert response.status_code == 200, f"{response.status_code} - {response.content}"

    # confirm the metadata has been updated correctly
    response = client.get(url=f"/v1/tables/{TEST_OWNER}/test_dataset_metadata_test")
    assert response.status_code == 200, f"{response.status_code} - {response.content}"
    metadata = response.json()["metadata"]
    assert metadata is not None
    assert metadata["set"], metadata

    # delete the table
    response = client.delete(url=f"/v1/tables/{TEST_OWNER}/test_dataset_metadata_test")
    assert response.status_code == 200, f"{response.status_code} - {response.content}"


def test_maintain_table_schema():

    ensure_owner()

    client = TestClient(application)

    new_table = CreateTableRequest(
        name="test_dataset_schema_test",
        location="gs://dataset/",
        steward="bob",
        table_schema=Schema(columns=[Column(name="column")]),
    )

    # create the table
    response = client.post(url=f"/v1/tables/{TEST_OWNER}", content=new_table.serialize())
    assert response.status_code == 200, f"{response.status_code} - {response.content}"

    # confirm we know the schema value before we start
    response = client.get(url=f"/v1/tables/{TEST_OWNER}/test_dataset_schema_test")
    assert response.status_code == 200, f"{response.status_code} - {response.content}"
    current_schema = response.json()["current_schema"] 
    assert current_schema == {"columns": [{"name": "column", "aliases": [], "default": None, "description": "", "type": "VARCHAR"}]}, current_schema

    # update the schema
    response = client.patch(
        url=f"/v1/tables/{TEST_OWNER}/test_dataset_schema_test/schema",
        content=Schema(columns=[Column(name="new", type="VARCHAR", default="true")]).serialize(),
    )
    assert response.status_code == 200, f"{response.status_code} - {response.content}"

    # confirm the schema has been updated correctly
    response = client.get(url=f"/v1/tables/{TEST_OWNER}/test_dataset_schema_test")
    assert response.status_code == 200, f"{response.status_code} - {response.content}"
    schema = response.json()["current_schema"]
    assert schema is not None
    assert response.json()["current_schema"] == {
        "columns": [{"name": "new", "aliases": [], "default": "true", "description": "", "type": "VARCHAR"}]
    }, schema

    # delete the table
    response = client.delete(url=f"/v1/tables/{TEST_OWNER}/test_dataset_schema_test")
    assert response.status_code == 200, f"{response.status_code} - {response.content}"


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    test_maintain_table_schema()
    run_tests()
