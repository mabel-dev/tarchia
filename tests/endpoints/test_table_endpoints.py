"""

- cannot create a column with an invalid name
- cannot create an alias with an invalid name
- cannot create a view with the name of an existing table
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

    try:
        os.remove(os.environ["CATALOG_NAME"])
    except FileNotFoundError:
        pass

    ensure_owner()

    client = TestClient(application)

    new_table = CreateTableRequest(
        name="test_dataset",
        location="gs://dataset/",
        steward="bob",
        table_schema=Schema(columns=[Column(name="column")]),
        freshness_life_in_days=0,
        retention_in_days=0,
        description="test"
    )

    # can't create the table for non-existent owner
    response = client.post(url=f"/v1/tables/s{TEST_OWNER}", content=new_table.serialize())
    assert response.status_code == 404, f"{response.status_code} - {response.content}"

    # create the table
    response = client.post(url=f"/v1/tables/{TEST_OWNER}", content=new_table.serialize())
    assert response.status_code == 200, f"{response.status_code} - {response.content}"

    # can we retrieve this table directly?
    response = client.get(url=f"/v1/tables/{TEST_OWNER}/test_dataset")
    assert response.status_code == 200, f"{response.status_code} - {response.content}"
    assert response.json()["visibility"] == "PRIVATE"

    # can we retrieve this table from the relation end point?
    response = client.get(url=f"/v1/relations/{TEST_OWNER}/test_dataset")
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

    try:
        os.remove(os.environ["CATALOG_NAME"])
    except FileNotFoundError:
        pass

    ensure_owner()

    client = TestClient(application)

    new_table = CreateTableRequest(
        name="test_dataset_metadata_test",
        location="gs://dataset/",
        steward="bob",
        table_schema=Schema(columns=[Column(name="column")]),
        freshness_life_in_days=0,
        retention_in_days=0,
        description="test"
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

    # update the description
    response = client.patch(
        url=f"/v1/tables/{TEST_OWNER}/test_dataset_metadata_test/description",
        content=orjson.dumps({"value": "not test"}),
    )
    assert response.status_code == 200, f"{response.status_code} - {response.content}"

    # confirm the metadata has been updated correctly
    response = client.get(url=f"/v1/tables/{TEST_OWNER}/test_dataset_metadata_test")
    assert response.status_code == 200, f"{response.status_code} - {response.content}"
    entry = response.json()
    metadata = entry["metadata"]
    assert metadata is not None
    assert metadata["set"], metadata
    assert entry["description"] == "not test"

    # delete the table
    response = client.delete(url=f"/v1/tables/{TEST_OWNER}/test_dataset_metadata_test")
    assert response.status_code == 200, f"{response.status_code} - {response.content}"


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
