import sys
import os

os.environ["CATALOG_NAME"] = "test_catalog.json"
os.environ["TARCHIA_DEBUG"] = "TRUE"

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from fastapi.testclient import TestClient
from tarchia.models import CreateTableRequest
from tarchia.models import Column
from tarchia.models import Schema
from tarchia.main import application

def test_create_read_update_delete_table():

    try:
        os.remove(os.environ["CATALOG_NAME"])
    except FileNotFoundError:
        pass

    client = TestClient(application)

    new_table = CreateTableRequest(
        name="test_dataset",
        location="gs://dataset/",
        table_schema=Schema(columns=[Column(name="column")]),
    )

    # create the table
    response = client.post(url="/v1/tables/joocer", content=new_table.model_dump_json())
    assert response.status_code == 200, f"{response.status_code} - {response.content}"

    # can we retrieve this table?
    response = client.get(url="/v1/tables/joocer/test_dataset")
    assert response.status_code == 200, f"{response.status_code} - {response.content}"
    assert response.json()["visibility"] == "PRIVATE"

    # we shouldn't be able to update the schema
    response = client.patch(url="/v1/tables/joocer/test_dataset", content='{"schema": ""}')
    assert response.status_code == 422, f"{response.status_code} - {response.content}"

    # can we update this table
    response = client.patch(url="/v1/tables/joocer/test_dataset", content='{"visibility": "INTERNAL"}')
    assert response.status_code == 200, f"{response.status_code} - {response.content}"
    response = client.get(url="/v1/tables/joocer/test_dataset")
    assert response.status_code == 200, f"{response.status_code} - {response.content}"
    assert response.json()["visibility"] == "INTERNAL", response.json()["visibility"]

if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
