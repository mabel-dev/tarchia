import sys
import os

os.environ["CATALOG_NAME"] = "test_catalog.json"

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from tarchia import config
from fastapi.testclient import TestClient
from tarchia.models import CreateTableRequest
from tarchia.models import Column
from tarchia.models import Schema
from tarchia.main import application
from orso.tools import random_string

def test_create_read_update_delete_table():

    AUTH_TOKEN = random_string()
    os.environ["AUTH_TOKEN"] = AUTH_TOKEN
    cookies = {"AUTH_TOKEN": AUTH_TOKEN}

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
    response = client.post(url="/v1/tables/joocer", cookies=cookies, content=new_table.model_dump_json())
    assert response.status_code == 200, f"{response.status_code} - {response.content}"

    # can we retrieve this table?
    response = client.get(url="/v1/tables/joocer/test_dataset", cookies=cookies)
    assert response.status_code == 200, f"{response.status_code} - {response.content}"
    assert response.json()["visibility"] == "PRIVATE"

    # we shouldn't be able to update the schema
    response = client.patch(url="/v1/tables/joocer/test_dataset", cookies=cookies, content='{"schema": ""}')
    assert response.status_code == 422, f"{response.status_code} - {response.content}"

    # can we update this table
    response = client.patch(url="/v1/tables/joocer/test_dataset", cookies=cookies, content='{"visibility": "INTERNAL"}')
    assert response.status_code == 200, f"{response.status_code} - {response.content}"
    response = client.get(url="/v1/tables/joocer/test_dataset", cookies=cookies)
    assert response.status_code == 200, f"{response.status_code} - {response.content}"
    assert response.json()["visibility"] == "INTERNAL", response.json()["visibility"]

if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    test_create_read_update_delete_table()
#    run_tests()
