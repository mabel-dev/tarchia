"""
OWNERS
- create
- update
- delete
- cannot delete an owner with tables
"""

import sys
import orjson
import os

os.environ["CATALOG_NAME"] = "test_catalog.json"
os.environ["TARCHIA_DEBUG"] = "TRUE"

try:
    os.remove(os.environ["CATALOG_NAME"])
except FileNotFoundError:
    pass

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from fastapi.testclient import TestClient
from tarchia.models import CreateTableRequest, CreateOwnerRequest, OwnerType
from tarchia.models import Column
from tarchia.models import Schema
from main import application
from tests.common import ensure_owner


def test_create_read_update_delete_owner():

    client = TestClient(application)

    owner = CreateOwnerRequest(
        name="joocer", steward="billy", type=OwnerType.INDIVIDUAL, memberships=[]
    )

    # create the owner
    response = client.post(url="/v1/owners", content=owner.serialize())
    assert response.status_code in {200, 409}, f"{response.status_code} - {response.content}"

    # can we retrieve this owner?
    response = client.get(url="/v1/owners/joocer")
    assert response.status_code == 200, f"{response.status_code} - {response.content}"
    assert response.json()["steward"] == "billy"

    # we shouldn't be able to update the owner_id
    response = client.patch(url="/v1/owners/joocer/owner_id", content="1234")
    assert response.status_code == 422, f"{response.status_code} - {response.content}"

    # can we update this owner
    response = client.patch(url="/v1/owners/joocer/steward", content='{"value":"bobby"}')
    assert response.status_code == 200, f"{response.status_code} - {response.content}"

    response = client.get(url="/v1/owners/joocer")
    assert response.status_code == 200, f"{response.status_code} - {response.content}"
    assert response.json()["steward"] == "bobby"

    # delete the table
    response = client.delete(url="/v1/owners/joocer")
    assert response.status_code == 200, f"{response.status_code} - {response.content}"

    # table shouldn't exist anymore
    response = client.get(url="/v1/owners/joocer")
    assert response.status_code == 404, f"{response.status_code} - {response.content}"


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    test_create_read_update_delete_owner()
    run_tests()
