import sys
import os

os.environ["CATALOG_NAME"] = "test_catalog.json"
os.environ["TARCHIA_DEBUG"] = "TRUE"

try:
    os.remove(os.environ["CATALOG_NAME"])
except FileNotFoundError:
    pass

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from fastapi.testclient import TestClient
from tarchia.models import CreateOwnerRequest, OwnerType
from tarchia.models import CreateTableRequest
from tarchia.models import Column
from tarchia.models import Schema
from main import application
from tests.common import ensure_owner

TEST_OWNER = "tester"


def test_create_read_update_delete_owner():

    try:
        os.remove(os.environ["CATALOG_NAME"])
    except FileNotFoundError:
        pass

    client = TestClient(application)

    owner = CreateOwnerRequest(
        name=TEST_OWNER, steward="billy", type=OwnerType.INDIVIDUAL, memberships=[], description="test"
    )

    # create the owner
    response = client.post(url="/v1/owners", content=owner.serialize())
    assert response.status_code in {200, 409}, f"{response.status_code} - {response.content}"

    # we can't create an owner with an existing name
    response = client.post(url="/v1/owners", content=owner.serialize())
    assert response.status_code == 409, f"{response.status_code} - {response.content}"

    # can we retrieve this owner?
    response = client.get(url=f"/v1/owners/{TEST_OWNER}")
    assert response.status_code == 200, f"{response.status_code} - {response.content}"
    assert response.json()["steward"] == "billy"

    # we shouldn't be able to update the owner_id
    response = client.patch(url=f"/v1/owners/{TEST_OWNER}/owner_id", content='{"value":"bobby"}')
    assert response.status_code == 405, f"{response.status_code} - {response.content}"

    # can we update this owner
    response = client.patch(url=f"/v1/owners/{TEST_OWNER}/steward", content='{"value":"bobby"}')
    assert response.status_code == 200, f"{response.status_code} - {response.content}"

    # can we update this owner
    response = client.patch(url=f"/v1/owners/{TEST_OWNER}/description", content='{"value":"not test"}')
    assert response.status_code == 200, f"{response.status_code} - {response.content}"

    # is the owner updated
    response = client.get(url=f"/v1/owners/{TEST_OWNER}")
    assert response.status_code == 200, f"{response.status_code} - {response.content}"
    entry = response.json()
    assert entry["steward"] == "bobby"
    assert entry["description"] == "not test"
    

    # delete the owner
    response = client.delete(url=f"/v1/owners/{TEST_OWNER}")
    assert response.status_code == 200, f"{response.status_code} - {response.content}"

    # owner shouldn't exist anymore
    response = client.get(url=f"/v1/owners/{TEST_OWNER}")
    assert response.status_code == 404, f"{response.status_code} - {response.content}"

def test_owner_rules():

    try:
        os.remove(os.environ["CATALOG_NAME"])
    except FileNotFoundError:
        pass

    ensure_owner()

    client = TestClient(application)

    new_table = CreateTableRequest(
        name="test_owner_rules",
        location="gs://dataset/",
        steward="bob",
        table_schema=Schema(columns=[Column(name="column")]),
        freshness_life_in_days=0,
        retention_in_days=0,
        description="test"
    )

    # add a table
    response = client.post(url=f"/v1/tables/{TEST_OWNER}", content=new_table.serialize())
    assert response.status_code in {200, 409}, f"{response.status_code} - {response.content}"

    # can't delete the owner when they have tables
    response = client.delete(url=f"/v1/owners/{TEST_OWNER}")
    assert response.status_code == 409, f"{response.status_code} - {response.content}"

    owner = CreateOwnerRequest(
        name=TEST_OWNER, steward="***", type=OwnerType.INDIVIDUAL, memberships=[], description="test"
    )

    # can't create an owner with an invalid name
    response = client.post(url="/v1/owners", content=owner.serialize())
    assert response.status_code == 409, f"{response.status_code} - {response.content}"


def test_invalid_owner():

    try:
        os.remove(os.environ["CATALOG_NAME"])
    except FileNotFoundError:
        pass

    client = TestClient(application)

    owner = CreateOwnerRequest(
        name="$owner", steward="billy", type=OwnerType.INDIVIDUAL, memberships=[], description="test"
    )

    # can't crreate an owner with an invalid name
    response = client.post(url="/v1/owners", content=owner.serialize())
    assert response.status_code == 422, f"{response.status_code} - {response.content}"


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
