"""

- cannot create a view with the name of an existing table
"""

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
from tarchia.models import CreateViewRequest
from main import application
from tests.common import ensure_owner

TEST_OWNER = "tester"

def test_create_read_update_delete_view():

    try:
        os.remove(os.environ["CATALOG_NAME"])
    except FileNotFoundError:
        pass

    ensure_owner()

    client = TestClient(application)

    new_view = CreateViewRequest(
        name="test_view",
        steward="bob",
        description="test",
        statement="SELECT * FROM $planets"
    )

    # can't create the table for non-existent owner
    response = client.post(url=f"/v1/views/s{TEST_OWNER}", content=new_view.serialize())
    assert response.status_code == 404, f"{response.status_code} - {response.content}"

    # create the view
    response = client.post(url=f"/v1/views/{TEST_OWNER}", content=new_view.serialize())
    assert response.status_code == 200, f"{response.status_code} - {response.content}"

    # can we retrieve this view directly?
    response = client.get(url=f"/v1/views/{TEST_OWNER}/test_view")
    assert response.status_code == 200, f"{response.status_code} - {response.content}"
    assert response.json()["statement"] == "SELECT * FROM $planets"

    # can we retrieve this view from the relation endpoint?
    response = client.get(url=f"/v1/relations/{TEST_OWNER}/test_view")
    assert response.status_code == 200, f"{response.status_code} - {response.content}"
    assert response.json()["statement"] == "SELECT * FROM $planets"

    # we shouldn't be able to update the table_id
    response = client.patch(url=f"/v1/tables/{TEST_OWNER}/test_view/view_id", content="1234")
    assert response.status_code == 422, f"{response.status_code} - {response.content}"

    # can we update this table
    response = client.patch(
        url=f"/v1/views/{TEST_OWNER}/test_view/statement", content='{"value":"SELECT VERSION()"}'
    )
    assert response.status_code == 200, f"{response.status_code} - {response.content}"

    response = client.get(url=f"/v1/views/{TEST_OWNER}/test_view")
    assert response.status_code == 200, f"{response.status_code} - {response.content}"
    assert response.json()["statement"] == "SELECT VERSION()", response.json()["visibility"]

    # delete the table
    response = client.delete(url=f"/v1/views/{TEST_OWNER}/test_view")
    assert response.status_code == 200, f"{response.status_code} - {response.content}"

    # table shouldn't exist anymore
    response = client.get(url=f"/v1/views/{TEST_OWNER}/test_view")
    assert response.status_code == 404, f"{response.status_code} - {response.content}"


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
