from fastapi.testclient import TestClient
from tarchia.models import CreateOwnerRequest, OwnerType
from main import application

TEST_OWNER = "tester"

def ensure_owner():
    """
    create an owner for tests
    """
    client = TestClient(application)

    owner = CreateOwnerRequest(
        name=TEST_OWNER, steward="billy", type=OwnerType.INDIVIDUAL, memberships=[], description="test"
    )

    # create the owner
    response = client.post(url="/v1/owners", content=owner.serialize())
    assert response.status_code in {200, 409}, f"{response.status_code} - {response.content}"
