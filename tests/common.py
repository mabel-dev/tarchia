
from fastapi.testclient import TestClient
from tarchia.models import CreateOwnerRequest, OwnerType
from main import application

def ensure_owner():
    """
    create an owner for tests
    """
    client = TestClient(application)

    owner = CreateOwnerRequest(
        name="joocer",
        steward="billy",
        type=OwnerType.INDIVIDUAL,
        memberships=[]
    )

    # create the owner
    response = client.post(url="/v1/owners", content=owner.serialize())
    assert response.status_code in {200, 409}, f"{response.status_code} - {response.content}"