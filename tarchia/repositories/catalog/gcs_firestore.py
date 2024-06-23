"""
This is not intended for production use, however it is being used for
development, prototyping and for regression testing.
"""

from typing import List

from tarchia import config
from tarchia.exceptions import AmbiguousTableError
from tarchia.exceptions import MissingDependencyError
from tarchia.exceptions import UnmetRequirementError
from tarchia.models import TableCatalogEntry
from tarchia.repositories.catalog.provider_base import CatalogProvider

GCP_PROJECT_ID = config.GCP_PROJECT_ID


def _get_project_id():  # pragma: no cover
    """Fetch the ID from GCP"""
    try:
        import requests
    except ImportError as exception:  # pragma: no cover
        raise UnmetRequirementError(
            "Firestore requires 'GCP_PROJECT_ID` to be set in config, or "
            "`requests` to be installed."
        ) from exception

    # if it's set in the config/environ, use that
    if GCP_PROJECT_ID:
        return GCP_PROJECT_ID

    # otherwise try to get it from GCP
    response = requests.get(
        "http://metadata.google.internal/computeMetadata/v1/project/project-id",
        headers={"Metadata-Flavor": "Google"},
        timeout=10,
    )
    response.raise_for_status()
    return response.text


def _initialize():  # pragma: no cover
    """Create the connection to Firebase"""
    try:
        import firebase_admin
        from firebase_admin import credentials
    except ImportError as err:  # pragma: no cover
        raise MissingDependencyError(err.name) from err
    if not firebase_admin._apps:
        # if we've not been given the ID, fetch it
        project_id = GCP_PROJECT_ID
        if project_id is None:
            project_id = _get_project_id()
        creds = credentials.ApplicationDefault()
        firebase_admin.initialize_app(creds, {"projectId": project_id, "httpTimeout": 10})


class FirestoreCatalogProvider(CatalogProvider):
    def __init__(self, db_path: str = None):
        self.collection = db_path

    def get_table(self, owner: str, table: str) -> TableCatalogEntry:
        """
        Retrieve metadata for a specified table, including its schema and manifest references.

        Parameters:
            table_id (str): The identifier of the table.

        Returns:
            Dict[str, Any]: A dictionary containing the metadata of the table.
        """
        from firebase_admin import firestore

        _initialize()
        database = firestore.client()
        documents = database.collection(self.collection)

        documents = documents.where([("owner", "=", owner), ("name", "=", table)])
        documents = documents.stream()
        documents = list({**doc.to_dict(), "_id": doc.id} for doc in documents)
        if len(documents) > 1:
            raise AmbiguousTableError(owner=owner, table=table)
        if len(documents) == 1:
            return documents[0]
        return None

    def update_table(self, table_id: str, entry: TableCatalogEntry) -> None:
        """
        Update the metadata for a specified table.

        Parameters:
            table_id (str): The identifier of the table.
            metadata (Dict[str, Any]): A dictionary containing the metadata to be updated.
        """

        self.store.upsert("catalog", entry.as_dict(), {"table_id": table_id})
        pass

    def list_tables(self, owner: str) -> List[TableCatalogEntry]:
        """
        List all tables in the catalog along with their basic metadata.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each representing the metadata of a table.
        """
        from firebase_admin import firestore

        _initialize()
        database = firestore.client()
        documents = database.collection(self.collection)

        documents = documents.where([("owner", "=", owner)])
        documents = documents.stream()
        return list({**doc.to_dict(), "_id": doc.id} for doc in documents)

    def delete_table(self, table_id: str) -> None:
        """
        Delete metadata for a specified table.

        Parameters:
            table_id (str): The identifier of the table to be deleted.
        """
        self.store.delete("catalog", {"table_id": table_id})
