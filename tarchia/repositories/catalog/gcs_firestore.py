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


class FirestoreCatalogProvider(CatalogProvider):
    def __init__(self, db_path: str = None):
        try:
            from google.cloud import firestore
        except ImportError as err:  # pragma: no cover
            raise MissingDependencyError(err.name) from err
        # if we've not been given the ID, fetch it
        self.project_id = GCP_PROJECT_ID
        if self.project_id is None:
            self.project_id = _get_project_id()

        self.collection = db_path

    def get_table(self, owner: str, table: str) -> TableCatalogEntry:
        """
        Retrieve metadata for a specified table, including its schema and manifest references.

        Parameters:
            table_id (str): The identifier of the table.

        Returns:
            Dict[str, Any]: A dictionary containing the metadata of the table.
        """
        from google.cloud import firestore
        from google.cloud.firestore_v1.base_query import FieldFilter

        database = firestore.Client(project=self.project_id)
        documents = database.collection(self.collection)

        documents = documents.where(filter=FieldFilter("owner", "==", owner)).where(
            filter=FieldFilter("name", "==", table)
        )
        documents = documents.stream()
        documents = list(doc.to_dict() for doc in documents)
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
        from google.cloud import firestore

        database = firestore.Client(project=self.project_id)
        database.collection(self.collection).document(entry.table_id).set(entry.as_dict())

    def list_tables(self, owner: str) -> List[TableCatalogEntry]:
        """
        List all tables in the catalog along with their basic metadata.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each representing the metadata of a table.
        """
        from google.cloud import firestore
        from google.cloud.firestore_v1.base_query import FieldFilter

        database = firestore.Client(project=self.project_id)
        documents = database.collection(self.collection)

        documents = documents.where(filter=FieldFilter("owner", "==", owner))
        documents = documents.stream()
        return list(doc.to_dict() for doc in documents)

    def delete_table(self, table_id: str) -> None:
        """
        Delete metadata for a specified table.

        Parameters:
            table_id (str): The identifier of the table to be deleted.
        """
        from google.cloud import firestore

        database = firestore.Client(project=self.project_id)
        database.collection(self.collection).document(table_id).delete()
