"""
This is not intended for production use, however it is being used for
development, prototyping and for regression testing.
"""

from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from tarchia.exceptions import MissingDependencyError
from tarchia.exceptions import UnmetRequirementError
from tarchia.interfaces.catalog.provider_base import CatalogProvider
from tarchia.models import OwnerEntry
from tarchia.models import TableCatalogEntry
from tarchia.models import ViewCatalogEntry
from tarchia.utils import config

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
        self.database = firestore.Client(project=self.project_id)

    def get_table(self, owner: str, table: str) -> dict:
        """
        Retrieve metadata for a specified table, including its schema and manifest references.

        Parameters:
            table_id (str): The identifier of the table.

        Returns:
            Dict[str, Any]: A dictionary containing the metadata of the table.
        """
        from google.cloud.firestore_v1.base_query import FieldFilter

        documents = self.database.collection(self.collection)
        documents = (
            documents.where(filter=FieldFilter("relation", "==", "table"))
            .where(filter=FieldFilter("owner", "==", owner))
            .where(filter=FieldFilter("name", "==", table))
        )
        documents = documents.stream()
        documents = list(doc.to_dict() for doc in documents)
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
        self.database.collection(self.collection).document(f"table-{entry.table_id}").set(
            entry.as_dict()
        )

    def list_tables(self, owner: str) -> List[Dict[str, Any]]:
        """
        List all tables in the catalog along with their basic metadata.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each representing the metadata of a table.
        """
        from google.cloud.firestore_v1.base_query import FieldFilter

        documents = self.database.collection(self.collection)

        documents = documents.where(filter=FieldFilter("relation", "==", "table")).where(
            filter=FieldFilter("owner", "==", owner)
        )
        documents = documents.stream()
        return list(doc.to_dict() for doc in documents)

    def delete_table(self, table_id: str) -> None:
        """
        Delete metadata for a specified table.

        Parameters:
            table_id (str): The identifier of the table to be deleted.
        """
        self.database.collection(self.collection).document(f"table-{table_id}").delete()

    def get_owner(self, name: str) -> dict:
        from google.cloud.firestore_v1.base_query import FieldFilter

        documents = self.database.collection(self.collection)

        documents = documents.where(filter=FieldFilter("name", "==", name))
        documents = documents.stream()
        documents = list(doc.to_dict() for doc in documents)
        if len(documents) == 1:
            return documents[0]
        return None

    def update_owner(self, entry: OwnerEntry) -> None:
        self.database.collection(self.collection).document(f"owner-{entry.owner_id}").set(
            entry.as_dict()
        )

    def delete_owner(self, owner_id: str) -> None:
        self.database.collection(self.collection).delete(f"owner-{owner_id}")

    def list_views(self, owner: str) -> List[Dict[str, Any]]:
        """
        List all tables in the catalog along with their basic metadata.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each representing the metadata of a table.
        """
        from google.cloud.firestore_v1.base_query import FieldFilter

        documents = self.database.collection(self.collection)

        documents = documents.where(filter=FieldFilter("relation", "==", "view")).where(
            filter=FieldFilter("owner", "==", owner)
        )
        documents = documents.stream()
        return list(doc.to_dict() for doc in documents)

    def get_view(self, owner: str, view: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve metadata for a specified view, including its schema and manifest references.

        Parameters:
            table_id (str): The identifier of the table.

        Returns:
            Dict[str, Any]: A dictionary containing the metadata of the table.
        """
        from google.cloud.firestore_v1.base_query import FieldFilter

        documents = self.database.collection(self.collection)
        documents = (
            documents.where(filter=FieldFilter("relation", "==", "view"))
            .where(filter=FieldFilter("owner", "==", owner))
            .where(filter=FieldFilter("name", "==", view))
        )
        documents = documents.stream()
        documents = list(doc.to_dict() for doc in documents)
        if len(documents) == 1:
            return documents[0]
        return None

    def delete_view(self, view_id: str) -> None:
        """
        Delete metadata for a specified table.

        Parameters:
            view_id (str): The identifier of the table to be deleted.
        """
        self.database.collection(self.collection).document(f"view-{view_id}").delete()

    def update_view(self, view_id: str, entry: ViewCatalogEntry) -> None:
        """
        Update the metadata for a specified table.

        Parameters:
            table_id (str): The identifier of the table.
            metadata (Dict[str, Any]): A dictionary containing the metadata to be updated.
        """
        self.database.collection(self.collection).document(f"view-{entry.view_id}").set(
            entry.as_dict()
        )
