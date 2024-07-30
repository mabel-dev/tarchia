"""
This is not intended for production use, however it is being used for
development, prototyping and for regression testing.
"""

from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from tarchia.interfaces.catalog.provider_base import CatalogProvider
from tarchia.models import OwnerEntry
from tarchia.models import TableCatalogEntry


class DevelopmentCatalogProvider(CatalogProvider):
    def __init__(self, db_path: str = None):
        """
        Initializes the database connection.

        Parameters:
            db_path (str): The file path for the database.
        """
        from tarchia.utils.doc_store import DocumentStore

        if not db_path:
            from tarchia.utils.config import CATALOG_NAME

            db_path = CATALOG_NAME

        self.db_path = db_path
        self.store = DocumentStore(self.db_path)

    def get_table(self, owner: str, table: str) -> Optional[dict]:
        """
        Retrieve metadata for a specified table, including its schema and manifest references.

        Parameters:
            table_id (str): The identifier of the table.

        Returns:
            Dict[str, Any]: A dictionary containing the metadata of the table.
        """

        result = self.store.find("tables", {"owner": owner, "name": table})
        return result[0] if result else None

    def update_table(self, table_id: str, entry: TableCatalogEntry) -> None:
        """
        Update the metadata for a specified table.

        Parameters:
            table_id (str): The identifier of the table.
            metadata (Dict[str, Any]): A dictionary containing the metadata to be updated.
        """

        self.store.upsert("tables", entry.as_dict(), {"table_id": table_id})

    def list_tables(self, owner: str) -> List[Dict[str, Any]]:
        """
        List all tables in the catalog along with their basic metadata.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each representing the metadata of a table.
        """
        result = self.store.find("tables", {"owner": owner})
        return result

    def delete_table(self, table_id: str) -> None:
        """
        Delete metadata for a specified table.

        Parameters:
            table_id (str): The identifier of the table to be deleted.
        """
        self.store.delete("tables", {"table_id": table_id})

    def get_owner(self, name: str) -> dict:
        result = self.store.find("owners", {"name": name})
        return result[0] if result else None

    def update_owner(self, entry: OwnerEntry) -> None:
        self.store.upsert("owners", entry.as_dict(), {"owner_id": entry.owner_id})

    def delete_owner(self, owner_id: str) -> None:
        self.store.delete("owners", {"owner_id": owner_id})

    def list_views(self, owner: str) -> List[Dict[str, Any]]:
        """
        List all views in the catalog along with their basic metadata.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each representing the metadata of a table.
        """
        result = self.store.find("views", {"owner": owner})
        return result

    def get_view(self, owner: str, view: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve metadata for a specified view.

        Parameters:
            table_id (str): The identifier of the table.

        Returns:
            Dict[str, Any]: A dictionary containing the metadata of the table.
        """

        result = self.store.find("views", {"owner": owner, "name": view})
        return result[0] if result else None

    def delete_view(self, view_id: str) -> None:
        """
        Delete metadata for a specified table.

        Parameters:
            table_id (str): The identifier of the table to be deleted.
        """
        self.store.delete("views", {"view_id": view_id})
