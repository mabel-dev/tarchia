"""
This is not intended for production use, however it is being used for
development, prototyping and for regression testing.
"""

from typing import List

from tarchia.exceptions import AmbiguousTableError
from tarchia.models import TableCatalogEntry
from tarchia.repositories.catalog.provider_base import CatalogProvider


class DevelopmentCatalogProvider(CatalogProvider):
    def __init__(self, db_path: str = None):
        """
        Initializes the database connection.

        Parameters:
            db_path (str): The file path for the database.
        """
        from tarchia.utils.doc_store import DocumentStore

        if not db_path:
            from tarchia.config import CATALOG_NAME

            db_path = CATALOG_NAME

        self.db_path = db_path
        self.store = DocumentStore(self.db_path)

    def get_table(self, owner: str, table: str) -> TableCatalogEntry:
        """
        Retrieve metadata for a specified table, including its schema and manifest references.

        Parameters:
            table_id (str): The identifier of the table.

        Returns:
            Dict[str, Any]: A dictionary containing the metadata of the table.
        """

        result = self.store.find("catalog", {"owner": owner, "name": table})
        if len(result) > 1:
            raise AmbiguousTableError(owner=owner, table=table)
        return result[0] if result else None

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
        result = self.store.find("catalog", {"owner": owner})
        return result

    def delete_table(self, table_id: str) -> None:
        """
        Delete metadata for a specified table.

        Parameters:
            table_id (str): The identifier of the table to be deleted.
        """
        self.store.delete("catalog", {"table_id": table_id})
