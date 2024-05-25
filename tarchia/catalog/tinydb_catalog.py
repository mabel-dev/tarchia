"""
TinyDB is not intended for production use, however it is being used for
development, prototyping and for regression testing.
"""

from typing import List
from typing import Union

from catalog.provider_base import CatalogProvider
from exceptions import MissingDependencyError
from models import StreamingMetadata
from models import TableMetadata


class TinyDBCatalogProvider(CatalogProvider):
    def __init__(self, db_path: str = "tarchia.json"):
        """
        Initializes the database connection to TinyDB.

        Parameters:
            db_path (str): The file path for the TinyDB database.
        """
        try:
            from tinydb import TinyDB
        except ImportError as err:  # pragma: no cover

            raise MissingDependencyError(err.name) from err

        if not db_path:
            db_path = "tarchia.db"

        self.db = TinyDB(db_path)
        self.table = self.db.table("catalog")

    def get_table_(self, table_id: str) -> Union[TableMetadata, StreamingMetadata]:
        """
        Retrieve metadata for a specified table, including its schema and manifest references.

        Parameters:
            table_id (str): The identifier of the table.

        Returns:
            Dict[str, Any]: A dictionary containing the metadata of the table.
        """
        from tinydb import Query

        Table = Query()
        result = self.table.search(Table.id == table_id)
        return result[0] if result else {}

    def update_table_metadata(
        self, table_id: str, metadata: Union[TableMetadata, StreamingMetadata]
    ) -> None:
        """
        Update the metadata for a specified table.

        Parameters:
            table_id (str): The identifier of the table.
            metadata (Dict[str, Any]): A dictionary containing the metadata to be updated.
        """
        from tinydb import Query

        Table = Query()
        self.table.upsert(metadata, Table.id == table_id)

    def list_tables(self) -> List[Union[StreamingMetadata, TableMetadata]]:
        """
        List all tables in the catalog along with their basic metadata.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each representing the metadata of a table.
        """
        return self.table.all()

    def delete_table_metadata(self, table_id: str) -> None:
        """
        Delete metadata for a specified table.

        Parameters:
            table_id (str): The identifier of the table to be deleted.
        """
        from tinydb import Query

        Table = Query()
        self.table.remove(Table.id == table_id)


# Example of how to use this class
if __name__ == "__main__":
    import random

    catalog = TinyDBCatalogProvider("test_db.json")
    table_id = f"table{random.randint(1,100)}"
    metadata = {
        "id": table_id,
        "name": "Sample Table",
        "schema": "schema_info",
        "manifest": "manifest_info",
    }

    # Add or update table metadata
    catalog.update_table_metadata(table_id, metadata)

    # Retrieve table metadata
    print(catalog.get_table_(table_id))

    # List all tables
    print(catalog.list_tables())

    # Delete table metadata
    # catalog.delete_table_metadata(table_id)
    print(catalog.list_tables())