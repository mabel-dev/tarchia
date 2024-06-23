import inspect
from typing import Any
from typing import Dict
from typing import List

from tarchia.models import TableCatalogEntry


class CatalogProvider:
    """
    Base class for a catalog provider that handles interactions
    with a document store for managing table metadata, schemas, and references to manifests.
    """

    def get_table(self, table_id: str) -> Dict[str, Any]:
        """
        Retrieve metadata for a specified table, including its schema and manifest references.

        Parameters:
            table_id (str): The identifier of the table.

        Returns:
            Dict[str, Any]: A dictionary containing the metadata of the table.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name} is not implemented."
        )

    def update_table(self, table_id: str, entry: TableCatalogEntry) -> None:
        """
        Update the metadata for a specified table.

        Parameters:
            table_id (str): The identifier of the table.
            entry (Dict[str, Any]): A dictionary containing the metadata to be updated.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name} is not implemented."
        )

    def list_tables(self) -> List[Dict[str, Any]]:
        """
        List all tables in the catalog along with their basic metadata.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each representing the metadata of a table.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name} is not implemented."
        )

    def delete_table(self, table_id: str) -> None:
        """
        Delete entry for a specified table.

        Parameters:
            table_id (str): The identifier of the table to be deleted.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name} is not implemented."
        )
