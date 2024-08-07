import inspect
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from tarchia.models import OwnerEntry
from tarchia.models import TableCatalogEntry
from tarchia.models import ViewCatalogEntry


class CatalogProvider:  # pragma: no cover
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

    def list_tables(self, owner: str) -> List[Dict[str, Any]]:
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

    def get_owner(self, name: str) -> OwnerEntry:
        raise NotImplementedError(
            f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name} is not implemented."
        )

    def update_owner(self, entry: OwnerEntry) -> None:
        raise NotImplementedError(
            f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name} is not implemented."
        )

    def delete_owner(self, entry: OwnerEntry) -> None:
        raise NotImplementedError(
            f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name} is not implemented."
        )

    def list_views(self, owner: str) -> List[Dict[str, Any]]:
        """
        List all views in the catalog along with their basic metadata.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each representing the metadata of a table.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name} is not implemented."
        )

    def get_view(self, owner: str, view: str) -> Optional[Dict[str, Any]]:
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

    def delete_view(self, view_id: str) -> None:
        """
        Delete entry for a specified table.

        Parameters:
            table_id (str): The identifier of the table to be deleted.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name} is not implemented."
        )

    def update_view(self, view_id: str, entry: ViewCatalogEntry) -> None:
        """
        Update the metadata for a specified table.

        Parameters:
            table_id (str): The identifier of the table.
            entry (Dict[str, Any]): A dictionary containing the metadata to be updated.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name} is not implemented."
        )
