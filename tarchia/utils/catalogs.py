from tarchia.config import METADATA_ROOT
from tarchia.exceptions import OwnerNotFoundError
from tarchia.exceptions import TableNotFoundError
from tarchia.models import OwnerEntry
from tarchia.models import TableCatalogEntry
from tarchia.repositories.catalog import catalog_factory
from tarchia.storage import storage_factory

catalog_provider = catalog_factory()
storage_provider = storage_factory()


def identify_table(owner: str, table: str) -> TableCatalogEntry:
    """Get the catalog entry for a table name/identifier"""
    catalog_entry = catalog_provider.get_table(owner=owner, table=table)
    if catalog_entry is None:
        raise TableNotFoundError(owner=owner, table=table)
    return TableCatalogEntry(**catalog_entry)


def identify_owner(name: str) -> OwnerEntry:
    """Get the catalog entry for a table name/identifier"""
    catalog_entry = catalog_provider.get_owner(name=name)
    if catalog_entry is None:
        raise OwnerNotFoundError(owner=name)
    return OwnerEntry(**catalog_entry)
