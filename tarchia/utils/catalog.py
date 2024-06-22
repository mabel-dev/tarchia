from tarchia.catalog import catalog_factory
from tarchia.config import METADATA_ROOT
from tarchia.exceptions import TableNotFoundError
from tarchia.models import TableCatalogEntry
from tarchia.storage import storage_factory

SNAPSHOT_ROOT = f"{METADATA_ROOT}/[table_id]/snapshots/"
MANIFEST_ROOT = f"{METADATA_ROOT}/[table_id]/manifests/"

catalog_provider = catalog_factory()
storage_provider = storage_factory()


def identify_table(identifier: str) -> TableCatalogEntry:
    """Get the catalog entry for a table name/identifier"""
    catalog_entry = catalog_provider.get_table(identifier)
    if catalog_entry is None:
        raise TableNotFoundError(table=identifier)
    return TableCatalogEntry(**catalog_entry)
