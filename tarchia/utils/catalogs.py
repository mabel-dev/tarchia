from typing import Optional

import orjson

from tarchia.exceptions import CommitNotFoundError
from tarchia.exceptions import OwnerNotFoundError
from tarchia.exceptions import TableNotFoundError
from tarchia.interfaces.catalog import catalog_factory
from tarchia.models import Commit
from tarchia.models import OwnerEntry
from tarchia.models import TableCatalogEntry

catalog_provider = catalog_factory()


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


def load_commit(storage_provider, commit_root, commit_sha) -> Optional[Commit]:
    if commit_sha:
        commit_file = storage_provider.read_blob(f"{commit_root}/commit-{commit_sha}.json")
        if commit_file is None:
            raise CommitNotFoundError(root=commit_root, commit=commit_sha)
        return Commit(**orjson.loads(commit_file))
    return None
