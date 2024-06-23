from typing import List
from typing import Optional
from typing import Tuple

import fastavro

from tarchia.manifests.models import EntryType
from tarchia.manifests.models import ManifestEntry
from tarchia.manifests.pruning import parse_filters
from tarchia.manifests.pruning import prune
from tarchia.storage import StorageProvider


def get_manifest(
    manifest: str,
    storage_provider: StorageProvider,
    filter_conditions: Optional[List[Tuple[str, str, str]]],
) -> List[str]:
    """
    Return the blobs from the manifests.

    Parameters:
        manifest: str
            The root manifest
        storage_provider: StorageProvider
            Inject the library to access storage
        filters: Optional List of Tuples (field, operation, value)
            Filters to apply to manifests, used for pruning blobs

    Returns:
        list of blob names

    Note:
        The filter does not filter individual records, it is used to eliminate blobs
        with no possible matching records. Blobs will still need to be filtered
        and blobs may not contain any matches.
    """
    manifest = []

    # get the manifest
    manifest_bytes = storage_provider.read_blob(manifest)
    manifest_complete = fastavro.reader(manifest_bytes)

    for entry in manifest_complete:
        manifest_entry = ManifestEntry(**entry)

        # filter the rows we don't want
        if filter_conditions and prune(manifest_entry, filter_conditions):
            continue

        # if the rows are manifests, call get_manifest
        if manifest_entry.file_type == EntryType.Manifest:
            manifest.extend(
                get_manifest(manifest_entry.file_path, storage_provider, filter_conditions)
            )
        else:
            manifest.append(manifest_entry.file_path)

    # return accumulated records
    return manifest
