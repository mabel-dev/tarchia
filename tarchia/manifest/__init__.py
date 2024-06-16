from typing import List
from typing import Optional
from typing import Tuple

import fastavro

from tarchia.manifest.models import EntryType
from tarchia.manifest.models import ManifestEntry
from tarchia.manifest.pruning import prune
from tarchia.storage import StorageProvider


def get_manifest(
    manifest: str, storage_provider: StorageProvider, filters: Optional[List[Tuple[str, str, str]]]
) -> List[str]:
    manifest = []

    # get the manifest
    manifest_bytes = storage_provider.read_blob(manifest)
    manifest_complete = fastavro.reader(manifest_bytes)

    for entry in manifest_complete:
        manifest_entry = ManifestEntry(**entry)

        # filter the rows we don't want
        if filters and prune(manifest_entry, filters):
            continue

        # if the rows are manifests, call get_manifest
        if manifest_entry.file_type == EntryType.Manifest:
            manifest.extend(get_manifest(manifest_entry.file_path, storage_provider, filters))
        else:
            manifest.append(manifest_entry.file_path)

    # return accumulated records
    return manifest
