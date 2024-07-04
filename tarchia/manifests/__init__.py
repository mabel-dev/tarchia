from typing import List
from typing import Optional
from typing import Tuple

import fastavro

from tarchia.manifests.models import EntryType
from tarchia.manifests.models import ManifestEntry
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


def write_manifest(location: str, storage_provider: StorageProvider, entries: List[ManifestEntry]):
    from io import BytesIO

    from .models import MANIFEST_SCHEMA

    stream = BytesIO()

    fastavro.writer(
        stream,
        schema=MANIFEST_SCHEMA,
        records=[e.as_dict() for e in entries],
        codec="zstandard",
    )

    stream.seek(0)
    storage_provider.write_blob(location, stream.read())


def build_manifest_entry(path: str, storage_provider: StorageProvider) -> ManifestEntry:
    """
    Build a manifest entry for a given Parquet file.

    Parameters:
        path (str): The file path of the Parquet file.
        storage_provider (StorageProvider): An instance of StorageProvider to read the file.

    Returns:
        ManifestEntry: The constructed manifest entry with file details and column statistics.
    """
    from io import BytesIO

    from pyarrow import parquet

    from tarchia.utils.to_int import to_int

    new_manifest_entry = ManifestEntry(
        file_path=path, file_format="parquet", file_type=EntryType.Data
    )

    # Read the file bytes and initialize the Parquet file object
    file_bytes = storage_provider.read_blob(path)
    new_manifest_entry.file_size = len(file_bytes)
    stream = BytesIO(file_bytes)
    parquet_file = parquet.ParquetFile(stream)
    new_manifest_entry.record_count = parquet_file.metadata.num_rows

    # Initialize statistics for each column
    for column in parquet_file.schema_arrow.names:
        new_manifest_entry.null_value_counts[column] = 0

        # Iterate over each row group to gather statistics
        for row_group_index in range(parquet_file.metadata.num_row_groups):
            column_index = parquet_file.schema_arrow.get_field_index(column)
            column_chunk = parquet_file.metadata.row_group(row_group_index).column(column_index)

            if column_chunk.statistics is not None:
                # Update null value counts
                if column_chunk.statistics.has_null_count:
                    new_manifest_entry.null_value_counts[column] += (
                        column_chunk.statistics.null_count
                    )

                # Update lower bounds
                min_value = to_int(column_chunk.statistics.min)
                if min_value:
                    if column not in new_manifest_entry.lower_bounds:
                        new_manifest_entry.lower_bounds[column] = min_value
                    else:
                        new_manifest_entry.lower_bounds[column] = min(
                            new_manifest_entry.lower_bounds[column], min_value
                        )

                # Update upper bounds
                max_value = to_int(column_chunk.statistics.max)
                if max_value:
                    if column not in new_manifest_entry.upper_bounds:
                        new_manifest_entry.upper_bounds[column] = max_value
                    else:
                        new_manifest_entry.upper_bounds[column] = max(
                            new_manifest_entry.upper_bounds[column], max_value
                        )

    return new_manifest_entry
