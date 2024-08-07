from typing import List
from typing import Optional
from typing import Tuple

from tarchia.exceptions import DataError
from tarchia.interfaces.storage import StorageProvider
from tarchia.interfaces.storage import storage_factory
from tarchia.metadata.manifests.pruning import prune
from tarchia.models import Column
from tarchia.models import Schema
from tarchia.models.manifest_models import EntryType
from tarchia.models.manifest_models import ManifestEntry


def get_manifest(
    location: Optional[str],
    storage_provider: StorageProvider,
    filter_conditions: Optional[List[Tuple[str, str, int]]],
) -> List[ManifestEntry]:
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
    from io import BytesIO

    import fastavro

    manifest = []

    if location is None:
        return manifest

    # get the manifest
    manifest_bytes = storage_provider.read_blob(location)
    manifest_complete = fastavro.reader(BytesIO(manifest_bytes))

    for entry in manifest_complete:
        manifest_entry = ManifestEntry(**entry)

        # filter the rows we don't want
        if filter_conditions and prune(manifest_entry, filter_conditions):
            continue

        # if the rows are manifests, call get_manifest
        if manifest_entry.file_type == EntryType.Manifest:
            manifest.extend(get_manifest(manifest_entry, storage_provider, filter_conditions))
        else:
            manifest.append(manifest_entry)

    # return accumulated records
    return manifest


def write_manifest(location: str, storage_provider: StorageProvider, entries: List[ManifestEntry]):
    from io import BytesIO

    import fastavro

    from tarchia.models.manifest_models import MANIFEST_SCHEMA

    stream = BytesIO()

    fastavro.writer(
        stream,
        schema=MANIFEST_SCHEMA,
        records=[e.as_dict() for e in entries],
        codec="zstandard",
    )

    stream.seek(0)
    storage_provider.write_blob(location, stream.read())


def build_manifest_entry(path: str, expected_schema: Schema) -> ManifestEntry:
    """
    Build a manifest entry for a given Parquet file.

    Parameters:
        path (str): The file path of the Parquet file.

    Returns:
        ManifestEntry: The constructed manifest entry with file details and column statistics.
    """
    from hashlib import sha256
    from io import BytesIO

    from pyarrow import parquet

    from tarchia.utils.to_int import to_int

    if "://" in path:
        host, blob_path = path.split("://")
        storage_provider = storage_factory(host)
    else:
        blob_path = path
        storage_provider = storage_factory("LOCAL")

    new_manifest_entry = ManifestEntry(
        file_path=path, file_format="parquet", file_type=EntryType.Data
    )

    # Read the file bytes and initialize the Parquet file object
    file_bytes = storage_provider.read_blob(blob_path, bucket_in_path=True)
    if file_bytes is None:
        from tarchia.exceptions import UnableToReadBlobError

        raise UnableToReadBlobError(f"Unable to read {blob_path}.")

    new_manifest_entry.file_size = len(file_bytes)
    new_manifest_entry.sha256_checksum = sha256(file_bytes).hexdigest()

    stream = BytesIO(file_bytes)
    parquet_file = parquet.ParquetFile(stream)
    new_manifest_entry.record_count = parquet_file.metadata.num_rows

    parquet_columns_names = set(parquet_file.schema.names)
    for column in expected_schema.columns:
        all_names = [column.name] + column.aliases
        if column.default is None and not parquet_columns_names.intersection(all_names):
            raise DataError(
                f"File '{path}' is missing column '{column.name}'. To avoid this error, ensure this column has a default value or is present in all files."
            )

    # Initialize statistics for each column
    for column in parquet_file.schema_arrow.names:
        # Iterate over each row group to gather statistics
        for row_group_index in range(parquet_file.metadata.num_row_groups):
            column_index = parquet_file.schema_arrow.get_field_index(column)
            column_chunk = parquet_file.metadata.row_group(row_group_index).column(column_index)

            if column_chunk.statistics is not None:
                # Update lower bounds
                min_value = to_int(column_chunk.statistics.min)
                if min_value is not None:
                    if column not in new_manifest_entry.lower_bounds:
                        new_manifest_entry.lower_bounds[column] = min_value
                    else:
                        new_manifest_entry.lower_bounds[column] = min(
                            new_manifest_entry.lower_bounds[column], min_value
                        )

                # Update upper bounds
                max_value = to_int(column_chunk.statistics.max)
                if max_value is not None:
                    if column not in new_manifest_entry.upper_bounds:
                        new_manifest_entry.upper_bounds[column] = max_value
                    else:
                        new_manifest_entry.upper_bounds[column] = max(
                            new_manifest_entry.upper_bounds[column], max_value
                        )

    return new_manifest_entry
