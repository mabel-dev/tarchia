import base64
import hashlib
import time
from typing import List

import orjson
from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Query

from tarchia import config
from tarchia.constants import MANIFEST_ROOT
from tarchia.constants import SNAPSHOT_ROOT
from tarchia.exceptions import TransactionError
from tarchia.models import Snapshot
from tarchia.models import TableRequest
from tarchia.models import Transaction

router = APIRouter()


def encode_and_sign_transaction(transaction: Transaction) -> str:
    """
    Encode and sign a transaction.

    Parameters:
        transaction (Transaction): The transaction object to be encoded and signed.

    Returns:
        str: The encoded and signed transaction as a string.
    """
    SIGNER = config.TRANSACTION_SIGNER
    if not isinstance(SIGNER, bytes):
        SIGNER = str(SIGNER).encode()

    transaction_bytes = transaction.serialize()
    encoded = base64.b64encode(transaction_bytes).decode()
    signature = hashlib.sha256(SIGNER + transaction_bytes).hexdigest()

    return f"{encoded}.{signature}"


def verify_and_decode_transaction(transaction_data: str) -> Transaction:
    """
    Verify and decode a transaction.

    Parameters:
        transaction_data (str): The encoded and signed transaction data as a string.

    Returns:
        Transaction: The decoded transaction object.

    Raises:
        TransactionError: If the transaction is invalid or expired.
    """
    SIGNER = config.TRANSACTION_SIGNER
    if not isinstance(SIGNER, bytes):
        SIGNER = str(SIGNER).encode()

    if not transaction_data:
        raise TransactionError("No Transaction.")
    if "." not in transaction_data:
        raise TransactionError("Transaction incorrectly formatted.")

    transaction, signature = transaction_data.split(".")

    decoded = base64.b64decode(transaction)
    transaction = orjson.loads(decoded)

    if int(transaction["expires_at"]) > time.time():
        raise TransactionError("Transaction Expired")

    recreated_signature = hashlib.sha256(SIGNER + decoded).hexdigest()
    if signature != recreated_signature:
        raise TransactionError("Transaction signature invalid.")

    return Transaction(**transaction)


def load_old_snapshot(storage_provider, snapshot_root, parent_snapshot):
    if parent_snapshot:
        snapshot_file = storage_provider.read_blob(f"{snapshot_root}/asat-{parent_snapshot}.json")
        return orjson.loads(snapshot_file)
    return {}


def build_new_manifest(old_manifest, transaction, storage_provider):
    from tarchia.manifests import build_manifest_entry

    new_manifest = [entry for entry in old_manifest if entry not in transaction.deletions]
    new_manifest.extend(
        build_manifest_entry(entry, storage_provider) for entry in transaction.additions
    )
    return new_manifest


def create_new_snapshot(snapshot_id, transaction, catalog_entry, manifest_path, timestamp):
    return Snapshot(
        snapshot_id=snapshot_id,
        parent_snapshot_path=transaction.parent_snapshot,
        last_updated_ms=timestamp,
        manifest_path=manifest_path,
        table_schema=catalog_entry.current_schema,
        encryption_details=catalog_entry.encryption_details,
    )


@router.post("/transactions/start")
async def start_transaction(table: TableRequest):
    from tarchia.storage import storage_factory
    from tarchia.utils import build_root
    from tarchia.utils import generate_uuid
    from tarchia.utils.catalogs import identify_table

    catalog_entry = identify_table(owner=table.owner, table=table.table)
    table_id = catalog_entry.table_id

    if snapshot is None:
        snapshot = catalog_entry.current_snapshot_id
    else:
        snapshot_root = build_root(
            SNAPSHOT_ROOT, owner=table.owner, table_id=catalog_entry.table_id
        )
        storage_provider = storage_factory()
        snapshot_file = storage_provider.read_blob(f"{snapshot_root}/asat-{snapshot}.json")
        if not snapshot_file:
            raise TransactionError("Snapshot not found")

    transaction_id = generate_uuid()
    transaction = Transaction(
        transaction_id=transaction_id,
        expires_at=int(time.time()),
        table_id=table_id,
        table=table.table,
        owner=table.owner,
        parent_snapshot=snapshot,
        additions=[],
        deletions=[],
        truncate=False,
    )
    encoded_data = encode_and_sign_transaction(transaction)
    return {"message": "Transaction started", "encoded_transaction": encoded_data}


@router.post("/transactions/commit")
async def commit_transaction(
    encoded_transaction: str,
    force=Query(
        False,
        description="Force a transaction to complete, even if operating on a non-latest snapshot",
    ),
):
    """
    Commits a transaction by verifying it, updating the manifest and snapshot,
    and updating the catalog.

    Parameters:
        encoded_transaction (str): Encoded transaction string.
        force (bool): Force transaction to complete even if the snapshot is not the latest.

    Returns:
        dict: Result of the transaction commit.
    """
    from tarchia.catalog import catalog_factory
    from tarchia.manifests import get_manifest
    from tarchia.manifests import write_manifest
    from tarchia.storage import storage_factory
    from tarchia.utils import build_root
    from tarchia.utils import generate_uuid
    from tarchia.utils.catalogs import identify_table

    try:
        transaction = verify_and_decode_transaction(encoded_transaction)
        catalog_entry = identify_table(owner=transaction.owner, table=transaction.table)

        if (
            not force
            and transaction.parent_snapshot
            and catalog_entry.current_snapshot_id != transaction.parent_snapshot
        ):
            raise TransactionError("Transaction failed: Snapshot out of date")

        owner = catalog_entry.owner
        table_id = catalog_entry.table_id

        snapshot_root = build_root(SNAPSHOT_ROOT, owner=owner, table_id=table_id)
        manifest_root = build_root(MANIFEST_ROOT, owner=owner, table_id=table_id)

        storage_provider = storage_factory()
        catalog_provider = catalog_factory()

        timestamp = int(time.time_ns() / 1e6)
        snapshot_id = str(timestamp)
        snapshot_path = f"{snapshot_root}/asat-{snapshot_id}.json"

        old_snapshot = load_old_snapshot(
            storage_provider, snapshot_root, transaction.parent_snapshot
        )
        old_manifest = (
            get_manifest(old_snapshot.get("manifest_path"), storage_provider)
            if old_snapshot.get("manifest_path")
            else []
        )

        new_manifest = build_new_manifest(old_manifest, transaction, storage_provider)
        manifest_id = generate_uuid()
        manifest_path = f"{manifest_root}/manifest-{manifest_id}.avro"
        write_manifest(
            location=manifest_path, storage_provider=storage_provider, entries=new_manifest
        )

        new_snapshot = create_new_snapshot(
            snapshot_id, transaction, catalog_entry, manifest_path, timestamp
        )
        storage_provider.write_blob(snapshot_path, new_snapshot.serialize())

        catalog_entry.last_updated_ms = timestamp
        catalog_entry.current_snapshot_id = snapshot_id
        catalog_provider.update_table(catalog_entry.table_id, catalog_entry)

        return {
            "message": "Transaction committed successfully",
            "transaction": transaction.transaction_id,
            "snapshot": snapshot_id,
        }
    except TransactionError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal Server Error") from e


@router.post("/transactions/stage")
async def add_files_to_transaction(file_paths: List[str], encoded_transaction: str):
    """
    Add files to a table.

    This operation can only be called as part of a transaction and does not make
    any changes to the table until the commit end-point is called.
    """
    transaction = verify_and_decode_transaction(encoded_transaction)

    # Add file paths to the transaction's addition list
    transaction.additions.extend(file_paths)

    # Reissue the updated transaction token
    new_encoded_transaction = encode_and_sign_transaction(transaction)
    return {"message": "Files added to transaction", "encoded_transaction": new_encoded_transaction}


@router.post("/transactions/truncate")
async def truncate_all_files(encoded_transaction: str):
    """
    Truncate (delete all records) from a table.

    This operation can only be called as part of a transaction and does not make
    any changes to the table until the commit end-point is called.
    """
    transaction = verify_and_decode_transaction(encoded_transaction)

    if len(transaction.additions) != 0:
        raise TransactionError("Use 'truncate' before staging files in transaction.")

    # truncate everything
    transaction.truncate = True
    transaction.additions = []
    transaction.deletions = []

    # Reissue the updated transaction token
    new_encoded_transaction = encode_and_sign_transaction(transaction)
    return {
        "message": "Table truncated in Transaction",
        "encoded_transaction": new_encoded_transaction,
    }
