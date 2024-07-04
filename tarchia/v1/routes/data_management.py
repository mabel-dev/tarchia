import base64
import hashlib
import time
from typing import List
from typing import Optional

import orjson
from fastapi import APIRouter
from fastapi import Path
from fastapi import Query

from tarchia import config
from tarchia.constants import IDENTIFIER_REG_EX
from tarchia.constants import MANIFEST_ROOT
from tarchia.constants import SNAPSHOT_ROOT
from tarchia.exceptions import TransactionError
from tarchia.models import Snapshot
from tarchia.models import Transaction

router = APIRouter()


def encode_and_sign_transaction(transaction: Transaction):
    SIGNER = config.TRANSACTION_SIGNER
    if not isinstance(SIGNER, bytes):
        SIGNER = str(SIGNER).encode()

    transaction_bytes = transaction.serialize()
    encoded = base64.b64encode(transaction_bytes).decode()
    signature = hashlib.sha256(SIGNER + transaction_bytes).hexdigest()

    return encoded + "." + signature


def verify_and_decode_transaction(transaction_data: str) -> Transaction:
    # We late import to allow code to override this
    SIGNER = config.TRANSACTION_SIGNER
    if not isinstance(SIGNER, bytes):
        SIGNER = str(SIGNER).encode()

    if not transaction_data:
        raise TransactionError("No Transaction")
    if "." not in transaction_data:
        raise TransactionError("Transaction not formatted correctly.")
    transaction, signature = transaction_data.split(".")

    decoded = base64.b64decode(transaction)
    transaction = orjson.loads(decoded)
    recreated_signature = hashlib.sha256(SIGNER + decoded).hexdigest()
    if signature != recreated_signature:
        raise TransactionError("Transaction signature invalid")
    if int(transaction["expires_at"]) > time.time():
        raise TransactionError("Transaction Expired")
    return Transaction(**transaction)


def calculate_table_hash(data):
    # Assuming `data` is the serialized form of the table
    return hashlib.sha256(data).hexdigest()


def calculate_dataset_hash(table_hashes):
    from binascii import unhexlify
    from functools import reduce
    from operator import xor

    hex_hashes = [unhexlify(h) for h in table_hashes]
    aggregated_hash = reduce(xor, hex_hashes)
    return hashlib.sha256(aggregated_hash).hexdigest()


@router.post("/transactions/start")
async def start_transaction(owner: str, table: str, snapshot: Optional[str] = None):
    from tarchia.storage import storage_factory
    from tarchia.utils import build_root
    from tarchia.utils import generate_uuid
    from tarchia.utils.catalogs import identify_table

    catalog_entry = identify_table(owner=owner, table=table)
    table_id = catalog_entry.table_id

    if snapshot is None:
        snapshot = catalog_entry.current_snapshot_id
    else:
        snapshot_root = build_root(SNAPSHOT_ROOT, owner=owner, table_id=catalog_entry.table_id)
        storage_provider = storage_factory()
        snapshot_file = storage_provider.read_blob(f"{snapshot_root}/asat-{snapshot}.json")
        if not snapshot_file:
            raise TransactionError("Snapshot not found")

    transaction_id = generate_uuid()
    transaction = Transaction(
        transaction_id=transaction_id,
        expires_at=int(time.time()),
        table_id=table_id,
        table=table,
        owner=owner,
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
    from tarchia.catalog import catalog_factory
    from tarchia.manifests import build_manifest_entry
    from tarchia.manifests import get_manifest
    from tarchia.manifests import write_manifest
    from tarchia.storage import storage_factory
    from tarchia.utils import build_root
    from tarchia.utils import generate_uuid
    from tarchia.utils.catalogs import identify_table

    transaction = verify_and_decode_transaction(encoded_transaction)
    catalog_entry = identify_table(owner=transaction.owner, table=transaction.table)

    if not force and not catalog_entry.current_schema == transaction.parent_snapshot:
        raise TransactionError("Transaction failed: Snapshot out of date")

    snaphot_id = generate_uuid()
    owner = catalog_entry.owner
    table_id = catalog_entry.table_id

    snapshot_root = build_root(SNAPSHOT_ROOT, owner=owner, table_id=table_id)
    manifest_root = build_root(MANIFEST_ROOT, owner=owner, table_id=table_id)

    storage_provider = storage_factory()
    catalog_provider = catalog_factory()

    snapshot_id = str(int(time.time_ns() / 1e6))
    snapshot_path = f"{snapshot_root}/asat-{snaphot_id}.json"

    if transaction.parent_snapshot:
        snapshot_file = storage_provider.read_blob(
            f"{snapshot_root}/asat-{transaction.parent_snapshot}.json"
        )
        old_snapshot = orjson.loads(snapshot_file)
    else:
        old_snapshot = {}

    # build the new manifest first
    # simple single file manifest for now, this isn't size limited, we'll see how that goes
    if transaction.truncate or old_snapshot.get("manifest_path") is None:
        old_manifest = []
    else:
        old_manifest = get_manifest(old_snapshot.get("manifest_path"), storage_provider)

    # build the new snapshot
    new_snaphot = Snapshot(
        snapshot_id=snapshot_id,
        parent_snapshot_path=transaction.parent_snapshot,
        last_updated_ms=int(time.time_ns() / 1e6),
        manifest_path="",
        table_schema=catalog_entry.current_schema,
        encryption_details=catalog_entry.encryption_details,
    )

    # build the file list for the new manifest
    new_manifest = []
    for entry in old_manifest:
        if entry not in transaction.deletions:
            new_manifest.append(entry)
    for entry in transaction.additions:
        new_manifest.extend(build_manifest_entry(entry))

    manifest_id = generate_uuid()
    manifest_path = f"{manifest_root}/manifest-{manifest_id}.avro"
    write_manifest(location=manifest_path, storage_provider=storage_provider, entries=new_manifest)

    new_snaphot.manifest_path = manifest_path

    print(new_snaphot.as_dict())
    storage_provider.write_blob(snapshot_path, new_snaphot.serialize())

    catalog_provider.update_table(catalog_entry.table_id, catalog_entry)

    return {
        "message": "Transaction committed successfully",
        "transaction": transaction.transaction_id,
        "snapshot": snaphot_id,
        "notice": "this isn't actually completely written",
    }


@router.post("/tables/{owner}/{table}/stage")
async def add_files_to_transaction(
    file_paths: List[str],
    encoded_transaction: str,
    owner: str = Path(description="The owner of the table.", pattern=IDENTIFIER_REG_EX),
    table: str = Path(description="The name of the table.", pattern=IDENTIFIER_REG_EX),
):
    """
    Add files to a table.

    This operation can only be called as part of a transaction and does not make
    any changes to the table until the commit end-point is called.
    """
    from tarchia.utils.catalogs import identify_table

    catalog_entry = identify_table(owner=owner, table=table)

    transaction = verify_and_decode_transaction(encoded_transaction)
    if transaction.table_id != catalog_entry.table_id:
        raise TransactionError("Transaction operate on single tables")

    # Add file paths to the transaction's addition list
    transaction.additions.extend(file_paths)

    # Reissue the updated transaction token
    new_encoded_transaction = encode_and_sign_transaction(transaction)
    return {"message": "Files added to transaction", "encoded_transaction": new_encoded_transaction}


@router.post("/tables/{owner}/{table}/truncate")
async def truncate_all_files(
    encoded_transaction: str,
    owner: str = Path(description="The owner of the table.", pattern=IDENTIFIER_REG_EX),
    table: str = Path(description="The name of the table.", pattern=IDENTIFIER_REG_EX),
):
    """
    Truncate (delete all records) from a table.

    This operation can only be called as part of a transaction and does not make
    any changes to the table until the commit end-point is called.
    """
    from tarchia.utils.catalogs import identify_table

    catalog_entry = identify_table(owner=owner, table=table)

    transaction = verify_and_decode_transaction(encoded_transaction)
    if transaction.table_id != catalog_entry.table_id:
        raise TransactionError("Transaction operate on single tables")

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
