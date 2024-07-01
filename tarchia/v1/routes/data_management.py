import base64
import hashlib
from typing import List
from typing import Optional

import orjson
from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Path
from fastapi import Query

from tarchia import config
from tarchia.constants import IDENTIFIER_REG_EX
from tarchia.constants import SNAPSHOT_ROOT

router = APIRouter()


def encode_and_sign_transaction(transaction_data: dict):
    # We late import to allow code to override this
    SIGNER = config.TRANSACTION_SIGNER
    if not isinstance(SIGNER, bytes):
        SIGNER = str(SIGNER).encode()
    # Placeholder for actual encoding and cryptographic signing
    transaction_bytes = orjson.dumps(transaction_data)
    encoded = base64.b64encode(transaction_bytes).decode()
    signature = hashlib.sha256(SIGNER + transaction_bytes).hexdigest()
    # You would add a cryptographic signature here
    return encoded + "." + signature


def verify_and_decode_transaction(transaction_data: str) -> dict:
    # We late import to allow code to override this
    SIGNER = config.TRANSACTION_SIGNER
    if not isinstance(SIGNER, bytes):
        SIGNER = str(SIGNER).encode()

    if not transaction_data:
        raise ValueError("No Transaction")

    transaction, signature = transaction_data.split(".")

    decoded = base64.b64decode(transaction)
    transaction = orjson.loads(decoded)
    recreated_signature = hashlib.sha256(SIGNER + decoded).hexdigest()
    if signature != recreated_signature:
        raise ValueError("Signature doesn't match")
    return transaction


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
            raise ValueError("Snapshot not found")

    transaction_id = generate_uuid()
    transaction_data = {
        "transaction_id": transaction_id,
        "table_id": table_id,
        "parent_snapshot": snapshot,
        "additions": [],
        "deletions": [],
        "truncate": False,
    }
    encoded_data = encode_and_sign_transaction(transaction_data)
    return {"message": "Transaction started", "encoded_transaction": encoded_data}


@router.post("/transactions/commit")
async def commit_transaction(
    encoded_transaction: str,
    force=Query(
        False,
        description="Force a transaction to complete, even if operating on a non-latest snapshot",
    ),
):
    from tarchia.utils import generate_uuid

    transaction_data = verify_and_decode_transaction(encoded_transaction)

    # if not force and not is_latest_snapshot(transaction_data["dataset"], transaction_data["parent_snapshot"]):
    #    return {"message": "Transaction failed: Snapshot out of date", "status": "error"}

    snaphot = generate_uuid()

    """
    write a new snapshot
    - write new manifest
    - write snaphot file
    - update catalog
    """

    return {
        "message": "Transaction committed successfully",
        "transaction": transaction_data["transaction_id"],
        "snapshot": snaphot,
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

    transaction_data = verify_and_decode_transaction(encoded_transaction)
    if not transaction_data or transaction_data["table_id"] != catalog_entry.table_id:
        raise HTTPException(status_code=400, detail="Invalid transaction data")

    # Add file paths to the transaction's addition list
    transaction_data["additions"].extend(file_paths)

    # Reissue the updated transaction token
    new_encoded_transaction = encode_and_sign_transaction(transaction_data)
    return {"message": "Files added to transaction", "encoded_transaction": new_encoded_transaction}


@router.post("/tables/{tableIdentifier}/truncate")
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
    table_id = catalog_entry.table_id

    transaction_data = verify_and_decode_transaction(encoded_transaction)
    if not transaction_data or transaction_data["table_id"] != table_id:
        raise HTTPException(status_code=400, detail="Invalid transaction data")

    transaction_data["truncate"] = True
    transaction_data["additions"] = []

    # Reissue the updated transaction token
    new_encoded_transaction = encode_and_sign_transaction(transaction_data)
    return {
        "message": "Table truncated in Transaction",
        "encoded_transaction": new_encoded_transaction,
    }
