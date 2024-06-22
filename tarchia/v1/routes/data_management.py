"""

Routes:
    [POST]      /v1/transactions/start
    [POST]      /v1/transactions/commit
    [DELETE]    /v1/tables/{tableIdentifier}/files
    [GET]       /v1/tables/{tableIdentifier}/files
    [POST]      /v1/tables/{tableIdentifier}/files
    [GET]       /v1/tables/{tableIdentifier}/snapshots
    [POST]      /v1/tables/{tableIdentifier}/snapshots
"""

import base64
import hashlib
from typing import List
from typing import Optional

import orjson
from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Path
from fastapi.responses import ORJSONResponse

from tarchia import config
from tarchia.config import METADATA_ROOT
from tarchia.repositories.catalog import catalog_factory
from tarchia.storage import storage_factory
from tarchia.utils import generate_uuid
from tarchia.utils.catalog import identify_table

SNAPSHOT_ROOT = f"{METADATA_ROOT}/[table_id]/snapshots/"
MANIFEST_ROOT = f"{METADATA_ROOT}/[table_id]/manifests/"


router = APIRouter()

catalog_provider = catalog_factory()
storage_provider = storage_factory()


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
async def start_transaction(tableIdentifier: str, snapshotIdentifier: str):
    catalog_entry = identify_table(tableIdentifier)
    table_id = catalog_entry.table_id

    print("TODO: check the snapshot exists")

    transaction_id = generate_uuid()
    transaction_data = {
        "transaction_id": transaction_id,
        "table_id": table_id,
        "parent_snapshot": snapshotIdentifier,
        "additions": [],
        "deletions": [],
        "truncate": False,
    }
    encoded_data = encode_and_sign_transaction(transaction_data)
    return {
        "message": "Transaction started",
        "encoded_transaction": encoded_data,
    }


@router.post("/transactions/commit")
async def commit_transaction(encoded_transaction: str):
    # transaction_data = verify_and_decode(encoded_transaction)
    # if not is_latest_snapshot(transaction_data["dataset"], transaction_data["parent_snapshot"]):
    #    return {"message": "Transaction failed: Snapshot out of date", "status": "error"}

    # apply_changes(transaction_data)
    return {
        "message": "Transaction committed successfully",
        #    "transaction_id": transaction_data["transaction_id"]
    }


@router.post("/tables/{tableIdentifier}/stage")
async def add_files_to_transaction(
    tableIdentifier: str, file_paths: List[str], encoded_transaction: str
):
    catalog_entry = identify_table(tableIdentifier)
    table_id = catalog_entry.get("table_id")

    transaction_data = verify_and_decode_transaction(encoded_transaction)
    if not transaction_data or transaction_data["table_id"] != table_id:
        raise HTTPException(status_code=400, detail="Invalid transaction data")

    # Add file paths to the transaction's addition list
    transaction_data["additions"].extend(file_paths)

    # Reissue the updated transaction token
    new_encoded_transaction = encode_and_sign_transaction(transaction_data)
    return {"message": "Files added to transaction", "encoded_transaction": new_encoded_transaction}


@router.post("/tables/{tableIdentifier}/truncate")
async def truncate_all_files(tableIdentifier: str, file_paths: List[str], encoded_transaction: str):
    catalog_entry = identify_table(tableIdentifier)
    table_id = catalog_entry.table_id

    transaction_data = verify_and_decode_transaction(encoded_transaction)
    if not transaction_data or transaction_data["table_id"] != table_id:
        raise HTTPException(status_code=400, detail="Invalid transaction data")

    transaction_data["truncate"] = True
    transaction_data["additions"] = []

    # Reissue the updated transaction token
    new_encoded_transaction = encode_and_sign_transaction(transaction_data)
    return {"message": "Files added to transaction", "encoded_transaction": new_encoded_transaction}


@router.post("/tables/{tableIdentifier}/push/{snapshotIdentifier}", response_class=ORJSONResponse)
async def promote_snaphow(
    tableIdentifier: str = Path(description="The unique identifier of the table."),
    snapshotIdentifier: Optional[str] = Path(
        description="The unique identifier of the snapshot to promote to the head."
    ),
):
    return False
