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
from uuid import uuid4

import orjson
from fastapi import APIRouter
from fastapi import HTTPException
from models import AddSnapshotRequest

router = APIRouter()


def generate_transaction_id():
    return str(uuid4())


def encode_and_sign_transaction(transaction_data):
    # Placeholder for actual encoding and cryptographic signing
    encoded = base64.b64encode(orjson.dumps(transaction_data).encode()).decode()
    signature = hashlib.sha256(transaction_data).hexdigest()
    # You would add a cryptographic signature here
    return encoded + "." + signature


def verify_and_decode_transaction(transaction_data):
    transaction, signature = transaction_data.split(".")

    decoded = base64.b64decode(transaction)
    recreated_signature = hashlib.sha256(decoded).hexdigest()
    if signature != recreated_signature:
        raise ValueError("Signature doesn't match")
    return decoded


def calculate_table_hash(data):
    # Assuming `data` is the serialized form of the table
    return hashlib.sha256(data).hexdigest()


def calculate_dataset_hash(table_hashes):
    # Simple XOR of hashes, could be replaced with more complex aggregation
    from binascii import unhexlify
    from functools import reduce
    from operator import xor

    hex_hashes = [unhexlify(h) for h in table_hashes]
    aggregated_hash = reduce(xor, hex_hashes)
    return hashlib.sha256(aggregated_hash).hexdigest()


@router.post("/transactions/start")
async def start_transaction(dataset: str, parent_snapshot: str):
    transaction_id = generate_transaction_id()
    transaction_data = {
        "transaction_id": transaction_id,
        "dataset": dataset,
        "parent_snapshot": parent_snapshot,
        "additions": [],
        "deletions": [],
    }
    encoded_data = encode_and_sign_transaction(transaction_data)
    return {
        "message": "Transaction started",
        "encoded_transaction": encoded_data,
        "transaction_id": transaction_id,
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


@router.post("/tables/{tableIdentifier}/files")
async def add_files_to_transaction(
    tableIdentifier: str, file_paths: List[str], encoded_transaction: str
):
    transaction_data = verify_and_decode_transaction(encoded_transaction)
    if not transaction_data or transaction_data["dataset"] != tableIdentifier:
        raise HTTPException(status_code=400, detail="Invalid transaction data")

    # Add file paths to the transaction's addition list
    transaction_data["additions"].extend(file_paths)

    # Reissue the updated transaction token
    new_encoded_transaction = encode_and_sign_transaction(transaction_data)
    return {"message": "Files added to transaction", "encoded_transaction": new_encoded_transaction}


@router.post("/tables/{tableIdentifier}/files/truncate")
async def truncate_all_files(tableIdentifier: str, file_paths: List[str], encoded_transaction: str):
    transaction_data = verify_and_decode_transaction(encoded_transaction)
    if not transaction_data or transaction_data["dataset"] != tableIdentifier:
        raise HTTPException(status_code=400, detail="Invalid transaction data")

    # Add file paths to the transaction's addition list
    transaction_data["additions"].extend(file_paths)

    # Reissue the updated transaction token
    new_encoded_transaction = encode_and_sign_transaction(transaction_data)
    return {"message": "Files added to transaction", "encoded_transaction": new_encoded_transaction}
