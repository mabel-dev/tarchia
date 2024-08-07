"""
Transaction control

@router.post("/transactions/start")     -> [GET]    /tables/{owner}/{table}/commits/{commit}/pull/start
@router.post("/transactions/commit")    -> [POST]   /pull/commit
@router.post("/transactions/stage")     -> [POST]   /pull/stage
@router.post("/transactions/truncate")  -> [POST]   /pull/truncate
@router.patch("/transaction/encryption")-> [PATCH]  /pull/encryption
                                        -> [POST]   /pull/abort
"""

import base64
import hashlib
import time
from typing import List
from typing import Literal
from typing import Union

import orjson
from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Path
from fastapi import Request

from tarchia.exceptions import TransactionError
from tarchia.models import Commit
from tarchia.models import CommitRequest
from tarchia.models import StageFilesRequest
from tarchia.models import Transaction
from tarchia.models import TransactionRequest
from tarchia.utils import config
from tarchia.utils import get_base_url
from tarchia.utils.catalogs import load_commit
from tarchia.utils.constants import COMMITS_ROOT
from tarchia.utils.constants import HISTORY_ROOT
from tarchia.utils.constants import IDENTIFIER_REG_EX
from tarchia.utils.constants import MAIN_BRANCH
from tarchia.utils.constants import MANIFEST_ROOT
from tarchia.utils.constants import SHA_OR_HEAD_REG_EX

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


def build_new_manifest(old_manifest, transaction, schema):
    from tarchia.metadata.manifests import build_manifest_entry

    existing_entries = {e.file_path for e in old_manifest}
    new_entries = [
        e
        for e in transaction.additions
        if e not in set(transaction.deletions).union(existing_entries)
    ]

    new_manifest = [entry for entry in old_manifest if entry not in transaction.deletions]
    new_manifest.extend(build_manifest_entry(entry, schema) for entry in new_entries)

    return new_manifest


def xor_hex_strings(hex_strings: List[str]) -> str:
    """
    XOR a list of hexadecimal strings and return the result as a hexadecimal string.

    Parameters:
        hex_strings: List[str]
            The list of hexadecimal strings to XOR.

    Returns:
        str
            The resulting hexadecimal string after XOR.
    """
    if not hex_strings:
        return "0" * 64  # Return a 64-character string of zeros if the list is empty

    result_bytes = bytes.fromhex(hex_strings[0])
    for hex_str in hex_strings[1:]:
        result_bytes = bytes(a ^ b for a, b in zip(result_bytes, bytes.fromhex(hex_str)))

    return result_bytes.hex()


@router.post("/tables/{owner}/{table}/commits/{commit_sha}/pull/start")
async def start_transaction(
    owner: str = Path(description="The owner of the table.", pattern=IDENTIFIER_REG_EX),
    table: str = Path(description="The name of the table.", pattern=IDENTIFIER_REG_EX),
    commit_sha: Union[str, Literal["head"]] = Path(
        description="The commit to retrieve.", pattern=SHA_OR_HEAD_REG_EX
    ),
):
    from tarchia.interfaces.storage import storage_factory
    from tarchia.utils import build_root
    from tarchia.utils import generate_uuid
    from tarchia.utils.catalogs import identify_table

    catalog_entry = identify_table(owner=owner, table=table)
    table_id = catalog_entry.table_id

    if commit_sha == "head":
        commit_sha = catalog_entry.current_commit_sha

    commit_root = build_root(COMMITS_ROOT, owner=owner, table_id=catalog_entry.table_id)
    storage_provider = storage_factory()
    parent_commit = load_commit(storage_provider, commit_root, commit_sha)

    if parent_commit is None:
        raise TransactionError("Commit not found")

    transaction_id = generate_uuid()
    transaction = Transaction(
        transaction_id=transaction_id,
        expires_at=int(time.time()),
        table_id=table_id,
        table=table,
        owner=owner,
        parent_commit_sha=commit_sha,
        additions=[],
        deletions=[],
        truncate=False,
        encryption=parent_commit.encryption,
        table_schema=parent_commit.table_schema,
    )
    encoded_data = encode_and_sign_transaction(transaction)
    return {"message": "Transaction started", "encoded_transaction": encoded_data}


@router.post("/pull/commit")
async def commit_transaction(request: Request, commit_request: CommitRequest):
    """
    Commits a transaction by verifying it, updating the manifest and commit,
    and updating the catalog.

    Parameters:
        encoded_transaction (str): Encoded transaction string.
        force (bool): Force transaction to complete even if the commit is not the latest.

    Returns:
        dict: Result of the transaction commit.
    """

    raise NotImplementedError("read notes")
    """
    get all of the commits between HEAD and the parent, if any include deletes
    then fail this commit, if they're all additive then we can continue

    make sure we change the parent commit to the HEAD commit
    """

    from tarchia.interfaces.catalog import catalog_factory
    from tarchia.interfaces.storage import storage_factory
    from tarchia.metadata.history import HistoryTree
    from tarchia.metadata.manifests import get_manifest
    from tarchia.metadata.manifests import write_manifest
    from tarchia.utils import build_root
    from tarchia.utils import generate_uuid
    from tarchia.utils.catalogs import identify_table

    base_url = get_base_url(request)
    timestamp = int(time.time_ns() / 1e6)
    uuid = generate_uuid()

    try:
        transaction = verify_and_decode_transaction(commit_request.encoded_transaction)
        catalog_entry = identify_table(owner=transaction.owner, table=transaction.table)

        if (
            transaction.parent_commit_sha
            and catalog_entry.current_commit_sha != transaction.parent_commit_sha
        ):
            raise TransactionError("Transaction failed: Commit out of date")

        owner = catalog_entry.owner
        table_id = catalog_entry.table_id

        commit_root = build_root(COMMITS_ROOT, owner=owner, table_id=table_id)
        manifest_root = build_root(MANIFEST_ROOT, owner=owner, table_id=table_id)
        history_root = build_root(HISTORY_ROOT, owner=owner, table_id=table_id)

        storage_provider = storage_factory()
        catalog_provider = catalog_factory()

        # get the commit we're based on
        old_commit = load_commit(storage_provider, commit_root, transaction.parent_commit_sha)
        old_manifest = (
            get_manifest(old_commit.manifest_path, storage_provider, None)
            if old_commit.manifest_path
            else []
        )

        new_manifest = build_new_manifest(old_manifest, transaction, transaction.table_schema)
        manifest_path = f"{manifest_root}/manifest-{uuid}.avro"
        write_manifest(
            location=manifest_path, storage_provider=storage_provider, entries=new_manifest
        )

        # hash the manifests together
        combined_hash = xor_hex_strings([e.sha256_checksum for e in new_manifest])

        # build the new commit record
        commit = Commit(
            data_hash=combined_hash,
            user="user",
            message=commit_request.commit_message,
            branch=MAIN_BRANCH,
            parent_commit_sha=transaction.parent_commit_sha,
            last_updated_ms=timestamp,
            manifest_path=manifest_path,
            table_schema=transaction.table_schema,
            encryption=transaction.encryption,
            added_files=transaction.additions,
            removed_files=transaction.deletions,
        )

        commit_path = f"{commit_root}/commit-{commit.commit_sha}.json"
        storage_provider.write_blob(commit_path, commit.serialize())

        if catalog_entry.current_history:
            history_file = f"{history_root}/history-{catalog_entry.current_history}.avro"
            history_raw = storage_provider.read_blob(history_file)
            if history_raw:
                history = HistoryTree.load_from_avro(history_raw)
            else:
                history = HistoryTree(MAIN_BRANCH)
        else:
            history = HistoryTree(MAIN_BRANCH)

        history.commit(commit.history_entry)
        history_raw = history.save_to_avro()
        history_file = f"{history_root}/history-{uuid}.avro"
        storage_provider.write_blob(history_file, history_raw)

        catalog_entry.last_updated_ms = timestamp
        catalog_entry.current_commit_sha = commit.commit_sha
        catalog_entry.current_history = uuid
        catalog_provider.update_table(catalog_entry.table_id, catalog_entry)

        # trigger webhooks - this should be async so we don't wait for the outcome
        catalog_entry.trigger_event(
            catalog_entry.EventTypes.NEW_COMMIT,
            {
                "event": "NEW_COMMIT",
                "table": f"{catalog_entry.owner}.{catalog_entry.name}",
                "commit": commit.commit_sha,
                "url": f"{base_url}/v1/tables/{catalog_entry.owner}/{catalog_entry.name}/commits/{commit.commit_sha}",
            },
        )

        return {
            "table": f"{catalog_entry.owner}.{catalog_entry.name}",
            "message": "Transaction committed successfully",
            "transaction": transaction.transaction_id,
            "commit": commit.commit_sha,
            "url": f"{base_url}/v1/tables/{catalog_entry.owner}/{catalog_entry.name}/commits/{commit.commit_sha}",
        }
    except TransactionError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/pull/stage")
async def add_files_to_transaction(stage: StageFilesRequest):
    """
    Add files to a table.

    This operation can only be called as part of a transaction and does not make
    any changes to the table until the commit end-point is called.
    """
    transaction = verify_and_decode_transaction(stage.encoded_transaction)

    # Add file paths to the transaction's addition list
    transaction.additions.extend(stage.paths)

    # Reissue the updated transaction token
    new_encoded_transaction = encode_and_sign_transaction(transaction)
    return {"message": "Files added to transaction", "encoded_transaction": new_encoded_transaction}


@router.post("/pull/truncate")
async def truncate_all_files(tran: TransactionRequest):
    """
    Truncate (delete all records) from a table.

    This operation can only be called as part of a transaction and does not make
    any changes to the table until the commit end-point is called.
    """
    transaction = verify_and_decode_transaction(tran.encoded_transaction)

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


@router.patch("/pull/encryption")
async def update_encryption(tran: TransactionRequest):
    raise NotImplementedError("Create a commit")


@router.patch("/pull/abort")
async def abort_pull(tran: TransactionRequest):
    """doesn't do anything, exists for conceptual completeness"""
    return {"message": "Transaction Aborted"}
