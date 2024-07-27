import sys
import os

os.environ["CATALOG_NAME"] = "test_catalog.json"
os.environ["TARCHIA_DEBUG"] = "TRUE"

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

from tarchia.api.v1.data_management import encode_and_sign_transaction
from tarchia.api.v1.data_management import verify_and_decode_transaction
from tarchia.exceptions import TransactionError
from tarchia.models import Transaction, Schema, Column


def test_transaction_signing_happy():
    payload = Transaction(transaction_id="1", expires_at=0, table_id="1", table="1", owner="1", table_schema=Schema(columns=[Column(name="test", type="VARCHAR")]), encryption=None)

    signed_transaction = encode_and_sign_transaction(payload)
    verified_transaction = verify_and_decode_transaction(signed_transaction)

    assert payload == verified_transaction


def test_transaction_signing_very_wrong_transactions():

    with pytest.raises(TransactionError):
        verify_and_decode_transaction("this isn't a valid transaction")

    with pytest.raises(TransactionError):
        verify_and_decode_transaction(None)

    signed_transaction = encode_and_sign_transaction(
        Transaction(transaction_id="1", expires_at=0, table_id="1", table="1", owner="1", table_schema=Schema(columns=[Column(name="test", type="VARCHAR")]), encryption=None)
    )
    # ensure the transaction is valid
    verify_and_decode_transaction(signed_transaction)

    # we're going to do various tamperings
    with pytest.raises(TransactionError):
        verify_and_decode_transaction(signed_transaction[:-1] + "0")
    with pytest.raises(TransactionError):
        verify_and_decode_transaction(str(reversed(signed_transaction)))


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
