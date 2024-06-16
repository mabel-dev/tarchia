import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import pytest

from tarchia.v1.routes.data_management import encode_and_sign_transaction
from tarchia.v1.routes.data_management import verify_and_decode_transaction


def test_transaction_signing_happy():
    payload = {"transaction": True}

    signed_transaction = encode_and_sign_transaction(payload)
    verified_transaction = verify_and_decode_transaction(signed_transaction)

    assert payload == verified_transaction


def test_transaction_signing_very_wrong_transactions():

    with pytest.raises(ValueError):
        verify_and_decode_transaction("this isn't a valid transaction")

    with pytest.raises(ValueError):
        verify_and_decode_transaction(None)

    signed_transaction = encode_and_sign_transaction({"key": "value"})
    # ensure the transaction is valid
    verify_and_decode_transaction(signed_transaction)

    # we're going to do various tamperings
    with pytest.raises(ValueError):
        verify_and_decode_transaction(signed_transaction[1:])
    with pytest.raises(ValueError):
        verify_and_decode_transaction(signed_transaction + ".0")


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
