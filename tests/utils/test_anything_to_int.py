import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from tarchia.utils.to_int import to_int

import datetime
from decimal import Decimal
import random


# Tests
def test_to_int():
    # Test int
    assert isinstance(to_int(42), int)

    # Test float
    assert isinstance(to_int(3.14), int)

    # Test date
    assert isinstance(to_int(datetime.date(2020, 1, 1)), int)

    # Test datetime
    assert isinstance(to_int(datetime.datetime(2020, 1, 1, 12, 0)), int)

    # Test time
    assert isinstance(to_int(datetime.time(12, 0, 0)), int)

    # Test Decimal
    assert isinstance(to_int(Decimal("3.14")), int)

    # Test string
    assert isinstance(to_int("hello"), int)

    # Test bytes
    assert isinstance(to_int(b"hello"), int)

    # Test unsupported type (list)
    assert to_int([1, 2, 3]) is None

    # Test unsupported type (dict)
    assert to_int({"key": "value"}) is None


def test_string_order_preservation():
    strings = ["Apple", "banana", "CHERRY", "date", "Elderberry", "FIG", "grape"]
    strings = random.sample(strings, len(strings))
    sorted_strings = sorted(strings)
    int_values = [to_int(s) for s in sorted_strings]
    # Ensure the list of int values is sorted
    assert int_values == sorted(int_values)


def test_string_order_preservation_with_bytes():
    byte_strings = [b"Apple", b"banana", b"CHERRY", b"date", b"Elderberry", b"FIG", b"grape"]
    byte_strings = random.sample(byte_strings, len(byte_strings))
    sorted_byte_strings = sorted(byte_strings)
    int_values = [to_int(b) for b in sorted_byte_strings]

    # Ensure the list of int values is sorted
    assert int_values == sorted(int_values)


def test_float_order_preservation():
    floats = [1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7]
    floats = random.sample(floats, len(floats))
    sorted_floats = sorted(floats)
    int_values = [to_int(f) for f in sorted_floats]

    # Ensure the list of int values is sorted
    assert int_values == sorted(int_values)


def test_date_order_preservation():
    dates = [
        datetime.date(2020, 1, 1),
        datetime.date(2021, 1, 1),
        datetime.date(2022, 1, 1),
        datetime.date(2019, 1, 1),
        datetime.date(2018, 1, 1),
        datetime.date(2023, 1, 1),
        datetime.date(2017, 1, 1),
    ]
    dates = random.sample(dates, len(dates))
    sorted_dates = sorted(dates)
    int_values = [to_int(d) for d in sorted_dates]

    # Ensure the list of int values is sorted
    assert int_values == sorted(int_values)


def test_datetime_order_preservation():
    datetimes = [
        datetime.datetime(2020, 1, 1, 12, 0, 0),
        datetime.datetime(2021, 1, 1, 12, 0, 0),
        datetime.datetime(2022, 1, 1, 12, 0, 0),
        datetime.datetime(2019, 1, 1, 12, 0, 0),
        datetime.datetime(2018, 1, 1, 12, 0, 0),
        datetime.datetime(2023, 1, 1, 12, 0, 0),
        datetime.datetime(2017, 1, 1, 12, 0, 0),
    ]
    datetimes = random.sample(datetimes, len(datetimes))
    sorted_datetimes = sorted(datetimes)
    int_values = [to_int(dt) for dt in sorted_datetimes]

    # Ensure the list of int values is sorted
    assert int_values == sorted(int_values)


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
