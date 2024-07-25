import sys
import os
import pytest
from orso.types import OrsoTypes

os.environ["CATALOG_NAME"] = "test_catalog.json"
os.environ["TARCHIA_DEBUG"] = "TRUE"

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from tarchia.models import Schema, Column
from tarchia.metadata.manifests import build_manifest_entry
from tarchia.utils.to_int import to_int
from tarchia.exceptions import DataError

SCHEMA = Schema(
    columns=[
        Column(name="id", type=OrsoTypes.INTEGER),
        Column(name="name", type=OrsoTypes.VARCHAR),
        Column(name="mass", type=OrsoTypes.DOUBLE),
        Column(name="diameter", type=OrsoTypes.DOUBLE),
        Column(name="density", type=OrsoTypes.DOUBLE),
        Column(name="escapeVelocity", type=OrsoTypes.DOUBLE),
        Column(name="rotationPeriod", type=OrsoTypes.DOUBLE),
        Column(name="lengthOfDay", type=OrsoTypes.DOUBLE),
        Column(name="distanceFromSun", type=OrsoTypes.DOUBLE),
    ]
)

def test_build_basic_manifest():

    basic = build_manifest_entry("testdata/planets/planets.parquet", SCHEMA).as_dict()

    assert basic["file_path"] == "testdata/planets/planets.parquet"
    assert basic["record_count"] == 9
    assert basic["sha256_checksum"] == "5a66d1e67f9b3749983da132d78e4744ee78b09b6549719c4d6359f573ac3baa"

    lowers = basic["lower_bounds"]
    uppers = basic["upper_bounds"]

    assert lowers["name"] == to_int("Earth")
    assert uppers["name"] == to_int("Venus")

def test_manifest_missing_columns():
    """The name is unclear, here the data is missing columns"""

    test_schema = Schema(**SCHEMA.as_dict())
    test_schema.columns.append(Column(name="king", type=OrsoTypes.VARCHAR))

    with pytest.raises(DataError):
        build_manifest_entry("testdata/planets/planets.parquet", test_schema).as_dict()

def test_manifest_missing_column_with_default():
    """The name is unclear, here the data is missing columns"""

    test_schema = Schema(**SCHEMA.as_dict())
    test_schema.columns.append(Column(name="king", type=OrsoTypes.VARCHAR, default="bob"))

    # we don't raise an exception unlike the test_manifest_missing_columns test
    build_manifest_entry("testdata/planets/planets.parquet", test_schema).as_dict()


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()