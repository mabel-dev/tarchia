import sys
import os

os.environ["CATALOG_NAME"] = "test_catalog.json"
os.environ["TARCHIA_DEBUG"] = "TRUE"

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from tarchia.models import Schema, Column
from tarchia.manifests.pruning import parse_filters, prune
from tarchia.manifests import ManifestEntry

def test_basic_parsing():

    schema = Schema(columns=[(Column(name="integer", type="INTEGER"))])
    filters = parse_filters("integer=0", schema)
    assert filters == [('integer', '=', 0)]


def test_basic_pruning():
    manifest = ManifestEntry(file_path="", file_format="", file_type="Data", record_count=0, file_size=0, lower_bounds={"integer": -10}, upper_bounds={"integer": 10})

    assert prune(manifest, [("integer", "=", 11)])
    assert prune(manifest, [("integer", "=", -11)])

    assert not prune(manifest, [("integer", "=", 10)])
    assert not prune(manifest, [("integer", "=", 0)])
    assert not prune(manifest, [("integer", "=", -10)])

    assert prune(manifest, [("integer", ">", 11)])
    assert not prune(manifest, [("integer", ">", 10)])  # we truncate values so we keep the cusps
    assert not prune(manifest, [("integer", ">", 0)])
    assert not prune(manifest, [("integer", ">", -10)])
    assert not prune(manifest, [("integer", ">", -11)])

    assert prune(manifest, [("integer", ">=", 11)])
    assert not prune(manifest, [("integer", ">=", 10)])
    assert not prune(manifest, [("integer", ">=", 0)])
    assert not prune(manifest, [("integer", ">=", -10)])
    assert not prune(manifest, [("integer", ">=", -11)])

    assert not prune(manifest, [("integer", "<", 11)])
    assert not prune(manifest, [("integer", "<", 10)])
    assert not prune(manifest, [("integer", "<", 0)])
    assert not prune(manifest, [("integer", "<", -10)])
    assert prune(manifest, [("integer", "<", -11)])

    assert not prune(manifest, [("integer", "<=", 11)])
    assert not prune(manifest, [("integer", "<=", 10)])
    assert not prune(manifest, [("integer", "<=", 0)])
    assert not prune(manifest, [("integer", "<=", -10)])
    assert prune(manifest, [("integer", "<=", -11)])

if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()