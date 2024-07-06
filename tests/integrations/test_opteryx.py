"""
TESTS TO WRITE

- can opteryx query a tarchia table
- do we push filters?
"""

import sys
import os
import orjson

os.environ["CATALOG_NAME"] = "test_catalog.json"
os.environ["TARCHIA_DEBUG"] = "TRUE"

try:
    os.remove(os.environ["CATALOG_NAME"])
except FileNotFoundError:
    pass

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

def test_opteryx():
    import opteryx

    