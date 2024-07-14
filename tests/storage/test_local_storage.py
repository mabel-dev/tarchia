import sys
import os
import shutil
import time

os.environ["CATALOG_NAME"] = "test_catalog.json"
os.environ["TARCHIA_DEBUG"] = "TRUE"

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from tarchia.storage import storage_factory

TEMP_FOLDER = "_temp"

local_storage = storage_factory("LOCAL")


def test_local_storage():
    """
    Test simple access to files and folders using the local storage provider
    """

    # Writing a blob to a file
    try:
        shutil.rmtree(TEMP_FOLDER, ignore_errors=True)
        os.mkdir(TEMP_FOLDER)
    except:
        pass

    # write some data for testing
    planets = [
        "mercury",
        "venus",
        "earth",
        "mars",
        "jupiter",
        "saturn",
        "uranus",
        "neptune",
        "pluto",
    ]
    mercury = None
    for planet in planets:
        path = f"{TEMP_FOLDER}/{planet}-{int(time.time_ns()//1e6)}"
        local_storage.write_blob(
            path, planet.encode()
        )
        if planet == "mercury":
            mercury = path

    # Reading a blob from a file
    content = local_storage.read_blob(mercury)
    assert content == b"mercury", content

    shutil.rmtree(TEMP_FOLDER, ignore_errors=True)



if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
