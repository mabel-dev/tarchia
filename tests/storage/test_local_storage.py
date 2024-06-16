import sys
import os
import shutil
import time

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from tarchia.storage import storage_factory

TEMP_FOLDER = "_temp"

local_storage = storage_factory("LOCAL")


def test_local_storage_no_as_at():
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
    for planet in planets:
        local_storage.write_blob(f"{TEMP_FOLDER}/{planet}-{int(time.time_ns()//1e3)}", planet.encode())

    # Reading a blob from a file
    mercury = local_storage.blob_list(f"{TEMP_FOLDER}/mercury")[0]
    content = local_storage.read_blob(mercury)
    assert content == b"mercury", content

    # Listing files with a specific prefix (m)
    files = local_storage.blob_list(f"{TEMP_FOLDER}/m")
    assert len(files) == 2
    assert any(f.startswith(f"{TEMP_FOLDER}/mercury") for f in files)
    assert any(f.startswith(f"{TEMP_FOLDER}/mars") for f in files)

    # Listing files with a specific prefix (jupiter)
    files = local_storage.blob_list(f"{TEMP_FOLDER}/jupiter")
    assert len(files) == 1
    assert any(f.startswith(f"{TEMP_FOLDER}/jupiter") for f in files)

    # Listing files with just a path prefix
    files = local_storage.blob_list(f"{TEMP_FOLDER}/")
    assert len(files) == 9

    # Listing files with a non-existant prefix
    files = local_storage.blob_list(f"{TEMP_FOLDER}/a")
    assert len(files) == 0

    shutil.rmtree(TEMP_FOLDER, ignore_errors=True)

def test_local_storage_as_at():
        
    try:
        shutil.rmtree(TEMP_FOLDER, ignore_errors=True)
        os.mkdir(TEMP_FOLDER)
    except:
        pass

    for t in range(10):
        local_storage.write_blob(f"{TEMP_FOLDER}/manifest-{str(t) * 8}.json", b"file contents")

    files = local_storage.blob_list(f"{TEMP_FOLDER}/manifest", as_at=40_000_000)
    assert len(files) == 1
    assert files[0] == '_temp/manifest-33333333.json'

    files = local_storage.blob_list(f"{TEMP_FOLDER}/manifest", as_at=44_444_444)
    assert len(files) == 1
    assert files[0] == '_temp/manifest-44444444.json', files


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
