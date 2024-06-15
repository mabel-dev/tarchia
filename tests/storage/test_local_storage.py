import sys
import os
import shutil

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from tarchia.storage import storage_factory

TEMP_FOLDER = "_temp"

local_storage = storage_factory("LOCAL")


def test_local_storage():

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
        local_storage.write_blob(f"{TEMP_FOLDER}/{planet}", planet.encode())

    # Reading a blob from a file
    content = local_storage.read_blob(f"{TEMP_FOLDER}/mercury")
    assert content == b"mercury", content

    # Listing files with a specific prefix (m)
    files = local_storage.blob_list(f"{TEMP_FOLDER}/m", as_at=0)
    assert len(files) == 2
    assert f"{TEMP_FOLDER}/mercury" in files
    assert f"{TEMP_FOLDER}/mars" in files

    # Listing files with a specific prefix (jupiter)
    files = local_storage.blob_list(f"{TEMP_FOLDER}/jupiter", as_at=0)
    assert len(files) == 1
    assert f"{TEMP_FOLDER}/jupiter" in files

    # Listing files with just a path prefix
    files = local_storage.blob_list(f"{TEMP_FOLDER}/", as_at=0)
    assert len(files) == 9

    # Listing files with a non-existant prefix
    files = local_storage.blob_list(f"{TEMP_FOLDER}/a", as_at=0)
    assert len(files) == 0

    shutil.rmtree(TEMP_FOLDER, ignore_errors=True)


if __name__ == "__main__":
    test_local_storage()
