"""
A simple document store used for testing and development.

Not intended for production use.
"""

from typing import Any
from typing import Dict
from typing import List

import orjson


class _DocumentStore:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.data = self._load()

    def _load(self) -> Dict[str, Dict[str, Any]]:
        try:
            with open(self.file_path, "rb") as file:
                return orjson.loads(file.read())
        except FileNotFoundError:
            return {}
        except orjson.JSONDecodeError as err:
            print(err)
            return {}

    def _save(self) -> None:
        with open(self.file_path, "wb") as file:
            file.write(orjson.dumps(self.data))

    def find(self, collection: str, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        def matches(doc: Dict[str, Any]) -> bool:
            return all(doc.get(k) == v for k, v in query.items())

        return [doc for doc in self.data.get(collection, []) if matches(doc)]

    def upsert(self, collection: str, document: Dict[str, Any], query: Dict[str, Any] = {}) -> None:
        collection_data = self.data.setdefault(collection, [])
        updated = False

        for doc in collection_data:
            if all(doc.get(k) == v for k, v in query.items()):
                doc.update(document)
                updated = True
                break

        if not updated:
            collection_data.append(document)

        self._save()

    def delete(self, collection: str, query: Dict[str, Any]) -> None:
        self.data[collection] = [
            doc
            for doc in self.data.get(collection, [])
            if not all(doc.get(k) == v for k, v in query.items())
        ]
        self._save()


class DocumentStore(_DocumentStore):
    """
    Singleton wrapper for the _DocumentStore class.
    Only allows one instance of _DocumentStore to exist per location.
    """

    slots = "_instances"

    _instances: dict[str, _DocumentStore] = {}

    def __new__(cls, location: str):
        if cls._instances.get(location) is None:
            cls._instances[location] = _DocumentStore(location)
        return cls._instances[location]
