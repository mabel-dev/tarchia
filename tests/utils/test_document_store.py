import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from tarchia.utils.doc_store import DocumentStore

# Helper function to set up a fresh DocumentStore
def setup_store() -> DocumentStore:
    test_file = 'test_document_store.json'
    if os.path.exists(test_file):
        os.remove(test_file)
    return DocumentStore(test_file)

# Helper function to tear down the DocumentStore
def teardown_store(store: DocumentStore):
    if os.path.exists(store.file_path):
        os.remove(store.file_path)

def test_insert_and_find_document():
    store = setup_store()
    document = {"name": "Alice", "age": 30}
    store.upsert("people",  document, {"name": "Alice"})
    
    result = store.find("people", {"name": "Alice"})
    teardown_store(store)
    
    assert len(result) == 1
    assert result[0] == document

def test_update_document():
    store = setup_store()
    document = {"name": "Bob", "age": 25}
    store.upsert("people", document, {"name": "Bob"})
    
    store.upsert("people", {"age": 26}, {"name": "Bob"})
    
    result = store.find("people", {"name": "Bob"})
    teardown_store(store)
    
    assert len(result) == 1
    assert result[0]["age"] == 26

def test_delete_document():
    store = setup_store()
    document = {"name": "Charlie", "age": 22}
    store.upsert("people", document, {"name": "Charlie"})
    
    store.delete("people", {"name": "Charlie"})
    
    result = store.find("people", {"name": "Charlie"})
    teardown_store(store)
    
    assert len(result) == 0

def test_persistence():
    store = setup_store()
    document = {"name": "Dave", "age": 40}
    store.upsert("people", document, {"name": "Dave"})
    
    # Reload the store from the file to test persistence
    new_store = DocumentStore(store.file_path)
    result = new_store.find("people", {"name": "Dave"})
    teardown_store(new_store)
    
    assert len(result) == 1
    assert result[0] == document

def test_upsert_with_multiple_search_criteria():
    store = setup_store()
    document1 = {"name": "Eve", "age": 35, "city": "New York"}
    document2 = {"name": "Eve", "age": 35, "city": "Los Angeles"}
    
    store.upsert("people", document1, {"name": "Eve", "city": "New York"})
    store.upsert("people", document2, {"name": "Eve", "city": "Los Angeles"})
    
    result1 = store.find("people", {"name": "Eve", "city": "New York"})
    result2 = store.find("people", {"name": "Eve", "city": "Los Angeles"})
    teardown_store(store)
    
    assert len(result1) == 1
    assert result1[0] == document1
    assert len(result2) == 1
    assert result2[0] == document2

def test_upsert_update_with_multiple_criteria():
    store = setup_store()
    document = {"name": "Frank", "age": 28, "city": "Chicago"}
    store.upsert("people", document, {"name": "Frank", "city": "Chicago"})
    
    updated_document = {"name": "Frank", "age": 29, "city": "Chicago"}
    store.upsert("people", updated_document, {"name": "Frank", "city": "Chicago"})
    
    result = store.find("people", {"name": "Frank", "city": "Chicago"})
    teardown_store(store)
    
    assert len(result) == 1
    assert result[0]["age"] == 29



if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
