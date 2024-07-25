
import sys
import os
import pytest
import orjson
from orso.types import OrsoTypes

os.environ["CATALOG_NAME"] = "test_catalog.json"
os.environ["TARCHIA_DEBUG"] = "TRUE"

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
import pytest
from unittest.mock import patch, call

from tarchia.models import TableCatalogEntry, Schema, Column, OwnerEntry

def test_subscribe():
    eventable = TableCatalogEntry(
        name="test", steward="test", owner="test", table_id="test", location="test", partitioning=[], last_updated_ms=0, permissions=[], visibility="PUBLIC", current_schema=Schema(columns=[]), freshness_life_in_days=0, retention_in_days=0
    )
    eventable.subscribe(user='user1', event='NEW_COMMIT', url='http://example.com/webhook_created')
    assert len(eventable.subscriptions) == 1
    assert eventable.subscriptions[0].user == 'user1'
    assert eventable.subscriptions[0].event == 'NEW_COMMIT'
    assert eventable.subscriptions[0].url == 'http://example.com/webhook_created'

def test_unsubscribe():
    eventable = TableCatalogEntry(
        name="test", steward="test", owner="test", table_id="test", location="test", partitioning=[], last_updated_ms=0, permissions=[], visibility="PUBLIC", current_schema=Schema(columns=[]), freshness_life_in_days=0, retention_in_days=0
    )
    eventable.subscribe(user='user1', event='NEW_COMMIT', url='http://example.com/webhook_created')
    eventable.unsubscribe(user='user1', event='NEW_COMMIT', url='http://example.com/webhook_created')
    assert len(eventable.subscriptions) == 0

def test_trigger_event():

    eventable = TableCatalogEntry(
        name="test", steward="test", owner="test", table_id="test", location="test", partitioning=[], last_updated_ms=0, permissions=[], visibility="PUBLIC", current_schema=Schema(columns=[]), freshness_life_in_days=0, retention_in_days=0
    )
    eventable.subscribe(user='user1', event=eventable.EventTypes.NEW_COMMIT, url='http://example.com/webhook_created')
    eventable.trigger_event(eventable.EventTypes.NEW_COMMIT, {'table_id': '123', 'name': 'ExampleTable'})


def test_trigger_event_not_supported():
    eventable = TableCatalogEntry(
        name="test", steward="test", owner="test", table_id="test", location="test", partitioning=[], last_updated_ms=0, permissions=[], visibility="PUBLIC", current_schema=Schema(columns=[]), freshness_life_in_days=0, retention_in_days=0
    )
    with pytest.raises(ValueError):
        eventable.trigger_event('unsupported_event', {'table_id': '123', 'name': 'ExampleTable'})

def test_subscribe_not_supported_event():
    eventable = TableCatalogEntry(
        name="test", steward="test", owner="test", table_id="test", location="test", partitioning=[], last_updated_ms=0, permissions=[], visibility="PUBLIC", current_schema=Schema(columns=[]), freshness_life_in_days=0, retention_in_days=0
    )
    with pytest.raises(ValueError):
        eventable.subscribe(user='user1', event="TABLE_CREATED", url='http://example.com/webhook')

def test_notify_subscribers():
    eventable = TableCatalogEntry(
        name="test", steward="test", owner="test", table_id="test", location="test", partitioning=[], last_updated_ms=0, permissions=[], visibility="PUBLIC", current_schema=Schema(columns=[]), freshness_life_in_days=0, retention_in_days=0
    )
    with patch.object(eventable, '_send_request_with_retries') as mock_send_request:
        eventable.notify_subscribers('http://example.com/webhook', {'table_id': '123', 'name': 'ExampleTable'})
        eventable._executor.shutdown(wait=True)
        mock_send_request.assert_called_once_with('http://example.com/webhook', orjson.dumps({'table_id': '123', 'name': 'ExampleTable'}))

def test_send_request_with_retries():
    eventable = TableCatalogEntry(
        name="test", steward="test", owner="test", table_id="test", location="test", partitioning=[], last_updated_ms=0, permissions=[], visibility="PUBLIC", current_schema=Schema(columns=[]), freshness_life_in_days=0, retention_in_days=0
    )
    with patch('requests.post') as mock_post:
        mock_post.return_value.status_code = 200
        eventable._send_request_with_retries('http://example.com/webhook', {'table_id': '123', 'name': 'ExampleTable'})
        mock_post.assert_called_with('http://example.com/webhook', json={'table_id': '123', 'name': 'ExampleTable'}, timeout=10)

if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests
    
    run_tests()