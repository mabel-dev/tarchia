"""
This module provides the functionality to manage event subscriptions and notifications
for a variety of events using a base class `Eventable`. It includes definitions for
supported events, subscription management, and asynchronous notification handling
with retry logic.

Classes:
    Subscription(BaseModel): Represents a subscription with user, event, and URL attributes.
    SupportedEvents(Enum): Defines the set of supported events.
    Eventable: Base class to manage event subscriptions and notifications.

Methods:
    subscribe(user, event, url): Subscribes a user to an event with a specific URL.
    unsubscribe(user, event, url): Unsubscribes a user from an event with a specific URL.
    trigger_event(event, data): Triggers an event and notifies all subscribed URLs.
    notify_subscribers(url, data): Notifies a single subscriber asynchronously with retries.
    _send_request_with_retries(url, data): Sends an HTTP request with retry logic.

Example Usage:
    eventable = Eventable()
    eventable.subscribe(user='user1', event='TABLE_CREATED', url='http://example.com/webhook_created')
    eventable.trigger_event('TABLE_CREATED', {'table_id': '123', 'name': 'ExampleTable'})
"""

import concurrent.futures
from enum import Enum
from typing import List

import orjson
import requests
from orso.tools import retry
from pydantic import BaseModel
from requests.exceptions import ConnectionError
from requests.exceptions import Timeout


def is_valid_url(url: str) -> bool:
    """
    Check if the given string is a valid URL.

    Parameters:
        url (str): The input string to be checked.

    Returns:
        bool: True if the input string is a valid URL, False otherwise.
    """
    from urllib.parse import urlparse

    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False


class Subscription(BaseModel):
    user: str
    event: str
    url: str


class Eventable:
    class EventTypes(Enum):
        UNDEFINED = "UNDEFINED"

    _executor = None
    subscriptions: List[Subscription] = []

    @classmethod
    def _ensure_executor(cls):
        if cls._executor is None or cls._executor._shutdown:
            cls._executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

    def subscribe(self, user: str, event: str, url: str):
        """Subscribe a URL to a specific event for a user."""
        if not event in self.EventTypes.__members__:
            raise ValueError(f"Event '{event}' is not supported.")
        if not is_valid_url(url):
            raise ValueError(f"URL does not appear to be valid")
        subscription = Subscription(user=user, event=event, url=url)
        self.subscriptions.append(subscription)

    def unsubscribe(self, user: str, event: str, url: str):
        """Unsubscribe a URL from a specific event for a user."""
        self.subscriptions = [
            s
            for s in self.subscriptions
            if not (s.user == user and s.event == event and s.url == url)
        ]

    def trigger_event(self, event: str, data: dict):
        """Trigger an event and notify all subscribers."""
        if event not in self.EventTypes:
            raise ValueError(f"Event '{event}' is not supported.")
        for subscription in self.subscriptions:
            if subscription.event == event:
                self.notify_subscribers(subscription.url, data)

    def notify_subscribers(self, url: str, data: dict):
        """Notify a single subscriber asynchronously with retries."""
        try:
            self._ensure_executor()
            if is_valid_url(url):
                self._executor.submit(self._send_request_with_retries, url, orjson.dumps(data))
        except Exception as err:
            print(f"[TARCHIA] Error notifying subscribers. {err} ({data})")

    @retry(
        max_tries=3,
        backoff_seconds=5,
        exponential_backoff=True,
        max_backoff=60,
        retry_exceptions=(ConnectionError, Timeout),
    )
    def _send_request_with_retries(self, url: str, data: dict):
        """Send the actual HTTP request with retries."""
        response = requests.post(url, json=data, timeout=10)
        response.raise_for_status()
        print(f"Notification sent to {url}: {response.status_code}")
