import os

from tarchia.config import BUCKET_NAME

from .storage_provider import StorageProvider


class GoogleCloudStorage(StorageProvider):
    def __init__(self) -> None:
        super().__init__()

        try:
            from google.api_core import retry  # type:ignore
            from google.api_core.exceptions import InternalServerError  # type:ignore
            from google.api_core.exceptions import TooManyRequests
            from google.auth.credentials import AnonymousCredentials  # type:ignore
            from google.cloud import storage  # type:ignore
            from urllib3.exceptions import ProtocolError  # type:ignore

        except ImportError:  # pragma: no cover
            from tarchia.exceptions import MissingDependencyError

            raise MissingDependencyError("google-cloud-storage")

        if os.environ.get("STORAGE_EMULATOR_HOST") is not None:
            self.client = storage.Client(credentials=AnonymousCredentials())
        else:  # pragma: no cover
            self.client = storage.Client()

        predicate = retry.if_exception_type(
            ConnectionResetError, ProtocolError, InternalServerError, TooManyRequests
        )
        self.retry = retry.Retry(predicate)
        self.bucket_name = BUCKET_NAME

    def write_blob(self, location: str, content: bytes):
        bucket = self.client.get_bucket(self.bucket_name)
        blob = bucket.blob(location)
        self.retry(blob.upload_from_string)(content, content_type="application/octet-stream")

    def read_blob(self, location: str) -> bytes:
        bucket = self.client.get_bucket(self.bucket_name)
        blob = bucket.get_blob(location)
        return blob.download_as_bytes()
