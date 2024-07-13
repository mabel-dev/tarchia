import os

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

    def write_blob(self, location: str, content: bytes):
        bucket_name, blob_name = location.split("/", 1)
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        self.retry(blob.upload_from_string)(content, content_type="application/octet-stream")

    def read_blob(self, location: str) -> bytes:
        bucket, blob_name = location.split("/", 1)
        gcs_bucket = self.client.get_bucket(bucket)
        blob = gcs_bucket.get_blob(blob_name)
        return blob.download_as_bytes()
