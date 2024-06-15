from typing import Optional

from tarchia import config
from tarchia.exceptions import InvalidConfigurationError

from .storage_provider import StorageProvider


def storage_factory(provider: Optional[str] = None) -> StorageProvider:
    """
    Factory function to create and return a storage provider instance based on the specified or
    default provider (local disk storage). The function supports multiple storage providers
    including local storage, Google Cloud Storage, and Amazon S3/MinIO.

    Parameters:
        provider (Optional[str]):
            The name of the storage provider to use.

    Returns:
        An instance of the appropriate storage provider class.

    Raises:
        InvalidConfigurationError: If the specified provider is not recognized or supported.

    Examples:
        To create a local storage instance:
            storage = storage_factory("LOCAL")

        To create a Google Cloud Storage instance:
            storage = storage_factory("GOOGLE")

        To create an S3 storage instance:
            storage = storage_factory("S3")

        To use the default storage provider from the configuration:
            storage = storage_factory()
    """

    if provider is None:
        provider = config.STORAGE_PROVIDER
    if provider is not None:
        provider = provider.upper()

    if provider is None or provider == "LOCAL":
        from .local_storage import LocalStorage

        return LocalStorage()
    if provider in ("GOOGLE", "GCP", "GCS"):
        from .google_cloud_storage import GoogleCloudStorage

        return GoogleCloudStorage()

    if provider in ("AMAZON", "S3", "MINIO"):
        from .s3_storage import S3Storage

        return S3Storage()
    raise InvalidConfigurationError(setting="STORAGE_PROVIDER")
