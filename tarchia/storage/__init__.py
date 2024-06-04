import config
from exceptions import InvalidConfigurationError


def storage_factory():
    if config.STORAGE_PROVIDER is None or config.STORAGE_PROVIDER.upper() == "LOCAL":
        from .local_storage import LocalStorage

        return LocalStorage()
    if config.STORAGE_PROVIDER.upper() in ("GOOGLE", "GCP", "GCS"):
        from .google_cloud_storage import GoogleCloudStorage

        return GoogleCloudStorage()

    if config.STORAGE_PROVIDER.upper() in ("AMAZON", "S3", "MINIO"):
        from .s3_storage import S3Storage

        return S3Storage()
    raise InvalidConfigurationError(setting="STORAGE_PROVIDER")
