from tarchia.exceptions import InvalidConfigurationError
from tarchia.utils import config

from .provider_base import CatalogProvider


def catalog_factory() -> CatalogProvider:  # pragma: no cover
    if config.CATALOG_PROVIDER is None or config.CATALOG_PROVIDER.upper() == "DEVELOPMENT":
        from tarchia.interfaces.catalog.dev_catalog import DevelopmentCatalogProvider

        return DevelopmentCatalogProvider(config.CATALOG_NAME or "catalog.json")
    if config.CATALOG_PROVIDER.upper() == "FIRESTORE":
        from tarchia.interfaces.catalog.gcs_firestore import FirestoreCatalogProvider

        return FirestoreCatalogProvider(config.CATALOG_NAME)
    raise InvalidConfigurationError(setting="CATALOG_PROVIDER")
