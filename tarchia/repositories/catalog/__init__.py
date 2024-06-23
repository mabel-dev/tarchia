from tarchia import config
from tarchia.exceptions import InvalidConfigurationError


def catalog_factory():
    if config.CATALOG_PROVIDER is None or config.CATALOG_PROVIDER.upper() == "DEVELOPMENT":
        from tarchia.repositories.catalog.dev_catalog import DevelopmentCatalogProvider

        return DevelopmentCatalogProvider(config.CATALOG_NAME or "catalog.json")
    if config.CATALOG_PROVIDER.upper() == "FIRESTORE":
        from tarchia.repositories.catalog.gcs_firestore import FirestoreCatalogProvider

        return FirestoreCatalogProvider(config.CATALOG_NAME)
    raise InvalidConfigurationError(setting="CATALOG_PROVIDER")
