from tarchia import config
from tarchia.exceptions import InvalidConfigurationError


def catalog_factory():
    if config.CATALOG_PROVIDER is None or config.CATALOG_PROVIDER.upper() == "DEVELOPMENT":
        from tarchia.repositories.catalog.dev_catalog import DevelopmentCatalogProvider

        return DevelopmentCatalogProvider(config.CATALOG_NAME or "catalog.json")
    raise InvalidConfigurationError(setting="CATALOG_PROVIDER")
