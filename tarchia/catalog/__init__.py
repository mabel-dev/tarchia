import config
from exceptions import InvalidConfigurationError


def catalog_factory():
    if config.CATALOG_PROVIDER is None or config.CATALOG_PROVIDER.upper() == "TINYDB":
        from catalog.tinydb_catalog import TinyDBCatalogProvider

        return TinyDBCatalogProvider()
    raise InvalidConfigurationError(setting="CATALOG_PROVIDER")
