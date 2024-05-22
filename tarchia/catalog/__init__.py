import config
from exceptions import InvalidConfigurationError


def catalog_factory():
    if config.CATALOG_PROVIDER == "TinyDB":
        from catalog.tinydb_catalog import TinyDBCatalogProvider

        return TinyDBCatalogProvider()
    raise InvalidConfigurationError(setting="CATALOG_PROVIDER")
