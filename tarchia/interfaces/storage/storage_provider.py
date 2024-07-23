import inspect


class StorageProvider:  # pragma: no cover
    def write_blob(self, location: str, content: bytes):
        raise NotImplementedError(
            f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name} is not implemented."
        )

    def read_blob(self, location: str, bucket_in_path: bool = False) -> bytes:
        raise NotImplementedError(
            f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name} is not implemented."
        )
