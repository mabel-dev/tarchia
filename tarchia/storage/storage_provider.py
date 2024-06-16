import inspect
from typing import List
from typing import Optional


class StorageProvider:
    def write_blob(self, location: str, content: bytes):
        raise NotImplementedError(
            f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name} is not implemented."
        )

    def read_blob(self, location: str) -> bytes:
        raise NotImplementedError(
            f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name} is not implemented."
        )

    def blob_list(self, prefix: str, as_at: Optional[int] = None) -> List[str]:
        raise NotImplementedError(
            f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name} is not implemented."
        )
