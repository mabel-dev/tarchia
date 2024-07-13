import os

from .storage_provider import StorageProvider

OS_SEP = os.sep

# Define os.O_BINARY for non-Windows platforms if it's not already defined
if not hasattr(os, "O_BINARY"):
    os.O_BINARY = 0  # Value has no effect on non-Windows platforms


class LocalStorage(StorageProvider):
    def write_blob(self, location: str, content: bytes):
        """
        Writes the given data to the specified file location.

        Parameters:
            location: str
                The file path where the data should be written.
            data: bytes
                The data to be written to the file.
        """
        os.makedirs(os.path.dirname(location), exist_ok=True)
        file_descriptor = os.open(location, os.O_WRONLY | os.O_BINARY | os.O_CREAT | os.O_TRUNC)
        try:
            os.write(file_descriptor, content)
        finally:
            os.close(file_descriptor)

    def read_blob(self, location: str) -> bytes:
        """
        Read a blob (binary large object) from disk.

        We're using the low-level read, on the whole it's about 5% faster - not
        much faster considering the effort to benchmark different disk access
        methods, but as one of the slowest parts of the system we wanted to find
        if there was a faster way.

        Parameters:
            location: str
                The name of the blob file to read.

        Returns:
            The blob as bytes.
        """
        file_descriptor = None
        try:
            file_descriptor = os.open(location, os.O_RDONLY | os.O_BINARY)
            size = os.path.getsize(location)
            return os.read(file_descriptor, size)
        except FileNotFoundError:  # pragma: no cover
            return None
        finally:
            if file_descriptor:
                os.close(file_descriptor)
