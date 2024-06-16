import os
from typing import List
from typing import Optional

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
        file_descriptor = os.open(location, os.O_RDONLY | os.O_BINARY)
        try:
            size = os.path.getsize(location)
            return os.read(file_descriptor, size)
        finally:
            os.close(file_descriptor)

    def blob_list(self, prefix: str, as_at: Optional[int] = None) -> List[str]:
        """
        Get all file paths in the specified folder that match the given prefix.
        If the prefix ends with a slash ("/" or "\\"), it is treated as a directory.

        Parameters:
            prefix (str): The prefix to match, including the folder path.

        Returns:
            List[str]: A list of file paths that match the prefix.
        """
        # Determine if the prefix is a folder or includes a filename part
        if prefix.endswith(OS_SEP):
            folder = prefix
            file_prefix = ""
        else:
            folder = os.path.dirname(prefix)
            file_prefix = os.path.basename(prefix)

        files = []
        try:
            with os.scandir(folder) as entries:
                for entry in entries:
                    if entry.is_file() and entry.name.startswith(file_prefix):
                        files.append(entry.path)
        except FileNotFoundError:
            return []

        # If as_at is specified, filter and return the most recent file before the given timestamp
        if as_at is not None:
            filtered_files = []
            for file_path in sorted(files):
                # Extract the timestamp part from the filename
                filename = os.path.basename(file_path)
                try:
                    # Assuming the timestamp is part of the filename, for example, 'file-20220101.txt'
                    timestamp_str = filename.split("-")[-1].split(".")[0]
                    timestamp = int(timestamp_str)
                    if timestamp > as_at:
                        break
                    filtered_files.append((timestamp, file_path))
                except ValueError:
                    continue

            if not filtered_files:
                return []

            # Sort by timestamp and get the most recent one
            filtered_files.sort(key=lambda x: x[0], reverse=True)
            return [filtered_files[0][1]]

        return files
