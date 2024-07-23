from typing import List
from typing import Optional

from pydantic import Field

from .metadata_models import Schema
from .tarchia_base import TarchiaBaseModel

HISTORY_SCHEMA = {
    "type": "record",
    "name": "Commit",
    "fields": [
        {"name": "sha", "type": "string"},
        {"name": "branch", "type": "string"},
        {"name": "message", "type": "string"},
        {"name": "user", "type": "string"},
        {"name": "timestamp", "type": "int"},
        {"name": "parent_sha", "type": ["null", "string"], "default": None},
    ],
}


class EncryptionDetails(TarchiaBaseModel):
    """
    Model representing encryption details.

    Attributes:
        algorithm (str): The encryption algorithm used.
        key_id (str): The identifier for the encryption key.
        fields (List[str]): The fields to be encrypted.
    """

    algorithm: str
    key_id: str
    fields: List[str]


class Commit(TarchiaBaseModel):
    """
    Model representing a commit.
    """

    data_hash: str
    user: str
    message: str
    branch: str
    parent_commit_sha: Optional[str]
    last_updated_ms: int
    manifest_path: Optional[str]
    table_schema: Schema
    encryption: Optional[EncryptionDetails] = None
    commit_sha: Optional[str] = None

    added_files: Optional[List[str]] = Field(default_factory=list)
    removed_files: Optional[List[str]] = Field(default_factory=list)

    def calculate_hash(self) -> str:
        import hashlib

        hasher = hashlib.sha256()
        hasher.update(self.data_hash.encode())
        hasher.update(self.message.encode())
        hasher.update(self.user.encode())
        hasher.update(self.branch.encode())
        hasher.update(str(self.last_updated_ms).encode())
        if self.parent_commit_sha:
            hasher.update(self.parent_commit_sha.encode())
        return hasher.hexdigest()

    def __init__(self, **data):
        super().__init__(**data)
        self.commit_sha = self.calculate_hash()

    @property
    def history_entry(self):
        """Slimemed record for Merkle Tree"""
        return HistoryEntry(
            sha=self.commit_sha,
            branch=self.branch,
            message=self.message,
            user=self.user,
            timestamp=self.last_updated_ms,
            parent_sha=self.parent_commit_sha,
        )


class HistoryEntry(TarchiaBaseModel):
    sha: str
    branch: str
    message: str
    user: str
    timestamp: int
    parent_sha: Optional[str] = None
