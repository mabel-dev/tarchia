from dataclasses import dataclass


@dataclass
class AddSnapshotRequest:
    parent: str


@dataclass
class CreateTableRequest:
    metadata: str


@dataclass
class TableCloneRequest:
    metadata: str


@dataclass
class UpdateMetadataRequest:
    metadata: str


@dataclass
class UpdateSchemaRequest:
    metadata: str
