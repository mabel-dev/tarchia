"""Define once, use everywhere"""

IDENTIFIER_REG_EX = r"^[a-zA-Z_][a-zA-Z0-9_]*$"
SHA_OR_HEAD_REG_EX = r"^(head|[a-f0-9]{64})$"

HISTORY_ROOT = "[metadata_root]/[owner]/[table_id]/metadata/history"
MANIFEST_ROOT = "[metadata_root]/[owner]/[table_id]/metadata/manifests"
COMMITS_ROOT = "[metadata_root]/[owner]/[table_id]/metadata/commits"

MAIN_BRANCH = "main"
