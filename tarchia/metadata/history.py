import hashlib
import os
import sys
from io import BytesIO
from typing import List
from typing import Optional

from fastavro import reader
from fastavro import writer

from tarchia.models import HISTORY_SCHEMA
from tarchia.models import HistoryEntry
from tarchia.utils.constants import MAIN_BRANCH

sys.path.insert(0, os.path.join(sys.path[0], "../.."))


class HistoryTree:
    def __init__(self, trunk_branch_name: str = MAIN_BRANCH):
        self.trunk_branch_name = trunk_branch_name
        self.commits = []
        self.branches = {self.trunk_branch_name: None}
        self.deleted_branches = set()

    def commit(self, new_commit: HistoryEntry) -> HistoryEntry:
        branch = new_commit.branch
        if branch in self.deleted_branches:
            raise ValueError(f"Cannot add commit to deleted branch '{branch}'.")
        if branch not in self.branches:
            self.branches[branch] = None
        self.commits.append(new_commit)
        self.branches[branch] = new_commit
        return new_commit

    def delete_branch(self, branch: str):
        raise NotImplementedError("Not correctly implemented")
        if branch not in self.branches:
            raise ValueError(f"Branch '{branch}' does not exist.")
        if branch == self.trunk_branch_name:
            raise ValueError(f"Cannot delete the trunk branch '{self.trunk_branch_name}'.")
        self.deleted_branches.add(branch)

    def merge_branch(self, source_branch: str, target_branch: str) -> HistoryEntry:
        raise NotImplementedError("Not correctly implemented")
        if source_branch not in self.branches:
            raise ValueError(f"Source branch '{source_branch}' does not exist.")
        if source_branch in self.deleted_branches:
            raise ValueError(f"Source branch '{source_branch}' is deleted.")
        if target_branch in self.deleted_branches:
            raise ValueError(f"Target branch '{target_branch}' is deleted.")
        source_head = self.branches[source_branch]
        if not source_head:
            raise ValueError(f"Source branch '{source_branch}' has no commits to merge.")
        target_head = self.branches.get(target_branch)
        new_commit = self.commit(
            HistoryEntry(
                sha="",
                branch=target_branch,
                message=f"Merge branch '{source_branch}' into '{target_branch}'",
                user="",
                timestamp=0,
                parent_sha=target_head,
            )
        )
        return new_commit

    def calculate_root_hash(self) -> str:
        if not self.commits:
            return ""
        nodes = [commit.hash for commit in self.commits]
        while len(nodes) > 1:
            if len(nodes) % 2 != 0:
                nodes.append(nodes[-1])
            new_level = []
            for i in range(0, len(nodes), 2):
                new_level.append(self._hash_pair(nodes[i], nodes[i + 1]))
            nodes = new_level
        return nodes[0]

    def _hash_pair(self, left: str, right: str) -> str:
        hasher = hashlib.sha256()
        hasher.update(left.encode("utf-8"))
        hasher.update(right.encode("utf-8"))
        return hasher.hexdigest()

    def save_to_avro(self) -> bytes:
        """We don't save directly so we can abstract the file storage"""
        file = BytesIO()
        records = [commit.as_dict() for commit in self.commits]
        writer(file, HISTORY_SCHEMA, records, codec="zstandard")
        file.seek(0, 0)
        return file.read()

    @classmethod
    def from_list(
        cls, commits: List[HistoryEntry], trunk_branch_name: str = MAIN_BRANCH
    ) -> "HistoryTree":
        tree = cls(trunk_branch_name)
        tree.commits = sorted(commits, key=lambda entry: entry.timestamp, reverse=True)
        for commit in tree.commits:
            if tree.branches.get(commit.branch) is None:
                tree.branches[commit.branch] = commit
        return tree

    @classmethod
    def load_from_avro(cls, contents: bytes, trunk_branch_name: str = MAIN_BRANCH) -> "HistoryTree":
        stream = BytesIO(contents)
        data = list(reader(stream, HISTORY_SCHEMA))
        commits = (HistoryEntry(**record) for record in data)
        tree = cls.from_list(commits, trunk_branch_name)
        return tree

    def get_commit_by_hash(self, commit_hash: str) -> Optional[HistoryEntry]:
        for commit in self.commits:
            if commit.hash == commit_hash:
                return commit
        return None

    def get_branch_head(self, branch: str) -> Optional[HistoryEntry]:
        if branch in self.deleted_branches:
            return None
        return self.branches.get(branch)

    def get_current_branches(self) -> List[str]:
        return [branch for branch in self.branches if branch not in self.deleted_branches]

    def walk_branch(self, branch: str):
        head_commit = self.get_branch_head(branch)
        if not head_commit:
            print(f"Branch '{branch}' does not exist or is deleted.")
            return
        yield from self.walk_tree(head_commit)

    def walk_tree(self, start_commit: HistoryEntry):
        current = start_commit
        while current:
            yield current
            if current.parent_sha:
                parent = current.parent_sha
                current = None
                for entry in self.commits:
                    if entry.sha == parent:
                        current = entry
                        break
            else:
                current = None


if __name__ == "__main__":
    # Example usage
    merkle_tree = HistoryTree(trunk_branch_name="main")
    root_commit = merkle_tree.commit(
        HistoryEntry(sha="root", branch="main", message="Initial commit", user="user1", timestamp=1)
    )
    second_commit = merkle_tree.commit(
        HistoryEntry(
            sha="2nd",
            branch="main",
            message="Second commit",
            user="user1",
            timestamp=2,
            parent_sha="root",
        )
    )
    feature_commit = merkle_tree.commit(
        HistoryEntry(
            sha="feat",
            branch="feature",
            message="Fork Me",
            user="user1",
            timestamp=3,
            parent_sha="2nd",
        )
    )
    third_commit = merkle_tree.commit(
        HistoryEntry(
            sha="3rd",
            branch="main",
            message="third commit",
            user="user1",
            timestamp=4,
            parent_sha="2nd",
        )
    )
    feat2_commit = merkle_tree.commit(
        HistoryEntry(
            sha="2feat",
            branch="feature",
            message="2nd feat commit",
            user="user1",
            timestamp=5,
            parent_sha="feat",
        )
    )
    # merge_commit = merkle_tree.merge_branch("feature1", "main")

    # Get all current branches
    current_branches = merkle_tree.get_current_branches()
    print("Current branches:", current_branches)

    # Walk the branches and display their commits
    for branch in current_branches:
        print(f"\nWalking branch '{branch}':")
        merkle_tree.walk_branch(branch)

    # Save and load the tree
    file_bytes = merkle_tree.save_to_avro()
    loaded_tree = HistoryTree.load_from_avro(file_bytes)

    print(len(file_bytes))
    print("RELOADED")
    current_branches = loaded_tree.get_current_branches()
    print("Current branches:", current_branches)

    # Walk the branches and display their commits
    for branch in current_branches:
        print(f"\nWalking branch '{branch}':")
        loaded_tree.walk_branch(branch)
