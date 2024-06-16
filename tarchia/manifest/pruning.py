from typing import Any
from typing import List
from typing import Tuple

from .models import ManifestEntry


def prune(
    record: ManifestEntry, condition: List[Tuple[str, str, Any]]
) -> List[Tuple[str, str, Any]]:
    """
    Convert user-provided filters to manifest filters using min/max information.

    Parameters:
        user_filter (Tuple[str, str, Any]): User-provided filter in the form (column, operator, value).

    Returns:
        List[Tuple[str, str, Any]]: Converted filters suitable for manifest pruning.
    """

    for predicate in condition:
        column, op, value = predicate
        op = op.lower()

        if op == "eq":
            if record.lower_bounds.get(column) > value:
                return True
            if record.upper_bounds.get(column) < value:
                return True
        elif op == "gt":
            if record.upper_bounds.get(column) <= value:
                return True
        elif op == "gte":
            if record.upper_bounds.get(column) < value:
                return True
        elif op == "lt":
            if record.lower_bounds.get(column) >= value:
                return True
        elif op == "lte":
            if record.lower_bounds.get(column) > value:
                return True
        elif op == "inlist" and isinstance(value, list):
            min_value, max_value = min(value), max(value)
            if record.lower_bounds.get(column) > max_value:
                return True
            if record.upper_bounds.get(column) < min_value:
                return True

        return False


# Example usage
user_filters = [
    ("age", "=", 18),
    ("salary", ">", 50000),
    ("height", ">=", 170),
    ("experience", "<", 5),
    ("rating", "<=", 4.5),
    ("score", "in", [1, 2, 3]),
]
