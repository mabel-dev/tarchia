from typing import Any
from typing import List
from typing import Optional
from typing import Tuple

from .models import ManifestEntry


def parse_filters(filter_string: Optional[str] = None) -> List[Tuple[str, str, str]]:
    """
    Parse a filter string into a list of tuples.

    Parameters:
        filter_string: str - The filter string in the format 'column=value, column>value, ...'

    Returns:
        List[Tuple[str, str, str]]: A list of tuples containing (column, operator, value).
    """
    if filter_string is None:
        return None

    operators = ["=", ">", ">=", "<", "<=", "_in_"]
    filters = []

    for item in filter_string.split(","):
        for operator in operators:
            if operator in item:
                column, value = item.split(operator, 1)
                filters.append((column.strip(), operator, value.strip()))
                break

    return filters


def prune(record: ManifestEntry, condition: List[Tuple[str, str, Any]]) -> bool:
    """
    Convert user-provided filters to manifest filters using min/max information.

    Parameters:
        user_filter (Tuple[str, str, Any]): User-provided filter in the form (column, operator, value).

    Returns:
        bool: True to prune the record
    """
    # TODO: implement pruning
    return False

    for predicate in condition:
        column, op, value = predicate
        try:
            value = type.parse(value)
        except:
            return False

        if op == "=":
            if record.lower_bounds.get(column) > value:
                return True
            if record.upper_bounds.get(column) < value:
                return True
        elif op == ">":
            if record.upper_bounds.get(column) <= value:
                return True
        elif op == ">=":
            if record.upper_bounds.get(column) < value:
                return True
        elif op == "<":
            if record.lower_bounds.get(column) >= value:
                return True
        elif op == "<=":
            if record.lower_bounds.get(column) > value:
                return True
        elif op == "_in_" and isinstance(value, list):
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
