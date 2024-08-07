from typing import Any
from typing import List
from typing import Tuple

from tarchia.models import Schema
from tarchia.models.manifest_models import ManifestEntry
from tarchia.utils.to_int import to_int


def parse_value(field: str, value: Any, schema: Schema) -> int:
    for column in schema.columns:
        if column.name == field:
            value = column.type.parse(value)
            return to_int(value)
    return None


def parse_filters(filter_string: str, schema: Schema) -> List[Tuple[str, str, int]]:
    """
    Parse a filter string into a list of tuples.

    Parameters:
        filter_string: str - The filter string in the format 'column=value, column>value, ...'

    Returns:
        List[Tuple[str, str, str]]: A list of tuples containing (column, operator, value).
    """
    if filter_string is None:
        return None

    operators = ("=", ">", ">=", "<", "<=")
    filters = []

    for item in filter_string.split(","):
        for operator in operators:
            if operator in item:
                column, value = map(str.strip, item.split(operator, 1))
                if value and value[0] == value[-1] == "'":
                    value = value[1:-1]
                int_value = parse_value(column, value, schema)
                if int_value is not None:
                    filters.append((column, operator, int_value))
                break

    return filters


def prune(record: ManifestEntry, condition: List[Tuple[str, str, int]]) -> bool:
    """
    Convert user-provided filters to manifest filters using min/max information.

    Parameters:
        user_filter (Tuple[str, str, int]): User-provided filter in the form (column, operator, value).

    Returns:
        bool: True to prune the record
    """

    for column, op, value in condition:
        lower_bound = record.lower_bounds.get(column)
        upper_bound = record.upper_bounds.get(column)

        if op == "=" and (lower_bound > value or upper_bound < value):
            return True
        if op in (">", ">=") and upper_bound < value:
            return True
        if op in ("<", "<=") and lower_bound > value:
            return True

    return False
