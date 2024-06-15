"""
This is a filtering mechanism to be applied when reading data.
"""

import operator
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from tarchia.exceptions import InvalidFilterError

# Convert text representation of operators to functions
FILTERABLE_OPERATORS = {
    "=": operator.eq,
    "==": operator.eq,
    "!=": operator.ne,
    "<>": operator.ne,
    "<": operator.lt,
    ">": operator.gt,
    "<=": operator.le,
    ">=": operator.ge,
}


def evaluate_tuple(predicate: Tuple[str, str, Any], record: Dict[str, Any]) -> bool:
    """
    Evaluate a single predicate tuple against a record.

    Parameters:
        predicate (Tuple[str, str, Any]): A tuple in the form (key, op, value).
        record (Dict[str, Any]): The record to evaluate.

    Returns:
        bool: The result of the predicate evaluation.
    """
    key, op, value = predicate
    record_value = record.get(key)
    if record_value is None:
        return False
    operator_func = FILTERABLE_OPERATORS.get(op)
    if not operator_func:
        raise InvalidFilterError(f"Invalid operator '{op}' in predicate")
    return operator_func(record_value, value)


def evaluate(
    predicate: Union[Tuple[str, str, Any], List[Union[Tuple[str, str, Any], list]]],
    record: Dict[str, Any],
) -> bool:
    """
    Evaluate a predicate in DNF against a record.

    Parameters:
        predicate (Union[Tuple, List[Union[Tuple, list]]]): The predicate to evaluate.
        record (Dict[str, Any]): The record to evaluate.

    Returns:
        bool: The result of the DNF evaluation.
    """
    if predicate is None:  # No filter doesn't filter
        return True

    if isinstance(predicate, tuple):
        return evaluate_tuple(predicate, record)

    if isinstance(predicate, list):
        if all(isinstance(p, tuple) for p in predicate):
            return all(evaluate_tuple(p, record) for p in predicate)
        if all(isinstance(p, list) for p in predicate):
            return any(evaluate(p, record) for p in predicate)
        raise InvalidFilterError("Invalid structure in filter: mixed types in list")

    raise InvalidFilterError("Invalid structure in filter: expected tuple or list")


class DnfFilters:
    __slots__ = ("empty_filter", "predicates")

    def __init__(self, filters: Optional[List[Tuple[str, str, Any]]] = None):
        """
        A class to support filtering data.

        Parameters:
            filters (Optional[List[Tuple[str, str, Any]]]): List of predicates.
        """
        self.empty_filter = filters is None
        self.predicates = filters if filters else []

    def filter_dictset(self, dictset: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
        """
        Filter an iterable of dictionaries.

        Parameters:
            dictset (Iterable[Dict[str, Any]]): The dictset to process.

        Yields:
            Dict[str, Any]: Filtered dictionaries.
        """
        if self.empty_filter:
            yield from dictset
        else:
            yield from (record for record in dictset if evaluate(self.predicates, record))

    def __call__(self, record: Dict[str, Any]) -> bool:
        """
        Evaluate a single record against the filters.

        Parameters:
            record (Dict[str, Any]): The record to evaluate.

        Returns:
            bool: The result of the evaluation.
        """
        return evaluate(self.predicates, record)
