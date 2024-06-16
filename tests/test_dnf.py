import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import pytest
from tarchia.exceptions import InvalidFilterError
from tarchia.utils.dnf_filters import DnfFilters, evaluate

# Sample records for testing
record1 = {"name": "john", "age": 30}
record2 = {"name": "jane", "age": 25}
record3 = {"name": "john", "age": 40}
record4 = {"name": "tom", "age": 35, "salary": 80000}
record5 = {"name": "lucy", "age": 22, "salary": 60000}

# Additional test cases for evaluate function


def test_evaluate_range():
    assert evaluate([("age", ">=", 20), ("age", "<=", 40)], record1) == True
    assert evaluate([("age", ">=", 25), ("age", "<=", 35)], record2) == True
    assert evaluate([("age", ">=", 30), ("age", "<=", 50)], record3) == True
    assert evaluate([("age", ">=", 35), ("age", "<=", 45)], record4) == True
    assert evaluate([("age", ">=", 20), ("age", "<=", 21)], record5) == False


def test_evaluate_greater_than():
    assert evaluate(("salary", ">", 70000), record4) == True
    assert evaluate(("salary", ">", 90000), record4) == False
    assert evaluate(("salary", ">", 50000), record5) == True


def test_evaluate_less_than():
    assert evaluate(("age", "<", 25), record5) == True
    assert evaluate(("age", "<", 22), record5) == False
    assert evaluate(("age", "<", 40), record3) == False


def test_evaluate_greater_than_or_equal():
    assert evaluate(("age", ">=", 35), record4) == True
    assert evaluate(("age", ">=", 36), record4) == False
    assert evaluate(("age", ">=", 22), record5) == True


def test_evaluate_less_than_or_equal():
    assert evaluate(("age", "<=", 35), record4) == True
    assert evaluate(("age", "<=", 34), record4) == False
    assert evaluate(("age", "<=", 22), record5) == True


def test_dnf_filters_range():
    filters = DnfFilters([("age", ">=", 30), ("age", "<=", 40)])
    records = [record1, record2, record3, record4, record5]
    expected = [record1, record3, record4]
    assert list(filters.filter_dictset(records)) == expected


def test_dnf_filters_combined_conditions():
    filters = DnfFilters([("age", ">=", 22), ("salary", ">", 60000)])
    records = [record1, record2, record3, record4, record5]
    expected = record4
    assert next(filters.filter_dictset(records)) == expected


def test_dnf_filters_empty_record():
    filters = DnfFilters([("age", ">=", 30), ("age", "<=", 40)])
    records = [{}]
    expected = []
    assert list(filters.filter_dictset(records)) == expected


def test_dnf_filters_no_matching_records():
    filters = DnfFilters([("age", ">=", 50)])
    records = [record1, record2, record3, record4, record5]
    expected = []
    assert list(filters.filter_dictset(records)) == expected


def test_dnf_filters_call_with_range():
    filters = DnfFilters([("age", ">=", 30), ("age", "<=", 40)])
    assert filters(record1) == True
    assert filters(record2) == False
    assert filters(record3) == True
    assert filters(record4) == True
    assert filters(record5) == False


# Test cases for evaluate function
def test_evaluate_single_predicate():
    assert evaluate(("name", "==", "john"), record1) == True
    assert evaluate(("age", ">", 25), record1) == True
    assert evaluate(("age", "<", 25), record1) == False
    assert evaluate(("name", "!=", "john"), record1) == False
    assert evaluate(("name", "==", "jane"), record1) == False


def test_evaluate_and_condition():
    assert evaluate([("name", "==", "john"), ("age", ">", 25)], record1) == True
    assert evaluate([("name", "=", "john"), ("age", "<", 25)], record1) == False
    assert evaluate([("name", "==", "jane"), ("age", ">=", 25)], record2) == True


def test_evaluate_or_condition():
    assert evaluate([[("name", "==", "john")], [("age", ">", 25)]], record1) == True
    assert evaluate([[("name", "==", "john")], [("age", "<", 25)]], record1) == True
    assert evaluate([[("name", "==", "jane")], [("age", "<", 25)]], record1) == False


def test_evaluate_invalid_operator():
    with pytest.raises(InvalidFilterError):
        evaluate(("name", "===", "john"), record1)


def test_evaluate_invalid_structure():
    with pytest.raises(InvalidFilterError):
        evaluate([("name", "==", "john"), ["age", ">", 25]], record1)


# Test cases for DnfFilters class
def test_dnf_filters_empty():
    filters = DnfFilters()
    records = [record1, record2, record3]
    assert list(filters.filter_dictset(records)) == records


def test_dnf_filters_single_predicate():
    filters = DnfFilters([("name", "==", "john")])
    records = [record1, record2, record3]
    expected = [record1, record3]
    assert list(filters.filter_dictset(records)) == expected


def test_dnf_filters_and_condition():
    filters = DnfFilters([("name", "==", "john"), ("age", ">", 25)])
    records = [record1, record2, record3]
    expected = [record1, record3]
    assert list(filters.filter_dictset(records)) == expected


def test_dnf_filters_or_condition():
    filters = DnfFilters([[("name", "==", "john")], [("age", "<=", 25)]])
    records = [record1, record2, record3]
    expected = [record1, record2, record3]
    assert list(filters.filter_dictset(records)) == expected


def test_dnf_filters_call():
    filters = DnfFilters([("name", "==", "john")])
    assert filters(record1) == True
    assert filters(record2) == False



if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()

