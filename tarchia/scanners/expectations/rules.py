# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Inspired by the Great Expectations library.

Rather than testing for conformity through defining a schema, expectations are a set
of assertions we can apply to our data.

Whilst a schema-based approach isn't exactly procedural, expectations are a more
declarative way to define valid data.

These assertions can also define a schema (we can expect a set of columns, each with
an expected type), but they also allow us to have more complex assertions, such as
the values in a set of columns should add to 100, or the values in a column are
increasing.

This is designed to be applied to streaming data as each record passes through a point
in a flow - as such it is not intended to test an entire dataset at once to test its
validity, and some assertions are impractical - for example an expectation of the mean
of all of the values in a table.

- if data doesn't match, I'm not cross, I'm just disappointed.
"""

import json
import re
from dataclasses import is_dataclass
from inspect import getmembers
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Union

from data_expectations.internals.models import Expectation
from data_expectations.internals.text import sql_like_to_regex

GLOBAL_TRACKER: Dict[str, Any] = {}


def track_previous(func):
    def wrapper(*args, **kwargs):
        column = kwargs.get("column")
        key = f"{func.__name__}/{str(column)}"
        if "previous_value" in kwargs:
            previous_value = kwargs.pop("previous_value")
        else:
            previous_value = GLOBAL_TRACKER.get(key)
        result = func(previous_value=previous_value, *args, **kwargs)
        GLOBAL_TRACKER[key] = kwargs.get("row", {}).get(column) or previous_value
        return result

    return wrapper


class Expectations:
    def __init__(self, set_of_expectations: Iterable[Union[str, dict, Expectation]]):
        self.set_of_expectations: List[Expectation] = []
        for exp in set_of_expectations:
            if isinstance(exp, str):  # Parse JSON string
                exp = json.loads(exp)

            if isinstance(exp, dict):  # Convert dict to Expectation
                self.set_of_expectations.append(Expectation.load(exp))
            elif is_dataclass(exp) and isinstance(exp, Expectation):
                self.set_of_expectations.append(exp)

    @classmethod
    def all_expectations(cls):
        """
        Programmatically get the list of expectations and build them into a dictionary.
        We then use this dictionary to look up the methods to test the expectations in
        the set of expectations for a dataset.
        """
        expectations = {}
        for handle, member in getmembers(cls):
            if callable(member) and handle.startswith("expect_"):
                expectations[handle] = member
        return expectations

    @staticmethod
    def reset():
        global GLOBAL_TRACKER
        GLOBAL_TRACKER = {}

    ###################################################################################
    # COLUMN EXPECTATIONS
    ###################################################################################

    @staticmethod
    def expect_column_to_exist(
        *,
        row: dict,
        column: str,
        **kwargs,
    ):
        """
        Confirms that a specified column exists in the record.

        Parameters:
            row: dict
                The record to be checked.
            column: str
                Name of the column to check for existence.

        Returns: bool
            True if column exists, False otherwise.
        """
        if isinstance(row, dict):
            return column in row
        return False

    @staticmethod
    def expect_column_values_to_not_be_null(
        *,
        row: dict,
        column: str,
        **kwargs,
    ):
        """
        Confirms that the value in a column is not null. Non-existent values are considered null.

        Parameters:
            row: dict
                The record containing the column.
            column: str
                The column's name whose value should not be null.

        Returns: bool
            True if the value in the column is not null, False otherwise.
        """
        return row.get(column) is not None

    @staticmethod
    def expect_column_values_to_be_of_type(
        *,
        row: dict,
        column: str,
        expected_type,
        ignore_nulls: bool = True,
        **kwargs,
    ):
        """
        Confirms that the value in a specific column is of the expected type.

        Parameters:
            row: dict
                The record to be checked.
            column: str
                The column's name to validate the type of its value.
            expected_type:
                Expected type of the column value.
            ignore_nulls: bool
                If True, null values will not cause the expectation to fail.

        Returns: bool
            True if the type matches or if the value is null and ignore_nulls is True, False otherwise.
        """
        value = row.get(column)
        if value is not None:
            return type(value).__name__ == expected_type
        return ignore_nulls

    @staticmethod
    def expect_column_values_to_be_in_type_list(
        *,
        row: dict,
        column: str,
        type_list: Iterable,
        ignore_nulls: bool = True,
        **kwargs,
    ):
        """
        Confirms that the type of value in a specific column is one of the specified types.

        Parameters:
            row: dict
                The record to validate.
            column: str
                The column's name to validate the type of its value.
            type_list: Iterable
                List of expected types for the column value.
            ignore_nulls: bool
                If True, null values will not cause the expectation to fail.

        Returns: bool
            True if the type is in the type list or if the value is null and ignore_nulls is True, False otherwise.
        """
        value = row.get(column)
        if value is not None:
            return type(value).__name__ in type_list
        return ignore_nulls

    @staticmethod
    def expect_column_values_to_be_between(
        *,
        row: dict,
        column: str,
        minimum,
        maximum,
        ignore_nulls: bool = True,
        **kwargs,
    ):
        """
        Confirms that the value in a specific column is between two values.

        Parameters:
            row: dict
                The record to check.
            column: str
                The column's name to validate its value.
            minimum:
                Lower bound of the value.
            maximum:
                Upper bound of the value.
            ignore_nulls: bool
                If True, null values will not cause the expectation to fail.

        Returns: bool
            True if the value is between the two bounds or if the value is null and ignore_nulls is True, False otherwise.
        """
        value = row.get(column)
        if value is not None:
            return value >= minimum and value <= maximum
        return ignore_nulls

    @staticmethod
    @track_previous
    def expect_column_values_to_be_increasing(
        *,
        row: dict,
        column: str,
        ignore_nulls: bool = True,
        previous_value=None,
        **kwargs,
    ):
        """
        Confirms that the values in a specific column are in an increasing order.

        Parameters:
            row: dict
                The record to validate.
            column: str
                The column's name to validate its value.
            ignore_nulls: bool
                If True, null values will not cause the expectation to fail.
            previous_value: [type]
                The value of the column from the previous record.

        Returns: bool
            True if the current value is greater than or equal to the previous value or if the value is null and ignore_nulls is True. False otherwise.
        """
        value = row.get(column)
        if value is not None:
            return previous_value is None or previous_value <= value
        return ignore_nulls

    @staticmethod
    @track_previous
    def expect_column_values_to_be_decreasing(
        *,
        row: dict,
        column: str,
        ignore_nulls: bool = True,
        previous_value=None,
        **kwargs,
    ):
        """
        Confirms that the values in a specific column are in a decreasing order.

        Parameters:
            row: dict
                The record to validate.
            column: str
                The column's name to validate its value.
            ignore_nulls: bool
                If True, null values will not cause the expectation to fail.
            previous_value: [type]
                The value of the column from the previous record.

        Returns: bool
            True if the current value is less than or equal to the previous value or if the value is null and ignore_nulls is True. False otherwise.
        """
        value = row.get(column)
        if value is not None:
            return previous_value is None or previous_value >= value
        return ignore_nulls

    @staticmethod
    def expect_column_values_to_be_in_set(
        *,
        row: dict,
        column: str,
        symbols: Iterable,
        ignore_nulls: bool = True,
        **kwargs,
    ):
        """
        Confirms that the value in a specific column is within a predefined set.

        Parameters:
            row: dict
                The record to check.
            column: str
                The column's name to validate its value.
            symbols: Iterable
                The set of allowed values for the column.
            ignore_nulls: bool
                If True, null values will not cause the expectation to fail.

        Returns: bool
            True if the value is in the provided set or if the value is null and ignore_nulls is True, False otherwise.
        """
        value = row.get(column)
        if value is not None:
            return value in symbols
        return ignore_nulls

    @staticmethod
    def expect_column_values_to_match_regex(
        *,
        row: dict,
        column: str,
        regex: str,
        ignore_nulls: bool = True,
        **kwargs,
    ):
        """
        Confirms that the value in a specific column matches a given regular expression pattern.

        Parameters:
            row: dict
                The record to validate.
            column: str
                The column's name to validate its value.
            regex: str
                The regular expression pattern to match against the column's value.
            ignore_nulls: bool
                If True, null values will not cause the expectation to fail.

        Returns: bool
            True if the value matches the regex or if the value is null and ignore_nulls is True, False otherwise.
        """
        value = row.get(column)
        if value is not None:
            return re.compile(regex).match(str(value)) is not None
        return ignore_nulls

    @staticmethod
    def expect_column_values_to_match_like(
        *,
        row: dict,
        column: str,
        like: str,
        ignore_nulls: bool = True,
        **kwargs,
    ):
        """
        Confirms that the value in a specific column matches a given SQL-like pattern.

        Parameters:
            row: dict
                The record to validate.
            column: str
                The column's name to validate its value.
            like: str
                The SQL-like pattern to match against the column's value.
            ignore_nulls: bool
                If True, null values will not cause the expectation to fail.

        Returns: bool
            True if the value matches the pattern or if the value is null and ignore_nulls is True, False otherwise.
        """
        value = row.get(column)
        if value is not None:
            return sql_like_to_regex(like).match(str(value)) is not None
        return ignore_nulls

    @staticmethod
    def expect_column_values_length_to_be(
        *,
        row: dict,
        column: str,
        length: int,
        ignore_nulls: bool = True,
        **kwargs,
    ):
        """
        Confirms that the length of the value in a specific column is equal to a specified length.

        Parameters:
            row: dict
                The record to validate.
            column: str
                The column's name to validate its value length.
            length: int
                The expected length for the column's value.
            ignore_nulls: bool
                If True, null values will not cause the expectation to fail.

        Returns: bool
            True if the length of the value matches the specified length or if the value is null and ignore_nulls is True, False otherwise.
        """
        value = row.get(column)
        if value is not None:
            if not hasattr(value, "__len__"):
                value = str(value)
            return len(value) == length
        return ignore_nulls

    @staticmethod
    def expect_column_values_length_to_be_between(
        *,
        row: dict,
        column: str,
        minimum: int,
        maximum: int,
        ignore_nulls: bool = True,
        **kwargs,
    ):
        """
        Confirms that the length of the value in a specific column falls within a specified range.

        Parameters:
            row: dict
                The record to validate.
            column: str
                The column's name to validate its value length.
            minimum: int
                The minimum acceptable length for the column's value.
            maximum: int
                The maximum acceptable length for the column's value.
            ignore_nulls: bool
                If True, null values will not cause the expectation to fail.

        Returns: bool
            True if the length of the value is within the specified range or if the value is null and ignore_nulls is True, False otherwise.
        """
        value = row.get(column)
        if value is not None:
            if not hasattr(value, "__len__"):
                value = str(value)
            return len(value) >= minimum and len(value) <= maximum
        return ignore_nulls

    @staticmethod
    def expect_column_values_to_be_more_than(
        *,
        row: dict,
        column: str,
        threshold,
        ignore_nulls: bool = True,
        **kwargs,
    ):
        """
        Confirms that the value in a specific column is greater than a given threshold.

        Parameters:
            row: dict
                The record to validate.
            column: str
                The column's name to validate its value.
            threshold: [type]
                The minimum acceptable value for the column.
            ignore_nulls: bool
                If True, null values will not cause the expectation to fail.

        Returns: bool
            True if the value is greater than the threshold or if the value is null and ignore_nulls is True, False otherwise.
        """
        value = row.get(column)
        if value is not None:
            return value > threshold
        return ignore_nulls

    @staticmethod
    def expect_column_values_to_be_less_than(
        *,
        row: dict,
        column: str,
        threshold,
        ignore_nulls: bool = True,
        **kwargs,
    ):
        """
        Confirms that the value in a specific column is less than a given threshold.

        Parameters:
            row: dict
                The record to validate.
            column: str
                The column's name to validate its value.
            threshold: [type]
                The maximum acceptable value for the column.
            ignore_nulls: bool
                If True, null values will not cause the expectation to fail.

        Returns: bool
            True if the value is less than the threshold or if the value is null and ignore_nulls is True, False otherwise.
        """
        value = row.get(column)
        if value is not None:
            return value < threshold
        return ignore_nulls
