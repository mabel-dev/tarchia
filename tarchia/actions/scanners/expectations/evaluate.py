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

import typing

from data_expectations import Expectations
from data_expectations.errors import ExpectationNotMetError
from data_expectations.errors import ExpectationNotUnderstoodError

ALL_EXPECTATIONS = Expectations.all_expectations()


def evaluate_record(
    expectations: Expectations, record: dict, suppress_errors: bool = False
) -> bool:
    """
    Test a single record against a defined set of expectations.

    Args:
        expectations: The Expectations instance.
        record: The dictionary record to be tested.
        all_expectations: The dictionary of all available expectations.
        suppress_errors: Whether to suppress expectation errors and return False instead.

    Returns:
        True if all expectations are met, False otherwise.
    """
    for expectation_definition in expectations.set_of_expectations:
        # get the name of the expectation
        expectation = expectation_definition.expectation

        if expectation not in ALL_EXPECTATIONS:
            raise ExpectationNotUnderstoodError(expectation=expectation)

        base_config = {
            "row": record,
            "column": expectation_definition.column,
            **expectation_definition.config,
        }

        if not ALL_EXPECTATIONS[expectation](**base_config):
            if not suppress_errors:
                raise ExpectationNotMetError(expectation, record)
            return False  # data failed to meet expectation

    return True


def evaluate_list(
    expectations: Expectations, dictset: typing.Iterable[dict], suppress_errors: bool = False
) -> bool:
    """
    Evaluate a set of records against a defined set of Expectations.

    Args:
        expectations: The Expectations instance.
        dictset: The iterable set of dictionary records to be tested.
        suppress_errors: Whether to suppress expectation errors and return False for the entire set.

    Returns:
        True if all records meet all Expectations, False otherwise.
    """
    return all(evaluate_record(expectations, record, suppress_errors) for record in dictset)
