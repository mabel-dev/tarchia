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

from typing import List
from typing import Optional


class MissingDependencyError(Exception):  # pragma: no cover
    def __init__(self, dependency: str):
        self.dependency = dependency
        message = f"No module named '{dependency}' can be found, please install or include in requirements.txt"
        super().__init__(message)


class InvalidConfigurationError(Exception):  # pragma: no cover
    def __init__(self, setting: str, source: Optional[str] = None):
        self.setting = setting
        self.source = source
        if source:
            message = (
                f"Configuration value for {setting} in {source} does not contain a valid value."
            )
        else:
            message = f"Configuration value for {setting} does not contain a valid value."
        super().__init__(message)


class InvalidFilterError(Exception):  # pragma: no cover
    """Exception raised when filters are not formatted correctly"""


class UnmetRequirementError(Exception):  # pragma: no cover
    """Exception raised when a requirement for operation is not met."""


class DataEntryError(Exception):  # pragma: no cover
    def __init__(self, endpoint: str, fields: List[str], message: str):
        self.endpoint = endpoint
        self.fields = fields
        self.message = message

        super().__init__(message)


class TableNotFoundError(Exception):  # pragma: no cover
    def __init__(self, owner: str, table: str):
        self.owner = owner
        self.table = table

        message = f"Table with reference {owner}.{table} could not be found."
        super().__init__(message)


class OwnerNotFoundError(Exception):  # pragma: no cover
    def __init__(self, owner: str):
        self.owner = owner

        message = f"Owner with reference {owner} could not be found."
        super().__init__(message)


class AmbiguousTableError(Exception):  # pragma: no cover
    def __init__(self, table: str):
        self.table = table

        message = f"Multiple tables with reference {table} were found."
        super().__init__(message)


class AlreadyExistsError(Exception):  # pragma: no cover
    def __init__(self, entity: str):
        self.entity = entity

        message = f"Item with name {entity} already exists."
        super().__init__(message)


class TableHasNoDataError(Exception):  # pragma: no cover
    def __init__(self, owner: str, table: str, as_at: Optional[int] = None):
        self.table = table
        self.as_at = as_at

        if not as_at:
            message = f"Table with reference {table} is empty."
        else:
            message = f"Table with reference {table} was empty at {as_at}."

        super().__init__(message)


class TransactionError(Exception):  # pragma: no cover
    pass
