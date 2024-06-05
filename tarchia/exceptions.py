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

from typing import Optional


class MissingDependencyError(Exception):  # pragma: no cover
    def __init__(self, dependency: str):
        self.dependency = dependency
        message = f"No module named '{dependency}' can be found, please install or include in requirements.txt"
        super().__init__(message)


class InvalidConfigurationError(Exception):
    def __init__(self, setting: str, source: Optional[str] = None):
        self.setting = setting
        self.source = source
        if source:
            message = (
                f"Configuration value for {setting} in {source} does not contain a valid value"
            )
        else:
            message = f"Configuration value for {setting} does not contain a valid value"
        super().__init__(message)


class InvalidFilterError(Exception):
    def __init__(self, message: str):
        super().__init__(message)
