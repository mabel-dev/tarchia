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

from os import environ
from pathlib import Path

import yaml

# python-dotenv allows us to create an environment file to store secrets. If
# there is no .env it will fail gracefully.
try:
    import dotenv  # type:ignore
except ImportError:  # pragma: no cover
    dotenv = None  # type:ignore

_env_path = Path(".") / ".env"
_config_values: dict = {}

#  deepcode ignore PythonSameEvalBinaryExpressiontrue: false +ve, values can be different
if _env_path.exists() and (dotenv is None):  # pragma: no cover  # nosemgrep
    # using a logger here will tie us in knots
    print("`.env` file exists but `dotEnv` not installed.")
elif dotenv is not None:  # pragma: no cover
    print("Loading environment variables from `.env`")
    dotenv.load_dotenv(dotenv_path=_env_path)

try:  # pragma: no cover
    _config_path = Path(".") / "opteryx.yaml"
    if _config_path.exists():
        with open(_config_path, "rb") as _config_file:
            _config_values = yaml.safe_load(_config_file)
        print(f"Loading config from {_config_path}")
except Exception as exception:  # pragma: no cover # it doesn't matter why - just use the defaults
    print(f"Config file {_config_path} not used - {exception}")


def get(key, default=None):
    value = environ.get(key)
    if value is None:
        value = _config_values.get(key, default)
    return value


# fmt:off
TARCHIA_DEBUG: str = bool(get("TARCHIA_DEBUG", "false"))
"""Flag to enable debug logging."""

CATALOG_PROVIDER: str = get("CATALOG_PROVIDER", "DEVELOPMENT")
"""The service providing the storage for the Catalog."""

CATALOG_NAME: str = get("CATALOG_NAME")
"""The name of the catalog collection/table"""

STORAGE_PROVIDER: str = get("STORAGE_PROVIDER", "local")
"""The service providing the storage for the metadata."""

METADATA_ROOT: str = get("METADATA_ROOT", "warehouse")
"""The root of the metadata store."""

TRANSACTION_SIGNER: str = get("TRANSACTION_SIGNER", "secret")
"""The key used to sign transactions."""

GCP_PROJECT_ID: str = get("GCP_PROJECT_ID") 
"""GCP Project ID - for Google Cloud Platform hosted systems"""

# fmt:on
