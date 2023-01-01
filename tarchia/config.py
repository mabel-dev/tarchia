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

from pathlib import Path
from os import environ

import yaml

config: dict = {}
try:  # pragma: no cover
    _config_path = Path(".") / "tarchia.yaml"
    if _config_path.exists():
        with open(_config_path, "rb") as _config_file:
            config = yaml.safe_load(_config_file)
        for key, value in config.items():
            config["key"] = environ.get(key, value)
        print(f"loaded config from {_config_path}")
except Exception as exception:  # pragma: no cover # it doesn't matter why - just use the defaults
    print(f"config file {_config_path} not used - {exception}")


# fmt:off

# GCP project ID - for Google Cloud Data
GCP_PROJECT_ID: str = config.get("GCP_PROJECT_ID")

METASTORE_HOST: str = config.get("METASTORE_HOST", "json")
DATASET_COLLECTION_NAME: str = config.get("DATASET_COLLECTION_NAME", "opteryx_data_catalogue")
BLOB_COLLECTION_NAME: str = config.get("BLOB_COLLECTION_NAME", "opteryx_blobs")
# fmt:on
