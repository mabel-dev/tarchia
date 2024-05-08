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
Data Models
"""

from typing import List, Literal, Optional

from pydantic import BaseModel, Extra


class NewBlob(BaseModel):
    location: str
    provider: Optional[str] = None

class Expectation(BaseModel):
    class Config:
        extra = Extra.allow

    expect: str

class Field(BaseModel):
    class Config:
        extra = Extra.allow
    
    name: str
    aliases: List[str] = [],
    data_type: Literal["BOOLEAN", "LIST", "NUMERIC", "STRUCT", "TIMESTAMP", "VARCHAR"]
    expectations: List[Expectation] = []

class Dataset(BaseModel):
    class Config:
        extra = Extra.allow

    id: str
    record_type: str = "dataset"
    href: str = "https://tarchia.opteryx.app/datasets/12345"
    preferred_name: str
    canonical_name: str
    aliases: list = []
    columns: List[Field] = []
    expectations: List[Expectation] = []