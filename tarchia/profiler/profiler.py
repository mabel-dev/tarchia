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

from opteryx import connectors
from op

def reader_factory(path):

    protocols = {
        "gcs": connectors.GcpCloudStorageConnector,
        "s3": connectors.AwsS3Connector,
        "file": connectors.DiskConnector
    }

    protocol = path.split("://")[0]
    return protocols.get(protocol, connectors.DiskConnector)


def just_a_test(blob_name):
    reader_class = reader_factory(blob_name)
    reader = reader_class()
    blob_bytes = reader.read_blob(blob_name)

    # the the blob filename extension
    extension = blob_name.split(".")[-1]

    # find out how to read this blob
    decoder, file_type = KNOWN_EXTENSIONS.get(extension, (None, None))
    # get the extenstion
    # get the appropriate reader (opteryx.utils.file_decoders)