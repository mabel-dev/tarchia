"""
{
  "format-version": 1,
  "table-uuid": "d6c6f4d8-2d4b-4c7b-ae8d-8b1d5e5f0f0f",
  "location": "s3://bucket/path/to/table",
  "last-updated-ms": 1627891234567,
  "last-column-id": 4,
  "schema": {
    "type": "struct",
    "schema-id": 0,
    "fields": [
      {
        "id": 1,
        "name": "id",
        "required": true,
        "type": "long"
      },
      {
        "id": 2,
        "name": "data",
        "required": false,
        "type": "string"
      }
    ]
  },
  "partition-spec": [
    {
      "spec-id": 0,
      "fields": [
        {
          "name": "data_bucket",
          "transform": "bucket[16]",
          "source-id": 2,
          "field-id": 1000
        }
      ]
    }
  ],
  "default-sort-order": {
    "order-id": 0,
    "fields": []
  },
  "snapshots": [
    {
      "snapshot-id": 3055720921850357000,
      "parent-snapshot-id": null,
      "timestamp-ms": 1627891234567,
      "manifest-list": "s3://bucket/path/to/manifest/list.avro",
      "summary": {
        "operation": "append"
      },
      "schema-id": 0
    }
  ],
  "current-snapshot-id": 3055720921850357000,
  "snapshot-log": [
    {
      "snapshot-id": 3055720921850357000,
      "timestamp-ms": 1627891234567
    }
  ],
  "metadata-log": [
    {
      "metadata-file": "s3://bucket/path/to/metadata/metadata-1.json",
      "timestamp-ms": 1627891234567
    }
  ]
}
"""
