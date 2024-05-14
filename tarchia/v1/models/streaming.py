"""
{
  "dataset_name": "proxy_event_logs",
  "dataset_type": "streaming_microbatch",
  "batch_details": {
    "batch_interval": "1 hour",
    "last_batch_id": "202305131300",
    "next_expected_batch": "202305131400"
  },
  "location_details": {
    "bucket_url": "gs://your-bucket/proxy-logs",
    "path_pattern": "/year={year}/month={month}/day={day}/hour={hour}",
    "file_pattern": "log_{batch_id}.parquet"
  },
  "data_access": {
    "access_policy": "Role-based access control",
    "encryption": "Google-managed keys"
  },
  "data_characteristics": {
    "data_format": "Parquet",
    "update_mechanism": "Append new files per batch"
  },
  "indexing_discovery": {
    "indexed_fields": ["timestamp", "event_type"],
    "discovery_service": "Google Data Catalog"
  },
  "created_at": "2023-01-01T00:00:00Z",
  "last_updated": "2023-05-13T12:30:00Z"
}

"""
