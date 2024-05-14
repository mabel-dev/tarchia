import json

import requests

# Set your Dremio Arctic API key and endpoint
API_KEY = "+bEyACkIRueL2b2l5buxf06CdID0tmZIjrQ9L0GcQIjtc4K00W5tpAKLh6Vplg=="
BASE_URL = "https://api.eu.dremio.cloud/v0/"

# Define headers for authentication
headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}


def create_table(catalog_name: str, table_name: str, schema: dict):
    """
    Create a new table in the specified Arctic catalog.

    Parameters:
        catalog_name: str
            The name of the Arctic catalog.
        table_name: str
            The name of the table to create.
        schema: dict
            The schema of the table.
    """
    url = f"{BASE_URL}/catalogs/{catalog_name}/tables"
    payload = {"name": table_name, "schema": schema}
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 201:
        print(f"Table '{table_name}' created successfully in catalog '{catalog_name}'.")
    else:
        raise Exception(f"Failed to create table: {response.status_code} - {response.text}")


def list_tables(catalog_name: str):
    """
    List all tables in the specified Arctic catalog.

    Parameters:
        catalog_name: str
            The name of the Arctic catalog.
    """
    url = f"{BASE_URL}/catalogs/{catalog_name}/tables"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        tables = response.json()
        return tables
    else:
        raise Exception(f"Failed to list tables: {response.status_code} - {response.text}")


def get_table_metadata(catalog_name: str, table_name: str):
    """
    Get metadata for a specific table in the Arctic catalog.

    Parameters:
        catalog_name: str
            The name of the Arctic catalog.
        table_name: str
            The name of the table.
    """
    url = f"{BASE_URL}/catalogs/{catalog_name}/tables/{table_name}/metadata"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        metadata = response.json()
        return metadata
    else:
        raise Exception(f"Failed to get table metadata: {response.status_code} - {response.text}")


# Example usage
catalog_name = "my_catalog"
table_name = "my_table"

try:
    create_table("mabel", "planets", {})
    tables = list_tables(catalog_name)
    print(f"Tables in catalog '{catalog_name}': {tables}")

    metadata = get_table_metadata(catalog_name, table_name)
    print(f"Metadata for table '{table_name}': {json.dumps(metadata, indent=4)}")
except Exception as e:
    print(e)
