"""
Schema Update Validator

This module provides functions to validate updates to database schemas based on specific rules.
The following types of schema transitions are supported and validated:

1. Adding Columns:
    - New columns must have a default value.
    - There should be no collisions in column names or aliases.

2. Renaming Columns:
    - Renamed columns must reference unique columns.
    - No column name or alias can be duplicated.

3. Type Changes:
    - Safe type transitions are allowed as defined in the SAFE_TYPE_TRANSITIONS dictionary.
    - Current safe transitions include:
        * INTEGER to DOUBLE
        * BOOLEAN to INTEGER
        * DATE to TIMESTAMP

Functions:
    - get_all_names_and_aliases(columns: List[Column]) -> List[str]:
        Extract all column names and their aliases from a list of columns.

    - check_uniqueness(names: List[str]) -> bool:
        Check if all names in the list are unique.

    - validate_added_columns(current_schema: Schema, updated_schema: Schema) -> bool:
        Validate the added columns in the updated schema.

    - validate_column_renaming(current_schema: Schema, updated_schema: Schema) -> bool:
        Validate the renaming of columns in the updated schema.

    - validate_type_changes(current_schema: Schema, updated_schema: Schema) -> bool:
        Validate the type changes in the updated schema.

    - validate_schema_update(current_schema: Schema, updated_schema: Schema) -> bool:
        Validate the transition from the current schema to the updated schema.

Exceptions:
    - InvalidSchemaTransitionError:
        Raised for invalid schema transitions.
"""

from typing import List

from orso.schema import OrsoTypes

from tarchia.exceptions import InvalidSchemaTransitionError
from tarchia.models import Column
from tarchia.models import Schema

# Define safe type transitions
SAFE_TYPE_TRANSITIONS = {
    OrsoTypes.INTEGER: {OrsoTypes.DOUBLE},
    OrsoTypes.BOOLEAN: {OrsoTypes.INTEGER},
    OrsoTypes.DATE: {OrsoTypes.TIMESTAMP},
}


def get_all_names_and_aliases(columns: List[Column]) -> List[str]:
    """
    Extract all column names and their aliases from a list of columns.

    Parameters:
        columns (List[Column]): The list of columns.

    Returns:
        List[str]: A list containing all names and aliases.
    """
    names_and_aliases = []
    for column in columns:
        names_and_aliases.append(column.name)
        names_and_aliases.extend(column.aliases)
    return names_and_aliases


def check_uniqueness(names: List[str]) -> bool:
    """
    Check if all names in the list are unique.

    Parameters:
        names (List[str]): The list of names and aliases.

    Returns:
        bool: True if all names are unique, False otherwise.
    """
    return len(names) == len(set(names))


def validate_added_columns(current_schema: Schema, updated_schema: Schema) -> bool:
    """
    Validate the added columns in the updated schema.

    Parameters:
        current_schema (Schema): The current schema.
        updated_schema (Schema): The updated schema.

    Returns:
        bool: True if the added columns are valid, raises an exception otherwise.
    """
    current_names_and_aliases = get_all_names_and_aliases(current_schema.columns)
    updated_names_and_aliases = get_all_names_and_aliases(updated_schema.columns)

    # Check for name and alias collisions
    if not check_uniqueness(updated_names_and_aliases):
        raise InvalidSchemaTransitionError(
            "Name or alias collision detected in the updated schema."
        )

    current_columns = {col.name: col for col in current_schema.columns}
    updated_columns = {col.name: col for col in updated_schema.columns}

    added_columns = set(updated_columns) - set(current_columns)
    for col_name in added_columns:
        if updated_columns[col_name].default is None:
            raise InvalidSchemaTransitionError(
                f"New column '{col_name}' must have a default value."
            )

    return True


def validate_column_renaming(current_schema: Schema, updated_schema: Schema) -> bool:
    """
    Validate the renaming of columns in the updated schema.

    Parameters:
        current_schema (Schema): The current schema.
        updated_schema (Schema): The updated schema.

    Returns:
        bool: True if the renaming is valid, raises an exception otherwise.
    """
    current_columns = {col.name: col for col in current_schema.columns}
    updated_columns = {col.name: col for col in updated_schema.columns}

    current_names = set(current_columns.keys())
    updated_names = set(updated_columns.keys())

    renamed_columns = {}
    for old_name in current_names:
        if old_name not in updated_names:
            # Column has been renamed or removed
            for new_name, new_column in updated_columns.items():
                if old_name in new_column.aliases:
                    if new_name in renamed_columns:
                        raise InvalidSchemaTransitionError(
                            f"Column '{new_name}' cannot alias multiple columns: {renamed_columns[new_name]} and {old_name}."
                        )
                    renamed_columns[new_name] = old_name
                    break

    # Check that each renamed column references a unique column
    if len(renamed_columns) != len(set(renamed_columns.values())):
        raise InvalidSchemaTransitionError("Renamed columns must reference unique columns.")

    return True


def validate_type_changes(current_schema: Schema, updated_schema: Schema) -> bool:
    """
    Validate the type changes in the updated schema.

    Parameters:
        current_schema (Schema): The current schema.
        updated_schema (Schema): The updated schema.

    Returns:
        bool: True if the type changes are valid, raises an exception otherwise.
    """
    current_columns = {col.name: col for col in current_schema.columns}
    updated_columns = {col.name: col for col in updated_schema.columns}

    for col_name in current_columns:
        if col_name in updated_columns:
            old_type = current_columns[col_name].type
            new_type = updated_columns[col_name].type
            if old_type != new_type and (
                old_type not in SAFE_TYPE_TRANSITIONS
                or new_type not in SAFE_TYPE_TRANSITIONS[old_type]
            ):
                raise InvalidSchemaTransitionError(
                    f"Invalid type change for column '{col_name}' from {old_type} to {new_type}."
                )

    return True


def validate_schema_update(current_schema: Schema, updated_schema: Schema) -> bool:
    """
    Validate the transition from the current schema to the updated schema.

    Parameters:
        current_schema (Schema): The current schema.
        updated_schema (Schema): The updated schema.

    Returns:
        bool: True if the transition is valid, raises an exception otherwise.
    """

    validate_added_columns(current_schema, updated_schema)
    validate_column_renaming(current_schema, updated_schema)
    validate_type_changes(current_schema, updated_schema)

    return True
