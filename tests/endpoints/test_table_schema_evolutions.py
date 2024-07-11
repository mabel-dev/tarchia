"""

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

- cannot create a column with an invalid name
- cannot create an alias with an invalid name
- cannot create a table with an invalid name
"""