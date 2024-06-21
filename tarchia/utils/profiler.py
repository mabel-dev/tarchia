from copy import deepcopy
from dataclasses import asdict
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

import numpy
from orso.schema import FlatColumn
from orso.types import OrsoTypes

INFINITY = float("inf")
SIXTY_FOUR_BITS: int = 8
SIXTY_FOUR_BYTES: int = 64
MAX_INT64: int = 9223372036854775807


def string_to_int64(value: str) -> int:
    """Convert the first 8 characters of a string to an integer representation.

    Parameters:
        value: str
            The string value to be converted.

    Returns:
        An integer representation of the first 8 characters of the string.
    """
    byte_value = (value + "\x00" * 8)[:SIXTY_FOUR_BITS].encode("utf-8")
    int_value = int.from_bytes(byte_value, "big")
    return int_value if int_value <= MAX_INT64 else MAX_INT64


def int64_to_string(value: int) -> str:
    # Convert the integer back to 8 bytes using big-endian byte order
    if value >= MAX_INT64:
        return None

    byte_value = value.to_bytes(SIXTY_FOUR_BITS, "big")

    # Decode the byte array back to a UTF-8 string
    # You might need to strip any padding characters that were added when encoding
    string_value = byte_value.decode("utf-8").rstrip("\x00")

    return string_value


@dataclass
class ColumnProfile:
    name: str
    type: OrsoTypes
    count: int = 0
    missing: int = 0
    maximum: Optional[int] = None
    minimum: Optional[int] = None

    def deep_copy(self):
        """Create a deep copy of the Profile instance."""
        return deepcopy(self)

    def __add__(self, profile: "ColumnProfile") -> "ColumnProfile":
        new_profile = self.deep_copy()
        new_profile.count += profile.count
        new_profile.missing += profile.missing
        new_profile.minimum = min([self.minimum or INFINITY, profile.minimum or INFINITY])
        if new_profile.minimum == INFINITY:
            new_profile.minimum = None
        new_profile.maximum = max([self.maximum or -INFINITY, profile.maximum or -INFINITY])
        if new_profile.maximum == -INFINITY:
            new_profile.maximum = None

        return new_profile

    def serialize(self) -> Dict[str, Any]:
        """Serialize the profile to a dictionary."""
        return {
            "null_value_counts": {self.name: self.missing},
            "lower_bounds": {self.name: self.minimum},
            "upper_bounds": {self.name: self.maximum},
        }


class TableProfile:
    def __init__(self):
        self._columns: List[ColumnProfile] = []
        self._column_names: List[str] = []

    def __add__(self, right_profile: "TableProfile") -> "TableProfile":
        """Add two TableProfile instances."""
        new_profile = TableProfile()
        for column_name in self._column_names:
            left_column = self.column(column_name)
            right_column = right_profile.column(column_name)
            if not right_column:
                right_column = ColumnProfile(
                    column_name, left_column.type, left_column.count, left_column.count
                )
            new_profile.add_column(left_column + right_column, column_name)
        return new_profile

    def add_column(self, profile: ColumnProfile, name: str):
        """Add a column profile to the table profile."""
        self._columns.append(profile)
        self._column_names.append(name)

    def __iter__(self):
        """An iterator over columns"""
        return iter(self.column)

    def column(self, i: Union[int, str]) -> Union[ColumnProfile, None]:
        """Get a column by its name or index"""
        if isinstance(i, str):
            for name, column in zip(self._column_names, self._columns):
                if name == i:
                    return column
            return None

        if isinstance(i, int):
            return self._columns[i]

    def to_dicts(self) -> List[dict]:
        return [asdict(v) for v in self._columns]

    def to_arrow(self) -> "pyarrow.Table":
        import pyarrow

        return pyarrow.Table.from_pylist(self.to_dicts())

    def to_dataframe(self) -> "DataFrame":
        import orso

        return orso.DataFrame(self.to_dicts())

    @classmethod
    def from_dataframe(cls, table) -> "TableProfile":
        from orso.schema import FlatColumn
        from orso.schema import RelationSchema

        profile = cls()

        profiler_classes = {
            OrsoTypes.VARCHAR: VarcharProfiler,
            OrsoTypes.BLOB: VarcharProfiler,
            OrsoTypes.INTEGER: NumericProfiler,
            OrsoTypes.DOUBLE: NumericProfiler,
            OrsoTypes.DECIMAL: NumericProfiler,
            OrsoTypes.ARRAY: ListStructProfiler,
            OrsoTypes.STRUCT: ListStructProfiler,
            OrsoTypes.BOOLEAN: BooleanProfiler,
            OrsoTypes.DATE: DateProfiler,
            OrsoTypes.TIMESTAMP: DateProfiler,
        }

        profiles = {}

        for morsel in table.to_batches(25000):
            if not isinstance(morsel.schema, RelationSchema):
                morsel._schema = RelationSchema(
                    name="morsel", columns=[FlatColumn(name=c) for c in morsel.schema]
                )

            for column in morsel.schema.columns:
                column_data = morsel.collect(column.name)
                if len(column_data) == 0:
                    continue

                profiler_class = profiler_classes.get(column.type, DefaultProfiler)
                profiler = profiler_class(column)
                profiler(column_data=column_data)
                if column.name in profiles:
                    profiles[column.name] += profiler.profile
                else:
                    profiles[column.name] = profiler.profile

        for name, summary in profiles.items():
            profile.add_column(summary, name)

        return profile


class BaseProfiler:
    def __init__(self, column: FlatColumn):
        self.column = column
        self.profile = ColumnProfile(name=column.name, type=column.type)

    def __call__(self, column_data: List[Any]):
        raise NotImplementedError("Must be implemented by subclass.")


class ListStructProfiler(BaseProfiler):
    def __call__(self, column_data: List[Any]):
        self.profile.count = len(column_data)
        self.profile.missing = sum(1 for val in column_data if val is None)


class DefaultProfiler(BaseProfiler):
    def __call__(self, column_data: List[Any]):
        self.profile.count = len(column_data)
        self.profile.missing = sum(1 for val in column_data if val != val)


class BooleanProfiler(BaseProfiler):
    def __call__(self, column_data: List[Any]):
        self.profile.count = len(column_data)

        column_data = [col for col in column_data if col is not None]
        self.profile.missing = self.profile.count - len(column_data)


class NumericProfiler(BaseProfiler):
    def __call__(self, column_data: List[Any]):
        self.profile.count = len(column_data)
        column_data = numpy.array(column_data, copy=False)  # Ensure column_data is a NumPy array
        if column_data.dtype.name == "object":
            column_data = column_data[~numpy.equal(column_data, -9223372036854775808)]
            column_data = [float(c) for c in column_data if c is not None]
        else:
            column_data = column_data[~numpy.isnan(column_data)]
        self.profile.missing = self.profile.count - len(column_data)
        # Compute min and max only if necessary
        if len(column_data) > 0:
            self.profile.minimum = int(numpy.min(column_data))
            self.profile.maximum = int(numpy.max(column_data))


class VarcharProfiler(BaseProfiler):
    def __call__(self, column_data: List[Any]):
        self.profile.count = len(column_data)
        column_data = [col for col in column_data if col is not None]
        if len(column_data) > 0:
            column_data = [col[:SIXTY_FOUR_BYTES] for col in column_data]
            self.profile.missing = self.profile.count - len(column_data)
            self.profile.minimum = string_to_int64(min(column_data))
            self.profile.maximum = string_to_int64(max(column_data))


class DateProfiler(BaseProfiler):
    def __call__(self, column_data: List[Any]):
        self.profile.count = len(column_data)
        if hasattr(column_data[0], "value"):
            column_data = numpy.array(
                [v.value for v in column_data if v is not None], dtype="int64"
            )
        else:
            column_data = numpy.array(column_data, dtype="datetime64[s]").astype("int64")
        column_data = column_data[~numpy.equal(column_data, -9223372036854775808)]
        self.profile.missing = self.profile.count - len(column_data)
        if len(column_data) > 0:
            numeric_profiler = NumericProfiler(self.column)
            numeric_profiler(column_data)
            numeric_profile = numeric_profiler.profile

            self.profile.minimum = numeric_profile.minimum
            self.profile.maximum = numeric_profile.maximum


def table_profiler(dataframe) -> List[Dict[str, Any]]:
    return TableProfile.from_dataframe(dataframe)


if __name__ == "__main__":
    import time

    import opteryx

    #    SQL = "SELECT * FROM 'scratch/tweets.arrow' -- $missions"
    SQL = "SELECT * FROM $missions"
    df = opteryx.query(SQL)
    # df = opteryx.query("SELECT '2023-02-02'")
    # df = opteryx.query("SELECT current_time")
    # df = opteryx.query("SELECT now() - current_date, INTERVAL '10' HOUR")
    print(df.display())
    t = time.monotonic_ns()
    for i in range(1000):
        a = table_profiler(df)
        # df.profile
    print((time.monotonic_ns() - t) / 1e9)
    print(a.to_dataframe())
