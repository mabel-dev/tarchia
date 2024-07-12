"""
Check the regex and isidentifier() perform the same.
"""
import os
import sys

import re
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from tarchia.constants import IDENTIFIER_REG_EX

# fmt:off
test_cases = [
    ("valid_identifier", True),
    ("_underscore_start", True),
    ("with123numbers", True),
    ("another_valid_one", True),
    ("_leading_underscore_and_numbers123", True),
    ("contains_caps_And_numbers123", True),
    ("lowercase", True),
    ("UPPERCASE", True),
    ("CamelCase", True),
    ("_private", True),
    ("with_underscores_between", True),
    ("a", True),
    ("_a", True),
    ("_a1", True),
    ("_1a", True),
    ("thisIsValid", True),
    ("__doubleUnderscore__", True),
    ("with_underscore_end_", True),
    ("_with_underscore_start_and_end_", True),
    ("i", True),
    ("I", True),
    ("i123", True),
    ("_i", True),
    ("_i123", True),
    ("valid1", True),
    ("another_valid1", True),
    ("yet_another_valid1", True),
    ("_yet_another_valid1", True),
    ("valid_id_with_123", True),
    ("_valid_id_with_123", True),
    ("just_a_simple_one", True),
    ("isidentifier", True),
    ("try_except_finally", True),
    ("MyClass", True),
    ("__init__", True),
    ("main", True),
    ("__", True),
    ("_", True),
    ("_1", True),
    ("_", True),
    ("_", True),
    ("__", True),
    ("_", True),
    ("identifier_", True),
    ("_identifier", True),
    ("_", True),
    ("_", True),
    ("__", True),
    ("a1", True),
    ("a_", True),
    ("_a_", True),
    ("_", True),
    ("1invalid", False),
    ("with space", False),
    ("with-hyphen", False),
    ("with.dot", False),
    ("123numbers_first", False),
    ("contains special&char", False),
    ("startswith$", False),
    ("#", False),
    ("!exclamation", False),
    ("space in between", False),
    ("tab\tchar", False),
    ("newline\nchar", False),
    ("invalid-char@", False),
    ("dollar$sign", False),
    ("percent%char", False),
    ("caret^char", False),
    ("ampersand&char", False),
    ("asterisk*char", False),
    ("parenthesis(char)", False),
    ("bracket[char]", False),
    ("brace{char}", False),
    ("semicolon;char", False),
    ("colon:char", False),
    ("quote'char", False),
    ('double"quote', False),
    ("comma,char", False),
    ("less<than", False),
    ("greater>than", False),
    ("slash/char", False),
    ("backslash\\char", False),
    ("pipe|char", False),
    ("tilde~char", False),
    ("grave`char", False),
    ("plus+char", False),
    ("equals=char", False),
    ("minus-char", False),
    ("@decorator", False),
    ("two words", False),
    (" leading_space", False),
    ("trailing_space ", False),
    ("mid dle", False),
    ("mid-dle", False),
    ("mid.dle", False),
    ("with@symbol", False),
    ("with#symbol", False),
    ("with!symbol", False),
    ("with%symbol", False),
    ("with^symbol", False),
    ("with&symbol", False),
    ("with*symbol", False)
]

# fmt:on


@pytest.mark.parametrize("string, expected", test_cases)
def test_validator(string, expected):
    assert (re.match(IDENTIFIER_REG_EX, string) is not None) == expected, f"ReExp Failed for {string}"
    assert string.isidentifier() == expected, f"isidentifier() Failed for {string}"


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(test_cases)} TESTS")
    for string, expected in test_cases:
        test_validator(string, expected)
    print("✅ okay")