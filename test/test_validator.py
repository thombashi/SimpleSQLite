import string

import pytest
from pathvalidate import unprintable_ascii_chars
from pathvalidate.error import ErrorReason, ValidationError

from simplesqlite._validator import validate_sqlite_attribute_name, validate_sqlite_table_name


__SQLITE_VALID_RESERVED_KEYWORDS = [
    "ABORT",
    "ACTION",
    "AFTER",
    "ANALYZE",
    "ASC",
    "ATTACH",
    "BEFORE",
    "BEGIN",
    "BY",
    "CASCADE",
    "CAST",
    "COLUMN",
    "CONFLICT",
    "CROSS",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "DATABASE",
    "DEFERRED",
    "DESC",
    "DETACH",
    "EACH",
    "END",
    "EXCLUSIVE",
    "EXPLAIN",
    "FAIL",
    "FOR",
    "FULL",
    "GLOB",
    "IGNORE",
    "IMMEDIATE",
    "INDEXED",
    "INITIALLY",
    "INNER",
    "INSTEAD",
    "KEY",
    "LEFT",
    "LIKE",
    "MATCH",
    "NATURAL",
    "NO",
    "OF",
    "OFFSET",
    "OUTER",
    "PLAN",
    "PRAGMA",
    "QUERY",
    "RAISE",
    "RECURSIVE",
    "REGEXP",
    "REINDEX",
    "RELEASE",
    "RENAME",
    "REPLACE",
    "RESTRICT",
    "RIGHT",
    "ROLLBACK",
    "ROW",
    "SAVEPOINT",
    "TEMP",
    "TEMPORARY",
    "TRIGGER",
    "VACUUM",
    "VIEW",
    "VIRTUAL",
    "WITH",
    "WITHOUT",
]
__SQLITE_INVALID_RESERVED_KEYWORDS = [
    "ADD",
    "ALL",
    "ALTER",
    "AND",
    "AS",
    "AUTOINCREMENT",
    "BETWEEN",
    "CASE",
    "CHECK",
    "COLLATE",
    "COMMIT",
    "CONSTRAINT",
    "CREATE",
    "DEFAULT",
    "DEFERRABLE",
    "DELETE",
    "DISTINCT",
    "DROP",
    "ELSE",
    "ESCAPE",
    "EXCEPT",
    "EXISTS",
    "FOREIGN",
    "FROM",
    "GROUP",
    "HAVING",
    "IN",
    "INDEX",
    "INSERT",
    "INTERSECT",
    "INTO",
    "IS",
    "ISNULL",
    "JOIN",
    "LIMIT",
    "NOT",
    "NOTNULL",
    "NULL",
    "ON",
    "OR",
    "ORDER",
    "PRIMARY",
    "REFERENCES",
    "SELECT",
    "SET",
    "TABLE",
    "THEN",
    "TO",
    "TRANSACTION",
    "UNION",
    "UNIQUE",
    "UPDATE",
    "USING",
    "VALUES",
    "WHEN",
    "WHERE",
]

VALID_RESERVED_KEYWORDS_TABLE_UPPER = __SQLITE_VALID_RESERVED_KEYWORDS
INVALID_RESERVED_KEYWORDS_TABLE_UPPER = __SQLITE_INVALID_RESERVED_KEYWORDS + ["IF"]
VALID_RESERVED_KEYWORDS_TABLE_LOWER = [
    keyword.lower() for keyword in VALID_RESERVED_KEYWORDS_TABLE_UPPER
]
INVALID_RESERVED_KEYWORDS_TABLE_LOWER = [
    keyword.lower() for keyword in INVALID_RESERVED_KEYWORDS_TABLE_UPPER
]

VALID_RESERVED_KEYWORDS_ATTR_UPPER = __SQLITE_VALID_RESERVED_KEYWORDS + ["IF"]
INVALID_RESERVED_KEYWORDS_ATTR_UPPER = __SQLITE_INVALID_RESERVED_KEYWORDS
VALID_RESERVED_KEYWORDS_ATTR_LOWER = [
    keyword.lower() for keyword in VALID_RESERVED_KEYWORDS_ATTR_UPPER
]
INVALID_RESERVED_KEYWORDS_ATTR_LOWER = [
    keyword.lower() for keyword in INVALID_RESERVED_KEYWORDS_ATTR_UPPER
]

UTF8_WORDS = [["あいうえお"], ["属性"]]


class Test_validate_sqlite_table_name:
    @pytest.mark.parametrize(
        ["value"],
        [
            [f"{keyword}a"]
            for keyword in (
                VALID_RESERVED_KEYWORDS_TABLE_UPPER
                + INVALID_RESERVED_KEYWORDS_TABLE_UPPER
                + VALID_RESERVED_KEYWORDS_ATTR_UPPER
                + INVALID_RESERVED_KEYWORDS_ATTR_UPPER
            )
        ],
    )
    def test_normal_ascii(self, value):
        validate_sqlite_table_name(value)

    @pytest.mark.parametrize(["value"], UTF8_WORDS)
    def test_normal_utf8(self, value):
        validate_sqlite_table_name(value)

    @pytest.mark.parametrize(
        ["value"], [[first_char + "hoge123"] for first_char in string.digits + "%#!-*"]
    )
    def test_normal_non_alphabet_first_char(self, value):
        validate_sqlite_table_name(value)

    @pytest.mark.parametrize(
        ["value"],
        [[f"a{invalid_c}b"] for invalid_c in unprintable_ascii_chars]
        + [[f"テ{invalid_c}！!スト"] for invalid_c in unprintable_ascii_chars],
    )
    def test_exception_invalid_win_char(self, value):
        try:
            validate_sqlite_table_name(value)
        except ValidationError as e:
            assert e.reason == ErrorReason.INVALID_CHARACTER

    @pytest.mark.parametrize(
        ["value", "expected"],
        [[None, ValidationError], ["", ValidationError], [1, TypeError], [True, TypeError]],
    )
    def test_exception_type(self, value, expected):
        with pytest.raises(expected):
            validate_sqlite_table_name(value)

    @pytest.mark.parametrize(
        ["value"],
        [
            [reserved_keyword]
            for reserved_keyword in VALID_RESERVED_KEYWORDS_TABLE_UPPER
            + VALID_RESERVED_KEYWORDS_TABLE_LOWER
        ],
    )
    def test_exception_reserved_valid(self, value):
        try:
            validate_sqlite_table_name(value)
        except ValidationError as e:
            assert e.reason == ErrorReason.RESERVED_NAME
            assert e.reusable_name

    @pytest.mark.parametrize(
        ["value"],
        [
            [reserved_keyword]
            for reserved_keyword in INVALID_RESERVED_KEYWORDS_TABLE_UPPER
            + INVALID_RESERVED_KEYWORDS_TABLE_LOWER
        ],
    )
    def test_exception_reserved_invalid_name(self, value):
        try:
            validate_sqlite_table_name(value)
        except ValidationError as e:
            assert e.reason == ErrorReason.RESERVED_NAME
            assert e.reusable_name is False


class Test_validate_sqlite_attr_name:
    @pytest.mark.parametrize(
        ["value"],
        [
            [f"{keyword}a"]
            for keyword in (
                VALID_RESERVED_KEYWORDS_TABLE_UPPER
                + INVALID_RESERVED_KEYWORDS_TABLE_UPPER
                + VALID_RESERVED_KEYWORDS_ATTR_UPPER
                + INVALID_RESERVED_KEYWORDS_ATTR_UPPER
                + ["_"]
            )
        ],
    )
    def test_normal_ascii(self, value):
        validate_sqlite_attribute_name(value)

    @pytest.mark.parametrize(["value"], UTF8_WORDS)
    def test_normal_utf8(self, value):
        validate_sqlite_attribute_name(value)

    @pytest.mark.parametrize(
        ["value"], [[first_char + "hoge123"] for first_char in string.digits + "%#!-*"]
    )
    def test_normal_non_alphabet_first_char(self, value):
        validate_sqlite_attribute_name(value)

    @pytest.mark.parametrize(
        ["value"],
        [[f"a{invalid_c}b"] for invalid_c in unprintable_ascii_chars]
        + [[f"テ{invalid_c}！!スト"] for invalid_c in unprintable_ascii_chars],
    )
    def test_exception_invalid_win_char(self, value):
        try:
            validate_sqlite_table_name(value)
        except ValidationError as e:
            assert e.reason == ErrorReason.INVALID_CHARACTER

    @pytest.mark.parametrize(
        ["value", "expected"],
        [[None, ValidationError], ["", ValidationError], [1, TypeError], [True, TypeError]],
    )
    def test_exception_type(self, value, expected):
        with pytest.raises(expected):
            validate_sqlite_attribute_name(value)

    @pytest.mark.parametrize(
        ["value"],
        [
            [reserved_keyword]
            for reserved_keyword in VALID_RESERVED_KEYWORDS_ATTR_UPPER
            + VALID_RESERVED_KEYWORDS_ATTR_LOWER
        ],
    )
    def test_exception_reserved_valid(self, value):
        try:
            validate_sqlite_attribute_name(value)
        except ValidationError as e:
            assert e.reason == ErrorReason.RESERVED_NAME
            assert e.reusable_name

    @pytest.mark.parametrize(
        ["value"],
        [
            [reserved_keyword]
            for reserved_keyword in INVALID_RESERVED_KEYWORDS_ATTR_UPPER
            + INVALID_RESERVED_KEYWORDS_ATTR_LOWER
        ],
    )
    def test_exception_reserved_invalid_name(self, value):
        try:
            validate_sqlite_attribute_name(value)
        except ValidationError as e:
            assert e.reason == ErrorReason.RESERVED_NAME
            assert e.reusable_name is False
