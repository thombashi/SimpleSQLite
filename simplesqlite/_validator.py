import re

from pathvalidate import unprintable_ascii_chars
from pathvalidate.error import ErrorReason, ValidationError


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

__SQLITE_VALID_RESERVED_KEYWORDS_TABLE = __SQLITE_VALID_RESERVED_KEYWORDS
__SQLITE_INVALID_RESERVED_KEYWORDS_TABLE = __SQLITE_INVALID_RESERVED_KEYWORDS + ["IF"]

__SQLITE_VALID_RESERVED_KEYWORDS_ATTR = __SQLITE_VALID_RESERVED_KEYWORDS + ["IF"]
__SQLITE_INVALID_RESERVED_KEYWORDS_ATTR = __SQLITE_INVALID_RESERVED_KEYWORDS

__RE_INVALID_CHARS = re.compile(
    "[{:s}]".format(re.escape("".join(unprintable_ascii_chars))), re.UNICODE
)


def validate_sqlite_table_name(table_name: str) -> None:
    """
    :param str table_name: Name to validate.
    :raises pathvalidate.ValidationError:
        - If the ``name`` includes unprintable character(s).
        - |raises_sqlite_keywords|
    """

    if not table_name:
        raise ValidationError(["null name"], reason=ErrorReason.NULL_NAME)

    if __RE_INVALID_CHARS.search(table_name):
        raise ValidationError(["unprintable character found"], reason=ErrorReason.INVALID_CHARACTER)

    table_name = table_name.upper()

    if table_name in __SQLITE_INVALID_RESERVED_KEYWORDS_TABLE:
        raise ValidationError(
            [f"'{table_name}' is a reserved keyword by sqlite"],
            reason=ErrorReason.RESERVED_NAME,
            reusable_name=False,
        )

    if table_name in __SQLITE_VALID_RESERVED_KEYWORDS_TABLE:
        raise ValidationError(
            [f"'{table_name}' is a reserved keyword by sqlite"],
            reason=ErrorReason.RESERVED_NAME,
            reusable_name=True,
        )


def validate_sqlite_attribute_name(attribute_name: str) -> None:
    """
    :param str attribute_name: Name to validate.
    :raises pathvalidate.ValidationError:
        - If the ``name`` includes unprintable character(s).
        - |raises_sqlite_keywords|
    """

    if not attribute_name:
        raise ValidationError(["null name"], reason=ErrorReason.NULL_NAME)

    if __RE_INVALID_CHARS.search(attribute_name):
        raise ValidationError(["unprintable character found"], reason=ErrorReason.INVALID_CHARACTER)

    attribute_name = attribute_name.upper()

    if attribute_name in __SQLITE_INVALID_RESERVED_KEYWORDS_ATTR:
        raise ValidationError(
            [f"'{attribute_name}' is a reserved keyword by sqlite"],
            reason=ErrorReason.RESERVED_NAME,
            reusable_name=False,
        )

    if attribute_name in __SQLITE_VALID_RESERVED_KEYWORDS_ATTR:
        raise ValidationError(
            [f"'{attribute_name}' is a reserved keyword by sqlite"],
            reason=ErrorReason.RESERVED_NAME,
            reusable_name=True,
        )
