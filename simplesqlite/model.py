"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

import re
import warnings
from collections import OrderedDict
from collections.abc import Generator, Sequence
from sqlite3 import Cursor
from typing import Any, Optional, Union, cast

import typepy
from sqliteschema import SQLiteTableSchema
from typepy.type import AbstractType

from ._column import Column
from .core import SimpleSQLite
from .error import DatabaseError, TableNotFoundError
from .query import Attr, AttrList
from .query import Set as SetQuery
from .query import WhereQuery


__all__ = ("Integer", "Real", "Text", "Blob", "Model", "Column")


def dict_factory(cursor: Cursor, row: Sequence) -> dict:
    record = {}

    for idx, col in enumerate(cursor.description):
        record[col[0]] = row[idx]

    return record


class Integer(Column):
    @property
    def sqlite_datatype(self) -> str:
        return "INTEGER"

    @property
    def typepy_class(self) -> type[AbstractType]:
        return typepy.Integer


class Real(Column):
    @property
    def sqlite_datatype(self) -> str:
        return "REAL"

    @property
    def typepy_class(self) -> type[AbstractType]:
        return typepy.RealNumber


class Text(Column):
    @property
    def sqlite_datatype(self) -> str:
        return "TEXT"

    @property
    def typepy_class(self) -> type[AbstractType]:
        return typepy.String


class Blob(Column):
    @property
    def sqlite_datatype(self) -> str:
        return "BLOB"

    @property
    def typepy_class(self) -> type[AbstractType]:
        return typepy.Bytes


class Model:
    __connection: SimpleSQLite
    __is_hidden = False
    __table_name: Optional[str] = None
    __attr_names: list[str] = []

    @classmethod
    def attach(cls, database_src: SimpleSQLite, is_hidden: bool = False) -> None:
        cls.__connection = SimpleSQLite(database_src)
        cls.__is_hidden = is_hidden

    @classmethod
    def get_table_name(cls) -> str:
        if cls.__table_name:
            return cls.__table_name

        table_name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", cls.__name__)
        table_name = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", table_name)
        table_name = table_name.replace("-", "_").lower()

        if cls.__is_hidden:
            table_name = f"_{table_name:s}_"

        cls.__table_name = table_name

        return cls.__table_name

    @classmethod
    def get_attr_names(cls) -> list[str]:
        if cls.__attr_names:
            return cls.__attr_names

        cls.__attr_names = [attr_name for attr_name in cls.__dict__ if cls.__is_attr(attr_name)]
        for attr_name in cls.__attr_names:
            col = cls._get_col(attr_name, validate_name=False)
            col._set_column_name_if_uninitialized(attr_name)

        return cls.__attr_names

    @classmethod
    def create(cls) -> None:
        cls.__validate_connection()
        assert cls.__connection  # to avoid type check error

        attr_descs = []

        for attr_name in cls.get_attr_names():
            col = cls._get_col(attr_name, validate_name=False)
            attr_descs.append(
                "{attr} {constraints}".format(
                    attr=Attr(col.get_column_name()), constraints=col.get_desc()
                )
            )

        cls.__connection.create_table(cls.get_table_name(), attr_descs)

    @classmethod
    def select(cls, where: Optional[WhereQuery] = None, extra: Optional[str] = None) -> Generator:
        cls.__validate_connection()
        assert cls.__connection  # to avoid type check error
        assert cls.__connection.connection  # to avoid type check error

        stash_row_factory = cls.__connection.connection.row_factory

        try:
            cls.__connection.set_row_factory(dict_factory)

            result = cls.__connection.select(
                select=AttrList(
                    [
                        cls._get_col(attr_name, validate_name=False).get_column_name()
                        for attr_name in cls.get_attr_names()
                    ]
                ),
                table_name=cls.get_table_name(),
                where=where,
                extra=extra,
            )
            assert result  # to avoid type check error
            for record in result.fetchall():
                yield cls(**record)
        finally:
            cls.__connection.set_row_factory(stash_row_factory)

    @classmethod
    def insert(cls, model_obj: "Model") -> None:
        cls.__validate_connection()
        assert cls.__connection  # to avoid type check error

        if type(model_obj).__name__ != cls.__name__:
            raise TypeError(
                "unexpected type: expected={}, actual={}".format(
                    cls.__name__, type(model_obj).__name__
                )
            )

        record = {}

        for attr_name in cls.get_attr_names():
            if attr_name in model_obj.__no_value_columns:
                continue

            value = getattr(model_obj, attr_name)
            cls.__validate_value(attr_name, value)

            record[cls._get_col(attr_name, validate_name=False).get_column_name()] = value

        try:
            cls.__connection.insert(cls.get_table_name(), record, list(record.keys()))
        except TableNotFoundError as e:
            raise RuntimeError(f"{e}: execute 'create' method before insert")

    @classmethod
    def update(
        cls, set_query: Union[str, Sequence[SetQuery]], where: Optional[WhereQuery] = None
    ) -> None:
        cls.__connection.update(cls.get_table_name(), set_query=set_query, where=where)

    @classmethod
    def commit(cls) -> None:
        cls.__validate_connection()
        assert cls.__connection  # to avoid type check error
        cls.__connection.commit()

    @classmethod
    def delete(cls, where: Optional[WhereQuery]) -> Optional[Cursor]:
        cls.__validate_connection()
        return cls.__connection.delete(cls.get_table_name(), where=where)

    @classmethod
    def fetch_schema(cls) -> SQLiteTableSchema:
        cls.__validate_connection()
        assert cls.__connection  # to avoid type check error
        return cls.__connection.schema_extractor.fetch_table_schema(cls.get_table_name())

    @classmethod
    def fetch_num_records(cls, where: None = None) -> int:
        assert cls.__connection  # to avoid type check error
        return cast(int, cls.__connection.fetch_num_records(cls.get_table_name(), where=where))

    @classmethod
    def attr_to_header(cls, attr_name: str) -> str:
        warnings.warn(
            "attr_to_header() is deprecated. Use attr_to_column() instead.", DeprecationWarning
        )
        return cls._get_col(attr_name).get_column_name()

    @classmethod
    def attr_to_column(cls, attr_name: str) -> str:
        return cls._get_col(attr_name).get_column_name()

    def as_dict(self) -> dict:
        record = OrderedDict()
        for attr_name in self.get_attr_names():
            value = getattr(self, attr_name)
            if value is None:
                continue

            record[self.attr_to_column(attr_name)] = value

        return record

    def __init__(self, **kwargs: Any) -> None:
        for attr_name in self.get_attr_names():
            value = kwargs.get(attr_name)
            if value is None:
                value = kwargs.get(self.attr_to_column(attr_name))

            setattr(self, attr_name, value)

        self.__update_no_value_columns()

    def __setattr__(self, name: str, value: Any) -> None:
        if name == "_Model__no_value_columns":
            # avoid infinite recursion
            super().__setattr__(name, value)
            return

        super().__setattr__(name, value)
        self.__update_no_value_columns()

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return True

        return self.__dict__ != other.__dict__

    def __repr__(self) -> str:
        return "{name:s} ({attributes:s})".format(
            name=type(self).__name__,
            attributes=", ".join([f"{key}={value}" for key, value in self.as_dict().items()]),
        )

    def __update_no_value_columns(self) -> None:
        self.__no_value_columns: set[str] = set()

        for attr_name in self.get_attr_names():
            value = getattr(self, attr_name)
            if value is None:
                value = getattr(self, self.attr_to_column(attr_name))
                if value is None:
                    self.__no_value_columns.add(attr_name)

    @classmethod
    def __validate_connection(cls) -> None:
        if cls.__connection is None:
            raise DatabaseError("SimpleSQLite connection required. you need to call attach first")

    @classmethod
    def __validate_value(cls, attr_name: str, value: Any) -> None:
        column = cls._get_col(attr_name)

        if value is None and not column.not_null:
            return

        column.typepy_class(value, strict_level=typepy.StrictLevel.MIN).validate()

    @classmethod
    def __is_attr(cls, attr_name: str) -> bool:
        private_var_regexp = re.compile(f"^_{Model.__name__}__[a-zA-Z]+")

        return (
            not attr_name.startswith("__")
            and private_var_regexp.search(attr_name) is None
            and not callable(cls.__dict__.get(attr_name))
        )

    @classmethod
    def _get_col(cls, attr_name: str, validate_name: bool = True) -> Column:
        if validate_name and attr_name not in cls.get_attr_names():
            raise ValueError(f"invalid attribute: {attr_name}")

        return cls.__dict__[attr_name]
