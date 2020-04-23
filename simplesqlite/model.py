"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

import abc
import re
from collections import OrderedDict
from sqlite3 import Cursor
from typing import Any, Dict, Generator, List, Optional, Sequence, Type, cast

import typepy
from typepy.type import AbstractType

from .core import SimpleSQLite
from .error import DatabaseError
from .query import Attr, AttrList, Value, WhereQuery


def dict_factory(cursor: Cursor, row: Sequence) -> Dict:
    record = {}

    for idx, col in enumerate(cursor.description):
        record[col[0]] = row[idx]

    return record


class Column(metaclass=abc.ABCMeta):
    @abc.abstractproperty
    def sqlite_datatype(self):
        return ""

    @abc.abstractproperty
    def typepy_class(self):
        return None

    @property
    def not_null(self):
        return self.__not_null

    def __init__(
        self,
        attr_name=None,
        not_null=False,
        primary_key=False,
        unique=False,
        autoincrement=False,
        default=None,
    ):
        self.__header_name = attr_name
        self.__not_null = not_null
        self.__primary_key = primary_key
        self.__unique = unique
        self.__autoincrement = autoincrement
        self.__default_value = None if self.__not_null else default

    def get_header(self, attr_name: str) -> str:
        if self.__header_name:
            return self.__header_name

        return attr_name

    def get_desc(self) -> str:
        constraints = [self.sqlite_datatype]

        if self.__primary_key:
            constraints.append("PRIMARY KEY")
        else:
            if self.__not_null:
                constraints.append("NOT NULL")
            if self.__unique:
                constraints.append("UNIQUE")

        if self.__autoincrement and self.sqlite_datatype == "INTEGER":
            constraints.append("AUTOINCREMENT")

        if self.__default_value is not None:
            constraints.append("DEFAULT {}".format(Value(self.__default_value)))

        return " ".join(constraints)


class Integer(Column):
    @property
    def sqlite_datatype(self) -> str:
        return "INTEGER"

    @property
    def typepy_class(self) -> Type[AbstractType]:
        return typepy.Integer


class Real(Column):
    @property
    def sqlite_datatype(self) -> str:
        return "REAL"

    @property
    def typepy_class(self) -> Type[AbstractType]:
        return typepy.RealNumber


class Text(Column):
    @property
    def sqlite_datatype(self) -> str:
        return "TEXT"

    @property
    def typepy_class(self) -> Type[AbstractType]:
        return typepy.String


class Blob(Column):
    @property
    def sqlite_datatype(self) -> str:
        return "BLOB"

    @property
    def typepy_class(self) -> Type[AbstractType]:
        return typepy.Bytes


class Model:
    __connection = None
    __is_hidden = False
    __table_name = None
    __attr_names = None

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
            table_name = "_{:s}_".format(table_name)

        cls.__table_name = table_name

        return cls.__table_name

    @classmethod
    def get_attr_names(cls) -> List[str]:
        if cls.__attr_names:
            return cls.__attr_names

        cls.__attr_names = [attr_name for attr_name in cls.__dict__ if cls.__is_attr(attr_name)]

        return cls.__attr_names

    @classmethod
    def create(cls) -> None:
        cls.__validate_connection()
        assert cls.__connection  # to avoid type check error

        attr_descs = []

        for attr_name in cls.get_attr_names():
            col = cls._get_col(attr_name)
            attr_descs.append(
                "{attr} {constraints}".format(
                    attr=Attr(col.get_header(attr_name)), constraints=col.get_desc()
                )
            )

        cls.__connection.create_table(cls.get_table_name(), attr_descs)

    @classmethod
    def select(cls, where: Optional[WhereQuery] = None, extra: None = None) -> Generator:
        cls.__validate_connection()
        assert cls.__connection  # to avoid type check error

        try:
            stash_row_factory = cls.__connection.connection.row_factory  # type: ignore
            cls.__connection.set_row_factory(dict_factory)

            result = cls.__connection.select(
                select=AttrList(
                    [
                        cls._get_col(attr_name).get_header(attr_name)
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
            value = getattr(model_obj, attr_name)

            if value is None:
                continue

            cls.__validate_value(attr_name, value)

            record[cls._get_col(attr_name).get_header(attr_name)] = value

        cls.__connection.insert(cls.get_table_name(), record)

    @classmethod
    def commit(cls) -> None:
        cls.__validate_connection()
        assert cls.__connection  # to avoid type check error
        cls.__connection.commit()

    @classmethod
    def fetch_schema(cls):
        return cls.__connection.schema_extractor.fetch_table_schema(cls.get_table_name())

    @classmethod
    def fetch_num_records(cls, where: None = None) -> int:
        assert cls.__connection  # to avoid type check error
        return cast(int, cls.__connection.fetch_num_records(cls.get_table_name(), where=where))

    @classmethod
    def attr_to_header(cls, attr_name: str) -> str:
        return cls._get_col(attr_name).get_header(attr_name)

    def as_dict(self) -> Dict:
        record = OrderedDict()
        for attr_name in self.get_attr_names():
            value = getattr(self, attr_name)
            if value is None:
                continue

            record[self.attr_to_header(attr_name)] = value

        return record

    def __init__(self, *args, **kwargs) -> None:
        for attr_name in self.get_attr_names():
            value = kwargs.get(attr_name)
            if value is None:
                value = kwargs.get(self.attr_to_header(attr_name))

            setattr(self, attr_name, value)

    def __eq__(self, other) -> bool:
        if type(self) != type(other):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other) -> bool:
        if type(self) != type(other):
            return True

        return self.__dict__ != other.__dict__

    def __repr__(self) -> str:
        return "{name:s} ({attributes:s})".format(
            name=type(self).__name__,
            attributes=", ".join(
                ["{}={}".format(key, value) for key, value in self.as_dict().items()]
            ),
        )

    @classmethod
    def __validate_connection(cls) -> None:
        if cls.__connection is None:
            raise DatabaseError("SimpleSQLite connection required. you need to call attach first")

    @classmethod
    def __validate_value(cls, attr_name: str, value: Any) -> None:
        column = cls._get_col(attr_name)

        if value is None and not column.not_null:
            return

        column.typepy_class(value).validate()

    @classmethod
    def __is_attr(cls, attr_name: str) -> bool:
        private_var_regexp = re.compile("^_{}__[a-zA-Z]+".format(Model.__name__))

        return (
            not attr_name.startswith("__")
            and private_var_regexp.search(attr_name) is None
            and not callable(cls.__dict__.get(attr_name))
        )

    @classmethod
    def _get_col(cls, attr_name: str) -> Column:
        if attr_name not in cls.get_attr_names():
            raise ValueError("invalid attribute: {}".format(attr_name))

        return cls.__dict__[attr_name]
