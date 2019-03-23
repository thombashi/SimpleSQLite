# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from __future__ import absolute_import, unicode_literals

import abc
import re
import sys

import six
import typepy
from six.moves import zip

from .core import SimpleSQLite
from .error import DatabaseError
from .query import Attr, AttrList, Value


@six.add_metaclass(abc.ABCMeta)
class Column(object):
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
        self, not_null=False, primary_key=False, unique=False, autoincrement=False, default=None
    ):
        self.__not_null = not_null
        self.__primary_key = primary_key
        self.__unique = unique
        self.__autoincrement = autoincrement
        self.__default_value = None if self.__not_null else default

    def get_desc(self):
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
    def sqlite_datatype(self):
        return "INTEGER"

    @property
    def typepy_class(self):
        return typepy.Integer


class Real(Column):
    @property
    def sqlite_datatype(self):
        return "REAL"

    @property
    def typepy_class(self):
        return typepy.RealNumber


class Text(Column):
    @property
    def sqlite_datatype(self):
        return "TEXT"

    @property
    def typepy_class(self):
        return typepy.String


class Blob(Column):
    @property
    def sqlite_datatype(self):
        return "BLOB"

    @property
    def typepy_class(self):
        return typepy.Binary


class Model(object):
    __connection = None
    __is_hidden = False
    __table_name = None
    __attr_names = None

    @classmethod
    def attach(cls, database_src, is_hidden=False):
        cls.__connection = SimpleSQLite(database_src)
        cls.__is_hidden = is_hidden

    @classmethod
    def get_table_name(cls):
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
    def get_attr_names(cls):
        if cls.__attr_names:
            return cls.__attr_names

        attr_names = [attr_name for attr_name in cls.__dict__ if cls.__is_attr(attr_name)]

        if sys.version_info[:2] >= (3, 5):
            cls.__attr_names = attr_names
        else:
            cls.__attr_names = sorted(attr_names)

        return cls.__attr_names

    @classmethod
    def get_attr_name_list(cls):
        import warnings

        warnings.warn("'get_attr_name_list()' has moved to 'get_attr_names()'", DeprecationWarning)

        return cls.get_attr_names()

    @classmethod
    def create(cls):
        cls.__validate_connection()

        cls.__connection.create_table(
            cls.get_table_name(),
            [
                "{attr} {constraints}".format(
                    attr=Attr(attr_name), constraints=cls.__get_col(attr_name).get_desc()
                )
                for attr_name in cls.get_attr_names()
            ],
        )

    @classmethod
    def select(cls, where=None, extra=None):
        cls.__validate_connection()

        result = cls.__connection.select(
            select=AttrList(cls.get_attr_names()),
            table_name=cls.get_table_name(),
            where=where,
            extra=extra,
        )
        for record in result.fetchall():
            yield cls(
                **{attr_name: value for attr_name, value in zip(cls.get_attr_names(), record)}
            )

    @classmethod
    def insert(cls, model_obj):
        cls.__validate_connection()

        if type(model_obj).__name__ != cls.__name__:
            raise TypeError(
                "unexpected type: expected={}, actual={}".format(
                    cls.__name__, type(model_obj).__name__
                )
            )

        record = {}
        attr_names = []

        for attr_name in cls.get_attr_names():
            value = getattr(model_obj, attr_name)

            if value is None:
                continue

            cls.__validate_value(attr_name, value)

            attr_names.append(attr_name)
            record[attr_name] = value

        cls.__connection.insert(cls.get_table_name(), record, attr_names=attr_names)

    @classmethod
    def commit(cls):
        cls.__connection.commit()

    @classmethod
    def fetch_schema(cls):
        return cls.__connection.schema_extractor.fetch_table_schema(cls.get_table_name())

    def __init__(self, *args, **kwargs):
        for attr_name in self.get_attr_names():
            setattr(self, attr_name, kwargs.get(attr_name))

    def __eq__(self, other):
        if type(self) != type(other):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        if type(self) != type(other):
            return True

        return self.__dict__ != other.__dict__

    def __repr__(self):
        return "{name:s}: {attributes:s}".format(
            name=type(self).__name__,
            attributes=", ".join(
                [
                    "{}={}".format(attr_name, getattr(self, attr_name))
                    for attr_name in self.get_attr_names()
                ]
            ),
        )

    @classmethod
    def __validate_connection(cls):
        if cls.__connection is None:
            raise DatabaseError("SimpleSQLite connection required")

    @classmethod
    def __validate_value(cls, attr_name, value):
        column = cls.__get_col(attr_name)

        if value is None and not column.not_null:
            return

        column.typepy_class(value).validate()

    @classmethod
    def __is_attr(cls, attr_name):
        private_var_regexp = re.compile("^_{}__[a-zA-Z]+".format(Model.__name__))

        return (
            not attr_name.startswith("__")
            and private_var_regexp.search(attr_name) is None
            and not callable(cls.__dict__.get(attr_name))
        )

    @classmethod
    def __get_col(cls, attr_name):
        if attr_name not in cls.get_attr_names():
            raise ValueError("invalid attribute: {}".format(attr_name))

        return cls.__dict__.get(attr_name)
