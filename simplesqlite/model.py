# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from __future__ import absolute_import, unicode_literals

import abc
import re
import sys

import six
from simplesqlite.query import Attr, AttrList
from six.moves import zip

from .error import DatabaseError


@six.add_metaclass(abc.ABCMeta)
class Column(object):
    @abc.abstractproperty
    def data_type(self):
        return ""

    def __init__(self, not_null=False, primary_key=False, unique=False):
        self.__not_null = not_null
        self.__primary_key = primary_key
        self.__unique = unique

    def get_desc(self):
        constraints = [self.data_type]

        if self.__primary_key:
            constraints.append("PRIMARY KEY")
        else:
            if self.__not_null:
                constraints.append("NOT NULL")
            if self.__unique:
                constraints.append("UNIQUE")

        return " ".join(constraints)


class Integer(Column):
    @property
    def data_type(self):
        return "INTEGER"


class Real(Column):
    @property
    def data_type(self):
        return "REAL"


class Text(Column):
    @property
    def data_type(self):
        return "TEXT"


class Blob(Column):
    @property
    def data_type(self):
        return "BLOB"


class Model(object):
    __connection = None
    __is_hidden = False
    __table_name = None
    __attr_name_list = None

    @classmethod
    def attach(cls, connection, is_hidden=False):
        cls.__connection = connection
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
    def get_attr_name_list(cls):
        if cls.__attr_name_list:
            return cls.__attr_name_list

        attr_name_list = [attr_name for attr_name in cls.__dict__ if cls.__is_attr(attr_name)]

        if sys.version_info[:2] >= (3, 5):
            cls.__attr_name_list = attr_name_list
        else:
            cls.__attr_name_list = sorted(attr_name_list)

        return cls.__attr_name_list

    @classmethod
    def create(cls):
        cls.__validate()

        cls.__connection.create_table(
            cls.get_table_name(),
            [
                "{attr} {constraints}".format(
                    attr=Attr(attr_name), constraints=cls.__dict__.get(attr_name).get_desc()
                )
                for attr_name in cls.get_attr_name_list()
            ],
        )

    @classmethod
    def select(cls, where=None, extra=None):
        cls.__validate()

        result = cls.__connection.select(
            select=AttrList(cls.get_attr_name_list()),
            table_name=cls.get_table_name(),
            where=where,
            extra=extra,
        )
        for record in result.fetchall():
            yield cls(
                **{attr_name: value for attr_name, value in zip(cls.get_attr_name_list(), record)}
            )

    @classmethod
    def insert(cls, model_obj):
        cls.__validate()

        if type(model_obj).__name__ != cls.__name__:
            raise TypeError("unexpected type: expected={}".format(cls.__name__))

        cls.__connection.insert(
            cls.get_table_name(),
            [getattr(model_obj, attr_name) for attr_name in cls.get_attr_name_list()],
        )

    @classmethod
    def commit(cls):
        cls.__connection.commit()

    def __init__(self, *args, **kwargs):
        for attr_name in self.get_attr_name_list():
            setattr(self, attr_name, kwargs.get(attr_name))

    def __repr__(self):
        return "{name:s}: {attributes:s}".format(
            name=type(self).__name__,
            attributes=", ".join(
                [
                    "{}={}".format(attr_name, getattr(self, attr_name))
                    for attr_name in self.get_attr_name_list()
                ]
            ),
        )

    @classmethod
    def __validate(cls):
        if cls.__connection is None:
            raise DatabaseError("SimpleSQLite connection required")

    @classmethod
    def __is_attr(cls, attr_name):
        private_var_regexp = re.compile("^_{}__[a-zA-Z]+".format(Model.__name__))

        return (
            not attr_name.startswith("__")
            and private_var_regexp.search(attr_name) is None
            and not callable(cls.__dict__.get(attr_name))
        )
