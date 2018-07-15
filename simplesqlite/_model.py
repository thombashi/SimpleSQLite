# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from __future__ import absolute_import, unicode_literals

import re
import sys

from simplesqlite.query import AttrList
from six.moves import zip

from .error import DatabaseError


class Model(object):
    connection = None
    hidden = False
    __attr_name_list = None

    @classmethod
    def get_table_name(cls):
        table_name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", cls.__name__)
        table_name = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", table_name)
        table_name = table_name.replace("-", "_").lower()

        if cls.hidden:
            return "_{:s}_".format(table_name)

        return table_name

    @classmethod
    def get_attr_name_list(cls):
        if cls.__attr_name_list:
            return cls.__attr_name_list

        attr_name_list = [
            attr_name
            for attr_name in cls.__dict__
            if not attr_name.startswith("__") and attr_name not in ("connection", "hidden")
        ]

        if sys.version_info[:2] >= (3, 5):
            cls.__attr_name_list = attr_name_list
        else:
            cls.__attr_name_list = sorted(attr_name_list)

        return cls.__attr_name_list

    @classmethod
    def create(cls):
        cls.__validate()

        cls.connection.create_table(
            cls.get_table_name(),
            [
                "{attr} {constraints}".format(
                    attr=attr_name, constraints=cls.__dict__.get(attr_name)
                )
                for attr_name in cls.get_attr_name_list()
            ],
        )

    @classmethod
    def select(cls, where=None, extra=None):
        cls.__validate()

        result = cls.connection.select(
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

        cls.connection.insert(
            cls.get_table_name(),
            [getattr(model_obj, attr_name) for attr_name in cls.get_attr_name_list()],
        )

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
        if cls.connection is None:
            raise DatabaseError("SimpleSQLite connection required")
