import abc
import warnings
from typing import Any, Optional

from typepy.type import AbstractType


class Column(metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def sqlite_datatype(self) -> str:
        return ""

    @property
    @abc.abstractmethod
    def typepy_class(self) -> type[AbstractType]:
        raise NotImplementedError

    @property
    def not_null(self) -> bool:
        return self.__not_null

    def __init__(
        self,
        attr_name: Optional[str] = None,
        not_null: bool = False,
        primary_key: bool = False,
        unique: bool = False,
        autoincrement: bool = False,
        default: Any = None,
    ) -> None:
        self.__column_name = attr_name
        self.__not_null = not_null
        self.__primary_key = primary_key
        self.__unique = unique
        self.__autoincrement = autoincrement
        self.__default_value = None if self.__not_null else default

    def get_header(self) -> str:
        warnings.warn(
            "get_header() is deprecated. Use get_column_name() instead.", DeprecationWarning
        )
        assert self.__column_name
        return self.__column_name

    def get_column_name(self) -> str:
        assert self.__column_name
        return self.__column_name

    def _set_column_name_if_uninitialized(self, header_name: str) -> None:
        self.__column_name = header_name

    def get_desc(self) -> str:
        from .query import Value

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
            constraints.append(f"DEFAULT {Value(self.__default_value)}")

        return " ".join(constraints)
