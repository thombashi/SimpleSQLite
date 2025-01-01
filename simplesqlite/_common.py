from collections import OrderedDict
from typing import TYPE_CHECKING, Final, Optional

from dataproperty.typing import TypeHint
from sqliteschema import SchemaHeader
from typepy import Integer, RealNumber, String


if TYPE_CHECKING:
    from simplesqlite import SimpleSQLite  # noqa

_sqlitetype_to_typepy: Final = {
    "INTEGER": Integer,
    "REAL": RealNumber,
    "TEXT": String,
}


def extract_table_metadata(
    con: "SimpleSQLite", table_name: str
) -> tuple[Optional[str], list[str], dict[str, TypeHint]]:
    primary_key = None
    index_attrs = []
    type_hints = OrderedDict()

    for attr in con.schema_extractor.fetch_table_schema(table_name).as_dict()[table_name]:
        attr_name = attr[SchemaHeader.ATTR_NAME]

        if attr[SchemaHeader.KEY] == "PRI":
            primary_key = attr_name
        elif attr[SchemaHeader.INDEX]:
            index_attrs.append(attr_name)

        type_hints[attr_name] = _sqlitetype_to_typepy.get(attr[SchemaHeader.DATA_TYPE])

    return (primary_key, index_attrs, type_hints)
