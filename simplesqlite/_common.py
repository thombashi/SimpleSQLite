from collections import OrderedDict
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from dataproperty.typing import TypeHint
from sqliteschema import SchemaHeader
from typepy import Integer, RealNumber, String


if TYPE_CHECKING:
    from simplesqlite import SimpleSQLite  # noqa

_sqlitetype_to_typepy = {"INTEGER": Integer, "REAL": RealNumber, "TEXT": String}


def extract_table_metadata(
    con: "SimpleSQLite", table_name: str
) -> Tuple[Optional[str], List[str], Dict[str, TypeHint]]:
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
