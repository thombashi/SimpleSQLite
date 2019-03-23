# encoding: utf-8

from __future__ import absolute_import, unicode_literals

from collections import OrderedDict

from sqliteschema import SchemaHeader
from typepy import Integer, RealNumber, String


_sqlitetype_to_typepy = {"INTEGER": Integer, "REAL": RealNumber, "TEXT": String}


def extract_table_metadata(con, table_name):
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
