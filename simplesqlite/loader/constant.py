# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""

from __future__ import absolute_import


class TableNameTemplate(object):
    __FORMAT = "%({:s})s"
    DEFAULT = __FORMAT.format("default")
    FILENAME = __FORMAT.format("filename")
    FORMAT_NAME = __FORMAT.format("format_name")
    FORMAT_ID = __FORMAT.format("format_id")
    GLOBAL_ID = __FORMAT.format("global_id")
    KEY = __FORMAT.format("key")
    TITLE = __FORMAT.format("title")
    SHEET = __FORMAT.format("sheet")
