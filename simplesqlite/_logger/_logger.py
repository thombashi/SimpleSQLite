# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from __future__ import absolute_import, unicode_literals

import sqliteschema
import tabledata

from ._null_logger import NullLogger


MODULE_NAME = "simplesqlite"
_is_enable = False


try:
    from loguru import logger

    logger.disable(MODULE_NAME)
except ImportError:
    logger = NullLogger()


def set_logger(is_enable):
    global _is_enable

    if is_enable == _is_enable:
        return

    if is_enable:
        logger.enable(MODULE_NAME)
    else:
        logger.disable(MODULE_NAME)

    tabledata.set_logger(is_enable)
    sqliteschema.set_logger(is_enable)

    try:
        import pytablereader

        pytablereader.set_logger(is_enable)
    except ImportError:
        pass


def set_log_level(log_level):
    # deprecated
    return
