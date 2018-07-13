# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from __future__ import absolute_import, unicode_literals

import logbook
import sqliteschema
import tabledata


logger = logbook.Logger("SimpleSQLie")
logger.disable()


def set_logger(is_enable):
    if is_enable != logger.disabled:
        return

    if is_enable:
        logger.enable()
    else:
        logger.disable()

    tabledata.set_logger(is_enable)
    sqliteschema.set_logger(is_enable)
    try:
        import pytablereader

        pytablereader.set_logger(is_enable)
    except ImportError:
        pass


def set_log_level(log_level):
    """
    Set logging level of this module. Using
    `logbook <http://logbook.readthedocs.io/en/stable/>`__ module for logging.

    :param int log_level:
        One of the log level of
        `logbook <http://logbook.readthedocs.io/en/stable/api/base.html>`__.
        Disabled logging if ``log_level`` is ``logbook.NOTSET``.
    :raises LookupError: If ``log_level`` is an invalid value.
    """

    # validate log level
    logbook.get_level_name(log_level)

    if log_level == logger.level:
        return

    if log_level == logbook.NOTSET:
        set_logger(is_enable=False)
    else:
        set_logger(is_enable=True)

    logger.level = log_level
    tabledata.set_log_level(log_level)
    sqliteschema.set_log_level(log_level)
    try:
        import pytablereader

        pytablereader.set_log_level(log_level)
    except ImportError:
        pass
